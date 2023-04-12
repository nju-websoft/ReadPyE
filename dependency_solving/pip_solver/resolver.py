import collections
import math
from packaging.specifiers import SpecifierSet
from packaging.markers import Marker, InvalidMarker
import packaging.requirements
import neo4j
import sys
sys.path.append("...")
from .exceptions import RequirementsConflicted, InconsistentCandidate, ResolutionImpossible, ResolutionTooDeep, ResolverException, ResolverTimeoutException
from .structs import State, RequirementInformation, Requirement, Candidate, Criterion
from kg_api.kg_query import QueryApplication

import signal

def handle_timeout(signum, frame):
    raise ResolverTimeoutException('#Resolve timeout#')


class Resolution(object):
    def __init__(self, kg_querier):
        # [State, ...]
        self._states = []

        self.querier = kg_querier

        self._user_requested = None       # {package: order}, use for requirements file
        self._known_depths = None         # {package, depth}
        self.candidates_dict = None       # {str: [Candidate]}

        self.python_version = None
        self.deadline = None
    

    def initialize(self):
        # Initialize the root state.
        self._states = [
            State(
                mapping=collections.OrderedDict(),  # OrderedDict{package: PinnedInformation}
                criteria={},                        # {package: Criterion}
                backtrack_causes=[],                # []
            )
        ]

        self._user_requested = {}       
        self._known_depths = collections.defaultdict(lambda: math.inf)
        self.candidates_dict = {}
    

    def _append_installations(self, candidate):
        if not candidate.installed:
            candidate.installed = True
            # print('Collecting {!r}'.format(candidate))
    

    @property
    def state(self):
        if len(self._states) > 0:
            return self._states[-1]
        else:
            raise AttributeError("No state.")
    

    def _push_new_state(self):
        """Push a new state into history.
        This new state will be used to hold resolution results of the next coming round.
        """
        base = self._states[-1]
        state = State(
            mapping=base.mapping.copy(),
            criteria=base.criteria.copy(),
            backtrack_causes=base.backtrack_causes[:],
        )
        self._states.append(state)
    

    def _get_available_versions(self, package):
        if package in self.candidates_dict:
            return self.candidates_dict[package]

        with self.querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
            version_info = session.read_transaction(self.querier.get_versions4package, package)

        if self.deadline:
            # before the deadline
            tmp = []
            for item in version_info:
                upload_time = item[0].get('upload_time', None)
                if upload_time and upload_time <= self.deadline:
                    tmp.append(item)
            version_info = tmp
        
        # satify Python version
        ret = []
        for item in version_info:
            if self.python_version in SpecifierSet(item[1]['specifier']):
                # check supplement specifier
                repos_spec = item[1]['repos_spec']
                for str_spec in repos_spec.split(';'):
                    if self.python_version in SpecifierSet(str_spec):
                        ret.append(Candidate(package, item[0]['version']))
                        break
        
        ret.sort(key=lambda x:x.version, reverse=True)
        
        self.candidates_dict[package] = ret
        return ret

    
    def _add_to_criteria(self, criteria, requirement, parent):
        identifier = requirement.name
        criterion = criteria.get(identifier, None)

        if criterion:
            # Add to an existing package
            incompatibilities = criterion.incompatibilities[:]
            last_candidates = criterion.candidates[:]
            information = criterion.information[:]
            information.append(RequirementInformation(requirement, parent))
        else:
            incompatibilities = []
            last_candidates = self._get_available_versions(identifier)
            information = [RequirementInformation(requirement, parent), ]
        
        # calculate candidates for this package
        candidates = [item for item in last_candidates if item.version in requirement.specifier]
        
        criterion = Criterion(
            candidates=candidates,
            information=information,
            incompatibilities=incompatibilities,
        )
        
        if not candidates:
            raise RequirementsConflicted(criterion)
        
        self._append_installations(candidates[0])
        
        criteria[identifier] = criterion
    

    def _remove_from_criteria(self, criteria, identifier, parent):
        criterion = criteria.get(identifier, None)

        information = []
        spec = SpecifierSet('', prereleases=True)
        extra = set()
        for item in criterion.information:
            if item.parent is None or item.parent.name != parent.name:
                information.append(item)
                spec &= item.requirement.specifier
                extra.update(item.requirement.extra)

        incompatibilities = criterion.incompatibilities[:]
        # re-calculate the candidates
        candidates = [item for item in self._get_available_versions(identifier) if item not in incompatibilities and item.version in spec]

        criterion = Criterion(
            candidates=candidates,
            information=information,
            incompatibilities=incompatibilities,
        )
        
        if not candidates:
            # no possible
            raise RequirementsConflicted(criterion)
        
        self._append_installations(candidates[0])
        
        criteria[identifier] = criterion

    

    @staticmethod
    def _is_satisfied_by(requirement, candidate):
        if candidate.version not in requirement.specifier or not requirement.extra.issubset(candidate.extra):
            # do not satify the specifier or do not contain all extra
            return False
        
        return True
    

    def _is_current_pin_satisfying(self, name, criterion):
        if len(criterion.information) == 0:
            # not required by any package anymore (re-select another version)
            return True

        if name not in self.state.mapping:
            # not pinned
            return False
        
        # Check if the pinned version satisfies all requirements
        current_pin = self.state.mapping[name]
        
        return all(self._is_satisfied_by(r, current_pin) for r in criterion.iter_requirement())
    

    @staticmethod
    def is_backtrack_cause(identifier, backtrack_causes):
        for backtrack_cause in backtrack_causes:
            if identifier == backtrack_cause.requirement.name:
                return True
            if backtrack_cause.parent and identifier == backtrack_cause.parent.name:
                return True
        return False
    

    def _get_preference(self, name):
        """Produce a sort key for given requirement based on preference.
        The lower the return value is, the more preferred this group of arguments is.
        Currently pip considers the followings in order:
        * # Prefer if any of the known requirements is "direct", e.g. points to an
          explicit URL.
        * If equal, prefer if any requirement is "pinned", i.e. contains
          operator ``===`` or ``==``.
        * If equal, calculate an approximate "depth" and resolve requirements
          closer to the user-specified requirements first.
        * Order user-specified requirements by the order they are specified.
        * If equal, prefers "non-free" requirements, i.e. contains at least one
          operator, such as ``>=`` or ``<``.
        * If equal, order alphabetically for consistency (helps debuggability).
        """
        info = self.state.criteria[name].information

        operators = set([r.str_specifier for r, _ in info])
        pinned = any(op[:2] == "==" for op in operators)
        unfree = bool(operators)

        if name in self._user_requested:
            requested_order = self._user_requested[name]
            inferred_depth = 1.0
        else:
            requested_order = math.inf
            parent_depths = (
                self._known_depths[parent.name] if parent is not None else 0.0
                for _, parent in info
            )
            inferred_depth = min(d for d in parent_depths) + 1.0
        self._known_depths[name] = inferred_depth

        # HACK: Setuptools have a very long and solid backward compatibility
        # track record, and extremely few projects would request a narrow,
        # non-recent version range of it since that would break a lot things.
        # (Most projects specify it only to request for an installer feature,
        # which does not work, but that's another topic.) Intentionally
        # delaying Setuptools helps reduce branches the resolver has to check.
        # This serves as a temporary fix for issues like "apache-airflow[all]"
        # while we work on "proper" branch pruning techniques.
        delay_this = name == "setuptools"

        # Prefer the causes of backtracking on the assumption that the problem
        # resolving the dependency tree is related to the failures that caused
        # the backtracking
        backtrack_cause = self.is_backtrack_cause(name, self.state.backtrack_causes)

        return (
            delay_this,
            not pinned,
            not backtrack_cause,
            inferred_depth,
            requested_order,
            not unfree,
            name,
        )
    

    def _get_dependencies(self, candidate, req_extra):
        with self.querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
            req_list = session.read_transaction(self.querier.get_requirements4version, candidate.name, candidate.str_version)
        req_list.sort(key=lambda x:x[1]['order'])

        ret = []

        environment = {'python_version': self.python_version}
        for pkg, rel in req_list:
            # satify the marker
            marker_str = rel.get('marker', None)
            if marker_str is not None:
                try:
                    marker = Marker(marker_str)
                except InvalidMarker:
                    continue
                
                has_rel = False
                if req_extra:
                    for extra in req_extra:
                        environment['extra'] = extra
                        if marker.evaluate(environment=environment):
                            has_rel = True
                            break
                else:
                    environment['extra'] = ''
                    has_rel = marker.evaluate(environment=environment)
                        
                if not has_rel:
                    # does not require this package
                    continue
            
            specifier = rel.get('specifier', '')
            
            extra = rel.get('extras', None)
            if extra:
                extra = extra.split(' ')
            
            ret.append(Requirement(pkg, specifier, extra))
        
        return ret
    

    def _get_updated_criteria(self, candidate, req_extra):
        criteria = self.state.criteria.copy()
        for requirement in self._get_dependencies(candidate, req_extra):
            self._add_to_criteria(criteria, requirement, parent=candidate)
        return criteria
    

    def _unpin_name(self, name):
        # remove existing requirements if name is pinned
        if name not in self.state.mapping:
            return
        
        curret_pin = self.state.mapping.pop(name)
        for requirement in self._get_dependencies(curret_pin, curret_pin.extra):
            self._remove_from_criteria(self.state.criteria, requirement.name, curret_pin)
    

    def _attempt_to_pin_criterion(self, name):
        criterion = self.state.criteria[name]
        req_extra = criterion.get_req_extra()
        
        # unpin name (avoid repetitive requirements from the same package)
        self._unpin_name(name)

        causes = []
        for candidate in criterion.candidates:
            self._append_installations(candidate)

            try:
                criteria = self._get_updated_criteria(candidate, req_extra)
            except RequirementsConflicted as e:

                causes.append(e.criterion)
                continue

            req_extra = criterion.get_req_extra()
            selected_candidate = Candidate(candidate.name, candidate.version, req_extra)
            # Check the newly-pinned candidate actually works. This should
            # always pass under normal circumstances, but in the case of a
            # faulty provider, we will raise an error to notify the implementer
            # to fix find_matches() and/or is_satisfied_by().
            satisfied = all(
                self._is_satisfied_by(r, selected_candidate) for r in criterion.iter_requirement()
            )
            if not satisfied:
                raise InconsistentCandidate(selected_candidate, criterion)
        
            self.state.criteria.update(criteria)

            # Put newly-pinned candidate at the end. This is essential because
            # backtracking looks at this mapping to get the last pin.
            self.state.mapping[name] = selected_candidate

            return []

        # All candidates tried, nothing works. This criterion is a dead
        # end, signal for backtracking.
        return causes
    

    def _backtrack(self):
        """Perform backtracking.
        When we enter here, the stack is like this::
            [ state Z ]
            [ state Y ]
            [ state X ]
            .... earlier states are irrelevant.
        1. No pins worked for Z, so it does not have a pin.
        2. We want to reset state Y to unpinned, and pin another candidate.
        3. State X holds what state Y was before the pin, but does not
           have the incompatibility information gathered in state Y.
        Each iteration of the loop will:
        1.  Discard Z.
        2.  Discard Y but remember its incompatibility information gathered
            previously, and the failure we're dealing with right now.
        3.  Push a new state Y' based on X, and apply the incompatibility
            information from Y to Y'.
        4a. If this causes Y' to conflict, we need to backtrack again. Make Y'
            the new Z and go back to step 2.
        4b. If the incompatibilities apply cleanly, end backtracking.
        """
        while len(self._states) >= 3:
            # Remove the state that triggered backtracking.
            del self._states[-1]

            # Retrieve the last candidate pin and known incompatibilities.
            broken_state = self._states.pop()
            name, candidate = broken_state.mapping.popitem()

            incompatibilities_from_broken = [
                (k, list(v.incompatibilities))
                for k, v in broken_state.criteria.items()
            ]

            # Also mark the newly known incompatibility.
            incompatibilities_from_broken.append((name, [candidate]))

            # print('Backtrack in {}'.format(candidate))

            # Create a new state from the last known-to-work one, and apply
            # the previously gathered incompatibility information.
            def _patch_criteria():
                for k, incompatibilities in incompatibilities_from_broken:
                    if not incompatibilities or k not in self.state.criteria:
                        continue
                    
                    criterion = self.state.criteria[k]

                    candidates = []
                    for item in criterion.candidates:
                        if item not in incompatibilities:
                            candidates.append(item)
                    
                    incompatibilities.extend(criterion.incompatibilities)

                    if not candidates:
                        return False
                    
                    self._append_installations(candidates[0])

                    self.state.criteria[k] = Criterion(
                        candidates=candidates,
                        information=criterion.information[:],
                        incompatibilities=incompatibilities,
                    )

                return True

            self._push_new_state()
            success = _patch_criteria()

            # It works! Let's work on this new state.
            if success:
                return True

            # State does not work after applying known incompatibilities.
            # Try the still previous state.

        # No way to backtrack anymore.
        return False
    

    def resolve(self, requirements, python_version, deadline=None, max_rounds=2000000):
        self.python_version = python_version
        self.deadline = deadline

        self.initialize()

        for i, r in enumerate(requirements):
            if r.name not in self._user_requested:
                self._user_requested[r.name] = i
            
            try:
                self._add_to_criteria(self.state.criteria, r, parent=None)
            except RequirementsConflicted as e:
                raise ResolutionImpossible(e.criterion.information)
        
        # The root state is saved as a sentinel so the first ever pin can have
        # something to backtrack to if it fails. The root state is basically
        # pinning the virtual "root" package in the graph.
        self._push_new_state()

        for round_index in range(max_rounds):
            unsatisfied_names = [key for key, criterion in self.state.criteria.items() if not self._is_current_pin_satisfying(key, criterion)]

            if not unsatisfied_names:
                # Nothing more to pin: done!
                # print(round_index)
                return self.state
            
            # Choose the most preferred unpinned criterion to try.
            name = min(unsatisfied_names, key=self._get_preference)
            failure_causes = self._attempt_to_pin_criterion(name)

            if failure_causes:
                # print('No incompatible candidates for package {}'.format(name))
                causes = [i for c in failure_causes for i in c.information]
                # Backtrack if pinning fails. The backtrack process puts us in
                # an unpinned state, so we can work on it in the next round.
                success = self._backtrack()
                self.state.backtrack_causes[:] = causes

                # Dead ends everywhere. Give up.
                if not success:
                    raise ResolutionImpossible(self.state.backtrack_causes)
            else:
                # Pinning was successful. Push a new state to do another pin.
                self._push_new_state()
                

        raise ResolutionTooDeep(max_rounds)
    

    @staticmethod
    def generate_requirements(req_list):
        ret = []
        for item in req_list:
            try:
                req = packaging.requirements.Requirement(item)

                ret.append(Requirement(req.name, req.specifier, req.extras))
            except packaging.requirements.InvalidRequirement:
                print('\"{}\" is an invalid requirement.'.format(item))
        
        return ret
    

    def generate_install_pairs(self, extra_deps):
        # identify the packages that need to be explicitly installed as well as the installation order.
        last_state = self._states.pop()
        ret_pairs = {k: last_state.mapping[k] for k in self._user_requested}

        # the installation order
        in_table = {}
        out_table = {}

        # dependencies
        for k, v in last_state.criteria.items():
            if k not in in_table:
                in_table[k] = set()
                out_table[k] = set()
            
            for pkg in v.iter_parent():
                if pkg:
                    name = pkg.name
                    if name not in in_table:
                        in_table[name] = set()
                        out_table[name] = set()
                    
                    in_table[k].add(name)
                    out_table[name].add(k)
        
        # extra
        for pkg, dep_list in extra_deps.items():
            for dep in dep_list:
                in_table[dep].add(pkg)
                out_table[pkg].add(dep)

        topo_sort = []
        while len(in_table) > 0:
            has_find = False
            for k in list(out_table):
                v = out_table[k]
                if len(v) == 0:
                    has_find = True

                    if k in ret_pairs:
                        topo_sort.append(ret_pairs[k])
                    
                    # delete the connected nodes
                    for in_node in in_table[k]:
                        out_table[in_node].remove(k)
                    
                    # delete itself
                    in_table.pop(k)
                    out_table.pop(k)
            
            if not has_find:
                # There exsits a dependency circle: the first encountered member of the cycle is installed last (https://pip.pypa.io/en/stable/cli/pip_install/#installation-order)
                start_nodes = [k for k, v in in_table.items() if len(v) == 0]

                if not start_nodes:
                    start_nodes.append(list(in_table)[0])
                
                is_removed = False
                for item in start_nodes:
                    label_dict = {k: 0 for k in in_table}
                    st = [item, ]
                    while len(st) > 0:
                        node = st.pop()
                        label_dict[node] = 1

                        for out_node in out_table[node]:
                            if label_dict[out_node] == 1:
                                # break this circle
                                out_table[node].remove(out_node)
                                in_table[out_node].remove(node)
                                is_removed = True
                                break
                            elif label_dict[out_node] == 0:
                                st.append(out_node)
                        
                        if is_removed:
                            break
                    
                    if is_removed:
                        break
                

        return [(x.name, x.str_version) for x in topo_sort]

    

    def main(self, requirements, python_version, extra_deps, deadline=None, max_rounds=10000):
        # Register the signal function handler
        signal.signal(signal.SIGALRM, handle_timeout)
        signal.alarm(300)

        ret = None
        try:
            self.resolve(requirements, python_version, deadline, max_rounds)
        except ResolverException as e:
            signal.alarm(0)
        else:
            signal.alarm(0)
            ret = self.generate_install_pairs(extra_deps)
        finally:
            return ret