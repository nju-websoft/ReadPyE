import copy
import neo4j
from packaging.version import parse
from packaging.specifiers import SpecifierSet

from .smt_solver import DepOptimizer
from .pip_solver.resolver import Resolution
import sys
sys.path.append("..")
from kg_api.kg_query import QueryApplication
from utils.handle_unknown import get_similar_packages


class EnvGenerator(object):
    def __init__(self, kg_querier, ratio_calculator):
        self.kg_querier = kg_querier
        self.calculator = ratio_calculator

        self.smt_solver = DepOptimizer()
        self.pip_solver = Resolution(kg_querier)

        self.pkg_version_dict = {}

        # the results from candidate discovery
        self.python_candidates = None       # [str, ]
        self.pv_candidates = None           # {top module: {pkg: [(version, spec, repos_spec, matching_degree), ]}}
        self.similarity_dict = None         # {top_module: {pkg: similarity}}
        
        self.existing_pyver = None
        self.existing_pvs = None

        # the intermediate state of candidates
        self.selected_pyvers = None         # [str, ]
        self.selected_pvs = None            # {top_module: {pkg: [version, ]}}

        self.avail_pyvers = None            # {top_module: set(pyver, )}
        
        self.installed_module_pkgs = None   # {top_module: set(pkg, )}
        self.extra_deps = None              # {pkg: set(top_module)}

        self.used_pkgs = None               # {top_module: set(pkg, )}
        self.unknown_modules = None         # set(module, )
    

    def _clean_intermediate_state(self):
        self.python_candidates = []
        self.pv_candidates = {}
        self.similarity_dict = {}
        
        self.existing_pyver = None
        self.existing_pvs = None

        # the intermediate state of candidates
        self.selected_pyvers = []
        self.selected_pvs = {}
        self.avail_pyvers = {}
        self.installed_module_pkgs = {}
        self.extra_deps = {}
        self.used_pkgs = {}
        self.unknown_modules = set()
    

    def backup_state(self):
        return self.python_candidates[:], copy.deepcopy(self.pv_candidates), self.selected_pyvers[:], copy.deepcopy(self.selected_pvs), copy.deepcopy(self.avail_pyvers)
    
    def restore_state(self, backup):
        self.python_candidates, self.pv_candidates, self.selected_pyvers, self.selected_pvs, self.avail_pyvers = backup
    

    def _cal_avail_pyvers_for_versions(self, v_list):
        # find available Python versions for v_list
        py_reqs = [(item[1], item[2]) for item in v_list]

        avail_pythons = []
        for meta_spec, repos_spec in py_reqs:
            meta_spec_obj = SpecifierSet(meta_spec)
            repos_spec_list = [SpecifierSet(spec) for spec in repos_spec.split(';')]

            for pyver in self.python_candidates:
                # satify metadata
                if pyver not in meta_spec_obj:
                    continue

                # satify repos spec
                for spec in repos_spec_list:
                    if pyver in spec:
                        avail_pythons.append(pyver)
                        break
        
        return set(avail_pythons)
    

    def select_pvs_for_module(self, top_module):
        # remove avail_pyvers info for the top module
        if top_module in self.avail_pyvers:
            self.avail_pyvers.pop(top_module)

        # select the maximum mathing degree for top module
        avail_pythons = set()
        if top_module in self.selected_pvs and len(self.selected_pvs[top_module]) > 0:
            # has items in selected pvs
            max_pv = self.selected_pvs[top_module]
            for v_list in max_pv.values():
                avail_pythons |= self._cal_avail_pyvers_for_versions(v_list)
        
        while len(avail_pythons) == 0:
            max_pv = {}

            # get pvs from pv_candidates
            pv_info = self.pv_candidates.get(top_module, None)
            if pv_info is None or len(pv_info) == 0:
                if top_module not in self.unknown_modules:
                    # no available pvs: get candidates by similarity
                    self.unknown_modules.add(top_module)
                    unknown_candidate_pvs, unknown_pkg_module_dict = get_similar_packages(self.kg_querier, self.calculator, [top_module, ])
                    
                    candidate_pvs = unknown_candidate_pvs.get(top_module, None)
                    if candidate_pvs is not None:
                        avail_pkgs = set(candidate_pvs) - self.used_pkgs[top_module]
                        if len(avail_pkgs) > 0:
                            self.used_pkgs[top_module].update(avail_pkgs)
                            self.similarity_dict[top_module].update({k: v for k, v in unknown_pkg_module_dict[top_module].items() if k in avail_pkgs})
                            self.pv_candidates[top_module] = {k: v for k, v in candidate_pvs.items() if k in avail_pkgs}
                            continue
                
                # no available pvs indeed
                break
            
            # Get maximum mathing degree
            max_score = 0
            del_pkgs = []
            for pkg, v_list in pv_info.items():
                if len(v_list) == 0:
                    del_pkgs.append(pkg)
                else:
                    max_score = max(max_score, v_list[0][-1])
            
            # delete the packages with no candidate versions
            for pkg in del_pkgs:
                pv_info.pop(pkg)

            # Get pv with maximum mathing degree
            for pkg, v_list in pv_info.items():
                v_tmp = []
                for item in v_list:
                    if item[-1] == max_score:
                        v_tmp.append(item)
                    else:
                        break
                
                # remove these pvs in pv_candidates
                length = len(v_tmp)
                if length > 0:
                    pv_info[pkg] = v_list[length:]
                    pyver_tmp = self._cal_avail_pyvers_for_versions(v_tmp)
                    if len(pyver_tmp) > 0:
                        # have available pyvers
                        max_pv[pkg] = v_tmp
                        avail_pythons |= pyver_tmp

        # has candidate pvs
        self.selected_pvs[top_module] = max_pv
        # pyvers
        self.avail_pyvers[top_module] = avail_pythons
     

    def _cal_selected_pyvers(self, use_py2=False):
        # python versions satifies all top modules
        if use_py2:
            select_pyvers = set([x for x in self.python_candidates if x.startswith('2')])
        else:
            select_pyvers = set(self.python_candidates)

        for avail_pyvers in self.avail_pyvers.values():
            select_pyvers &= avail_pyvers

        if len(select_pyvers) == 0:
            # just try to give an environment for interation
            self.selected_pyvers = self.python_candidates[:1]
        else:
            # sort
            self.selected_pyvers = [item for item in self.python_candidates if item in select_pyvers]


    def _filter_protected_pvs(self, pv_candidates):
        if self.existing_pvs is None or len(self.existing_pvs) == 0:
            return
        
        # pkgs in the existing env
        ex_pkgs = set(self.existing_pvs)
        
        for top_module, pv_info in pv_candidates.items():
            common_pkgs = set(pv_info) & set(ex_pkgs)
            if len(common_pkgs) == 0:
                continue
            
            # don't find packages for this top module: protect the existing packages
            self.unknown_modules.add(top_module)

            # delete the other packages
            for pkg in list(pv_info):
                if pkg not in common_pkgs:
                    pv_info.pop(pkg)
            
            # only remain the existing version
            index = -1
            for pkg in common_pkgs:
                for i, v_item in enumerate(pv_info[pkg]):
                    if v_item[0] == self.existing_pvs[pkg]:
                        index = i
                        break
                
                pv_info[pkg] = pv_info[pkg][index:index+1]
    

    def set_candidates(self, python_candidates, third_candidates, existing_env=None):
        # Init
        self._clean_intermediate_state()

        self.pv_candidates, self.similarity_dict = third_candidates
        self.used_pkgs = {k: set(v) for k, v in self.pv_candidates.items()}

        if existing_env is not None:
            self.existing_pyver, self.existing_pvs = existing_env
            if python_candidates is None or self.existing_pyver not in python_candidates:
                # no compatible Python version
                return False
            python_candidates = [self.existing_pyver, ]

            self._filter_protected_pvs(self.pv_candidates)


        if python_candidates is None or len(python_candidates) == 0:
            # no available Python version
            return False

        self.python_candidates = python_candidates

        # control the space
        if len(self.pkg_version_dict) > 200:
            self.pkg_version_dict = {}

        for top_module in self.pv_candidates:
            self.select_pvs_for_module(top_module)

        return True
    

    def add_python_constraint(self, constraint):
        if self.existing_pyver is not None:
            return False
        
        # adjust the selected pvs
        if isinstance(constraint, str):
            constraint = SpecifierSet(constraint)

        # backup
        backup = self.backup_state()
        self.python_candidates = [item for item in self.python_candidates if item in constraint]

        old_pyver = self.selected_pyvers[0]
        # selected_pyvers = [item for item in python_candidates if item in self.selected_pyvers]

        for top_module in list(self.avail_pyvers):
            if len(self.avail_pyvers[top_module] & set(self.python_candidates)) == 0:
                self.select_pvs_for_module(top_module)

        self._cal_selected_pyvers()
        if len(self.selected_pyvers) > 0 and self.selected_pyvers[0] != old_pyver:
            return True
        
        self.restore_state(backup)
        return False

    

    def add_pv_constraint(self, candidate_pvs, pkg_module_dict, parent):
        # update similarity dict
        for top_module, pkg_info in pkg_module_dict.items():
            if top_module not in self.similarity_dict:
                self.similarity_dict[top_module] = pkg_info
            else:
                self.similarity_dict[top_module].update(pkg_info)
        
        # update candidate pvs and selected pvs
        self._filter_protected_pvs(candidate_pvs)
        for top_module, pkg_info in candidate_pvs.items():

            # remove installed pkgs from selected pkgs for this top module
            if top_module in self.installed_module_pkgs:
                for pkg in self.installed_module_pkgs[top_module]:
                    self.selected_pvs[top_module].pop(pkg)
            
            if top_module not in self.unknown_modules:
                # available pvs
                specific_pvs = {p: set([v_info[0] for v_info in v_list]) for p, v_list in pkg_info.items()}

                # filter selected pvs
                if top_module in self.selected_pvs:
                    for pkg in list(self.selected_pvs[top_module]):
                        if pkg not in specific_pvs:
                            self.selected_pvs[top_module].pop(pkg)
                        else:
                            self.selected_pvs[top_module][pkg] = [x for x in self.selected_pvs[top_module][pkg] if x[0] in specific_pvs[pkg]]

                # filter candidate pvs
                if top_module in self.pv_candidates:
                    for pkg in list(self.pv_candidates[top_module]):
                        if pkg not in specific_pvs:
                            self.pv_candidates[top_module].pop(pkg)
                        else:
                            self.pv_candidates[top_module][pkg] = [x for x in self.pv_candidates[top_module][pkg] if x[0] in specific_pvs[pkg]]
                
                else:
                    self.used_pkgs[top_module] = set(pkg_info)
                    self.pv_candidates[top_module] = pkg_info
            
            if parent is not None:
                if parent not in self.extra_deps:
                    self.extra_deps[parent] = set()
                self.extra_deps[parent].add(top_module)

            # reselect pvs for top_module
            self.select_pvs_for_module(top_module)
            return True
    

    def _get_all_versions_for_package(self, pkg):
        # get all versions for a package
        if pkg not in self.pkg_version_dict:
            with self.kg_querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
                v_list = session.read_transaction(QueryApplication.get_versions_by_package, pkg)
            v_list.sort(key=lambda x: parse(x))
            self.pkg_version_dict[pkg] = v_list
        
        return self.pkg_version_dict[pkg]
    

    def _generate_version_range(self, pkg, version_list):
        # generate the version range (str) of a package with version list
        if not version_list:
            return pkg

        if len(version_list) == 1:
            return '{}=={}'.format(pkg, version_list[0])
        
        min_version = version_list[-1]
        max_version = version_list[0]
        req_str = '{}>={},<={}'.format(pkg, min_version, max_version)

        all_version = self._get_all_versions_for_package(pkg)
        tmp = all_version[all_version.index(min_version):all_version.index(max_version)+1]

        exclude_versions = [item for item in tmp if item not in version_list]
        for item in exclude_versions:
            req_str += ',!={}'.format(item)
        
        return req_str
        

    def generate_candidate_environment(self, use_py2=False):
        # select a Python version
        self._cal_selected_pyvers(use_py2)
        if len(self.selected_pyvers) > 0:
            python_version = self.selected_pyvers[0]
        else:
            return None

        if len(self.selected_pvs) == 0:
            return (python_version, [])

        select_pkgs = {k: list(v) for k, v in self.selected_pvs.items()}
        installed_pkgs = self.smt_solver.main(select_pkgs, self.similarity_dict)

        if installed_pkgs is None:
            # no pkg for one top module
            return None
        
        if len(installed_pkgs) == 0:
            return (python_version, [])
        
        installed_pkgs = set(installed_pkgs)

        self.installed_module_pkgs = {k: set([x for x in v if x in installed_pkgs]) for k, v in select_pkgs.items()}

        # candidate versions
        pv_dict = {}
        for pv_value in self.selected_pvs.values():
            for pkg, v_list in pv_value.items():
                if pkg in installed_pkgs:
                    # only version specifier
                    version_list = [item[0] for item in v_list]
                    if pkg not in pv_dict:
                        pv_dict[pkg] = version_list
                    else:
                        intersection_set = set(pv_dict[pkg]) & set(version_list)
                        if len(intersection_set) > 0:
                            pv_dict[pkg] = intersection_set
                        else:
                            pv_dict[pkg] = set(pv_dict[pkg]) | set(version_list)
        
        for pkg in pv_dict:
            if isinstance(pv_dict[pkg], set):
                pv_dict[pkg] = sorted(pv_dict[pkg], key=lambda x: parse(x), reverse=True)

        # generate the version range for version candidates
        installed_list = []

        # add existing envs
        if self.existing_pvs is not None:
            for p, v in self.existing_pvs.items():
                installed_list.append('{}=={}'.format(p, v))

        for pkg, version_list in pv_dict.items():
            if version_list:
                installed_list.append(self._generate_version_range(pkg, version_list))

        extra_deps = {}
        for pkg, dep_set in self.extra_deps.items():
            if pkg not in extra_deps:
                extra_deps[pkg] = set()
            for top_module in dep_set:
                extra_deps[pkg].update(self.installed_module_pkgs.get(top_module, set()))

        pip_res = self.pip_solver.main(Resolution.generate_requirements(installed_list), python_version, extra_deps)
        installed_list = []
        if pip_res is None:
            # can't solve
            for pkg, version_list in pv_dict.items():
                if version_list:
                    installed_list.append(f'{pkg}=={version_list[0]}')
        else:
            if self.existing_pvs is None:
                for pkg, max_version in pip_res:
                    installed_list.append(f'{pkg}=={max_version}')
            else:
                for pkg, max_version in pip_res:
                    if pkg not in self.existing_pvs:
                        installed_list.append(f'{pkg}=={max_version}')
        
        return (python_version, installed_list)