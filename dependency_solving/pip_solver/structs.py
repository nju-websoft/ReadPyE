import collections
from packaging.version import parse
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name


## Resolution state in a round.
# mapping: OrderedDict{str: Candidate}
# criteria: {str: Criterion}
# backtrack_causes: [RequirementInformation]
State = collections.namedtuple("State", "mapping criteria backtrack_causes")

# requirement: Requirement
# parent: Candidate
RequirementInformation = collections.namedtuple(
    "RequirementInformation", ["requirement", "parent"]
)


class Requirement(object):
    def __init__(self, name, specifier='', extra=None):
        '''
        name, specifier: str
        extra: Set
        '''
        self.name = canonicalize_name(name)
        if isinstance(specifier, str):
            self.str_specifier = specifier
            self.specifier = SpecifierSet(specifier, prereleases=True)
        else:
            self.str_specifier = str(specifier)
            self.specifier = specifier

        if extra:
            self.extra = set(extra)
        else:
            self.extra = set()
    

    def __repr__(self):
        # 'name[extra]specifier'
        ret = self.name
        if self.extra:
            ret += '[{}]'.format(','.join(self.extra))
        
        ret += self.str_specifier

        return ret


class Candidate(object):
    def __init__(self, name, version, extra=None):
        '''
        name, version: str
        extra: Set
        '''
        self.name = name

        if isinstance(version, str):
            self.str_version = version
            self.version = parse(version)
        else:
            self.str_version = str(version)
            self.version = version
        
        if extra:
            self.extra = set(extra)
        else:
            self.extra = set()
        
        self.installed = False
    

    def __repr__(self):
        # 'name[extra] version'

        return self.format_candidate(self.extra)
    

    def __eq__(self, other):
        if isinstance(other, Candidate) and self.name == other.name and self.version == other.version:
            return True
        return False
    

    def format_candidate(self, extra):
        # 'name[extra] version'
        ret = self.name
        if extra:
            ret += '[{}]'.format(','.join(extra))
        
        ret = '{}=={}'.format(ret, self.str_version)

        return ret



class Criterion(object):
    """Representation of possible resolution results of a package.
    This holds three attributes:
    * `information` is a collection of `RequirementInformation` pairs.
      Each pair is a requirement contributing to this criterion, and the
      candidate that provides the requirement.
    * `incompatibilities` is a collection of all known not-to-work candidates
      to exclude from consideration.
    * `candidates` is a collection containing all possible candidates deducted
      from the union of contributing requirements and known incompatibilities.
      It should never be empty, except when the criterion is an attribute of a
      raised `RequirementsConflicted` (in which case it is always empty).
    .. note::
        This class is intended to be externally immutable. **Do not** mutate
        any of its attribute containers.
    """

    def __init__(self, candidates, information, incompatibilities):
        # [Candidate, ...]
        self.candidates = candidates
        self.incompatibilities = incompatibilities

        # [RequirementInformation, ...]
        self.information = information

    def __repr__(self):
        requirements = ", ".join(
            "({!r}, via={!r})".format(req, parent)
            for req, parent in self.information
        )
        return "Criterion({})".format(requirements)

    def iter_requirement(self):
        return (i.requirement for i in self.information)

    def iter_parent(self):
        return (i.parent for i in self.information)
    
    def get_req_extra(self):
        ret = set()
        for i in self.information:
            ret.update(i.requirement.extra)
        return ret