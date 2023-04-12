class ResolverException(Exception):
    """A base class for all exceptions raised by this module.
    Exceptions derived by this class should all be handled in this module. Any
    bubbling pass the resolver should be treated as a bug.
    """


class RequirementsConflicted(ResolverException):
    def __init__(self, criterion):
        super(RequirementsConflicted, self).__init__(criterion)
        self.criterion = criterion

    def __str__(self):
        return "Requirements conflict: {}".format(
            ", ".join(repr(r) for r in self.criterion.iter_requirement()),
        )


class InconsistentCandidate(ResolverException):
    def __init__(self, candidate, criterion):
        super(InconsistentCandidate, self).__init__(candidate, criterion)
        self.candidate = candidate
        self.criterion = criterion

    def __str__(self):
        return "Provided candidate {!r} does not satisfy {}".format(
            self.candidate,
            ", ".join(repr(r) for r in self.criterion.iter_requirement()),
        )


class ResolverTimeoutException(ResolverException):
    def __init__(self, output):
        self.output = output
    
    def __str__(self):
        return self.output


class ResolutionError(ResolverException):
    pass


class ResolutionImpossible(ResolutionError):
    def __init__(self, causes):
        super(ResolutionImpossible, self).__init__(causes)
        # causes is a list of RequirementInformation objects
        self.causes = causes
    
    def __str__(self):
        return "ResolutionImpossible: {}".format("; ".join("({!r}, via={!r})".format(req, parent)
            for req, parent in self.causes))


class ResolutionTooDeep(ResolutionError):
    def __init__(self, round_count):
        super(ResolutionTooDeep, self).__init__(round_count)
        self.round_count = round_count
    
    def __str__(self):
        return "ResolutionTooDeep: the round of resolution is over {}".format(self.round_count)