import re
import collections
from .variables import SIM_THRESHOLD


def calculate_matching_degree(spanning_tree, leaves_set):
    '''
    For the matching degree
    '''
    if len(spanning_tree) == 0 or len(leaves_set) == 0:
        return 0.0
    
    ret = 0.0
    for name in leaves_set:
        if name in spanning_tree:
            ret += 1
        else:
            split_info = name.split('.')
            length = len(split_info)
            prefix_name = name
            i = 1
            while i < length:
                prefix_name = prefix_name[:-(len(split_info[-i])+1)]
                if prefix_name in spanning_tree:
                    break
                i += 1
        
            ret += 1 - i/length

    return ret/len(leaves_set)


'''
base class for similarity
'''
class TextSimilarity(object):
    def __init__(self, pkg_collections):
        self.pkg_collections = pkg_collections
        self.pkg_alias_collections = {x: None for x in self.pkg_collections}
    

    def ratio(self, s1, s2):
        '''
        calculate the text similarity
        '''
    
    
    def max_ratio(self, word, pkg):
        '''
        the API for similarity
        '''
        score = self.ratio(word, pkg)

        alias_name = self.pkg_alias_collections.get(pkg, None)
        if alias_name is not None:
            score = max(score, self.ratio(word, alias_name))

        return score
    

    def get_ratios_for_pkgs(self, word, pkg_list):
        '''
        calculate the text similarity for a list of pkgs
        '''
        result = []
        for x in pkg_list:
            score = self.max_ratio(word, x)
            result.append((score, x))
                
        return result


'''
No similarity
'''
class NoneSimilarity(TextSimilarity):
    def __init__(self, pkg_collections):
        super().__init__(pkg_collections)

    def max_ratio(self, word, pkg):
        return 1.0


'''
our naming similarity
'''
class NamingSimilarity(TextSimilarity):
    def __init__(self, pkg_collections):
        super().__init__(pkg_collections)

        # handle prefix and suffix
        name_pattern = re.compile(r'^(?:py(?:thon)?[23]?-?)?(.*?)(?:-?py(?:thon)?[23]?)?$')
        for pkg in self.pkg_collections:
            matchObj = re.match(name_pattern, pkg)
            pkg_alias = matchObj.group(1)
            if pkg_alias != pkg:
                # different
                self.pkg_alias_collections[pkg] = pkg_alias


    def _calculate_ratio(self, matches, length):
        return 2.0 * matches / length

    def real_quick_ratio(self, s1, s2):
        l1, l2 = len(s1), len(s2)
        return self._calculate_ratio(min(l1, l2), l1 + l2)

    def quick_ratio(self, s1, s2):
        length = len(s1) + len(s2)
        intersect = collections.Counter(s1) & collections.Counter(s2)
        return self._calculate_ratio(sum(intersect.values()), length)

    def ratio(self, s1, s2):
        '''
        longest substr of s1 and s2
        '''
        l1, l2 = len(s1), len(s2)
        if l2 == 0:
            return 0.0

        max_len = 0
        dp = [0 for _ in range(l2)]
        for i in range(l1):
            left_up = 0
            for j in range(l2):
                up = dp[j]
                if s1[i] == s2[j]:
                    dp[j] = left_up + 1
                    max_len = max(max_len, dp[j])
                else:
                    dp[j] = 0
                left_up = up
        
        return self._calculate_ratio(max_len, l1 + l2)
    
    def get_ratios_for_pkgs(self, word, pkg_list, cutoff=SIM_THRESHOLD):
        result = []
        for x in pkg_list:
            score = 0.0
            if self.real_quick_ratio(word, x) >= cutoff and self.quick_ratio(word, x) >= cutoff:
                score = self.ratio(word, x)

            alias_name = self.pkg_alias_collections.get(x, None)
            if alias_name is not None:
                if self.real_quick_ratio(word, alias_name) >= cutoff and self.quick_ratio(word, alias_name) >= cutoff:
                    score = max(score, self.ratio(word, alias_name))

            if score >= cutoff:
                result.append((score, x))
                
        return result