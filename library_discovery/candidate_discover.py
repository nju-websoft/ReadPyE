import time
import sys
import neo4j
from packaging.specifiers import SpecifierSet
from packaging.version import parse
from packaging.utils import canonicalize_name

sys.path.append("..")
from kg_api.kg_query import QueryApplication
from utils.calculator import calculate_matching_degree
from utils.handle_unknown import get_similar_packages

# from utils.calculator import NoneSimilarity as RatioCalculator
from utils.calculator import NamingSimilarity as RatioCalculator                                         


def _get_leaves(node_set):
    # Get leaf nodes of the parse tree
    sorted_node = sorted(node_set, key=lambda x:len(x.split('.')))
    length = len(sorted_node)

    del_index = []
    for i in range(length):
        for j in range(i+1, length):
            if sorted_node[j].startswith(sorted_node[i]+'.'):
                del_index.append(i)
                break
    
    for index in reversed(del_index):
        sorted_node.pop(index)
    
    return sorted_node


class DiscoveryApplication(object):
    def __init__(self, kg_querier, standard_libs, builtin_funcs):
        self.kg_querier = kg_querier
        self.standard_libs = standard_libs
        self.builtin_funcs = builtin_funcs

        with self.kg_querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
            self.release_list = session.read_transaction(QueryApplication.get_all_releases)
        
        self.calculator = None
    

    def load_all_pks(self):
        with self.kg_querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
            pkg_collections = session.read_transaction(QueryApplication.get_all_packages)

        self.calculator = RatioCalculator(pkg_collections)
        return self.calculator
    

    def get_top_candidates(self, score_dict):
        max_score = max(score_dict.values())
        return [key for key, value in score_dict.items() if value == max_score]
    

    def generate_forest_info(self, parse_info):
        imported_modules = parse_info.get('imported_module', set())
        imported_resources = parse_info.get('imported_resource', set())
        imported_attrs = parse_info.get('imported_attr', set())

        # Step 1: For imported modules
        module_forest = {}      # {top_module: [modules, ]}
        min_module_dict = {}    # {top_module: int} Shortest module
        module_query_dict = {}  # {top_module: set(module prefix)}
        for item in imported_modules:
            split_item = item.split('.')
            top_module = split_item[0]
            if top_module not in module_forest:
                module_forest[top_module] = []
                module_query_dict[top_module] = set()
            module_forest[top_module].append(item)

            split_length = len(split_item)
            if top_module not in min_module_dict or split_length < min_module_dict[top_module]:
                min_module_dict[top_module] = split_length

            # split modules to module_set
            tmp = top_module
            module_query_dict[top_module].add(tmp)
            for i in range(1, split_length):
                tmp += '.' + split_item[i]
                module_query_dict[top_module].add(tmp)
        
        # save the longest items
        for key, value in module_forest.items():
            module_forest[key] = _get_leaves(value)
        
        # Step 2: For imported resources and imported attributes
        attr_forest = {}    # {top_module: [attrs, ]}
        attr_query_dict = {}    # {top_module: [set(module, ), set(name, )]} possible modules and names
        for item in imported_resources.union(imported_attrs):
            split_item = item.split('.')
            top_module = split_item[0]
            if top_module not in attr_forest:
                attr_forest[top_module] = []
            attr_forest[top_module].append(item)

            # split modules to module_set
            if top_module not in attr_query_dict:
                attr_query_dict[top_module] = [set(), set()]

            module_length = min_module_dict[top_module]
            tmp = '.'.join(split_item[:module_length])
            attr_query_dict[top_module][0].add(tmp)
            for i in range(module_length, len(split_item)):
                tmp += '.' + split_item[i]
                attr_query_dict[top_module][0].add(tmp)
                attr_query_dict[top_module][1].add(split_item[i])
        
        # save the longest attrs
        for key, value in attr_forest.items():
            attr_forest[key] = _get_leaves(value)
        
        return module_forest, module_query_dict, attr_forest, attr_query_dict
    

    def python_discovery(self, parse_info):

        module_forest, module_query_dict, attr_forest, attr_query_dict = self.generate_forest_info(parse_info)
        
        # Step 1: For imported modules
        
        python_module_info = {} # {release: {top_module id: [module]}}
        if module_forest:
            with self.kg_querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
                for key, value in module_forest.items():
                    session.read_transaction(QueryApplication.get_standard_modules_by_module, key, list(module_query_dict[key]), len(value[-1].split('.'))-1, python_module_info)
        else:
            python_module_info = {k: {} for k in self.release_list}

        # satify the Python syntax
        python_sytax = parse_info.get('python_syntax', set())
        if len(python_sytax) > 0:
            spec = SpecifierSet('')
            for item in python_sytax:
                spec &= item
            python_module_info = {k: v for k,v in python_module_info.items() if k in spec}

        if len(python_module_info) == 0:
            # no available Python releases
            return {}

        # Calculate the matching degree
        release_module_mapping = {}     # record the id of top module in the release: {release: {top_module: mid}}
        release_score = {}
        for release, module_info in python_module_info.items():
            release_module_mapping[release] = {}
            release_score[release] = 0.0
            for mid, spanning_tree in module_info.items():
                if spanning_tree:
                    top_module = spanning_tree[0].split('.')[0]
                    release_module_mapping[release][top_module] = mid
                    release_score[release] += calculate_matching_degree(spanning_tree, module_forest[top_module])
        
        candidate_releases = self.get_top_candidates(release_score)

        # save the mappings of candidate Python versions
        release_module_mapping = {k: v for k, v in release_module_mapping.items() if k in candidate_releases}

        # Step 2: For imported resources and imported attributes
        
        ## trees of modules for the candidate libraries
        python_attr_info = {}   # {release: {mid: set(name, )}}
        with self.kg_querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
            for key, value in attr_query_dict.items():
                module_value, name_value = value

                for release in candidate_releases:
                    if key not in release_module_mapping[release]:
                        continue
                    if release not in python_attr_info:
                        python_attr_info[release] = {}
                    
                    mid = release_module_mapping[release][key]
                    # longest module, e.g. numpy.linalg.info, has_module*0..2
                    max_hop = max([len(item.split('.')) for item in module_value]) - 1

                    # query all submodules and their ids
                    submodule_dict = session.read_transaction(QueryApplication.get_submodules_by_module_id, mid, list(module_value), max_hop)
                    
                    # query attrs for all submodules
                    attr_info = session.read_transaction(QueryApplication.get_attr_by_module_id_list, list(submodule_dict), list(name_value))
                    
                    python_attr_info[release][mid] = set(submodule_dict.values()) | attr_info

        # Calculate the matching degree
        standard_attr_score = {}
        for release in python_attr_info:
            standard_attr_score[release] = 0.0
            for top_module in attr_forest:
                if top_module not in release_module_mapping[release]:
                    continue

                mid = release_module_mapping[release][top_module]
                spanning_tree = python_attr_info[release][mid]
                standard_attr_score[release] += calculate_matching_degree(spanning_tree, attr_forest[top_module])

        # Step 3: built-in functions
        builtin_attr_score = {}
        
        builtin_attrs = parse_info.get('builtin_attr', set())
        leaf_attr = _get_leaves(builtin_attrs)
        if len(leaf_attr) > 0:
            top_attrs = set()
            name_set = set()
            for item in leaf_attr:
                split_item = item.split('.')
                top_attrs.add(split_item[0])
                for i in range(1, len(split_item)):
                    name_set.add(split_item[i])
            
            with self.kg_querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
                for release in python_attr_info:
                    spanning_tree = session.read_transaction(QueryApplication._get_attributes_by_release_and_seed, release, list(top_attrs), list(name_set))
                    builtin_attr_score[release] = calculate_matching_degree(spanning_tree, leaf_attr)

        # Sum: imported attrs and built-in functions
        release_score = {x: standard_attr_score.get(x, 0.0)+builtin_attr_score.get(x, 0.0) for x in candidate_releases}

        return release_score
    

    def python_whole_steps(self, parse_info):
        release_score = self.python_discovery(parse_info)
        
        # sort by matching degree, then by version
        candidate_releases = sorted(release_score, key=lambda x: (release_score[x], parse(x)), reverse=True)
        # candidate_releases = self.get_top_candidates(release_score)

        return candidate_releases
    

    def third_discovery(self, parse_info):
        module_forest, module_query_dict, attr_forest, attr_query_dict = self.generate_forest_info(parse_info)

        module_info = {}
        with self.kg_querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
            for key, value in module_forest.items():
                module_info[key] = session.read_transaction(QueryApplication.get_third_modules_by_module, key, list(module_query_dict[key]), len(value[-1].split('.'))-1)

        # Calculate the matching degree
        candidate_top_modules = {}  # {top_module: [mid, ]}
        unknown_modules = []        # [top_module, 
        for top_module, forest in module_info.items():
            if len(forest) == 0:
                # the modules are not in KG
                unknown_modules.append(top_module)
                continue

            module_score = {}
            for mid, spanning_tree in forest.items():
                module_score[mid] = calculate_matching_degree(spanning_tree, module_forest[top_module])

            candidate_top_modules[top_module] = self.get_top_candidates(module_score)

        # Step 2: For imported resources and imported attributes

        # filter the modules that are not in the KG
        attr_query_dict = {k: v for k, v in attr_query_dict.items() if k in candidate_top_modules}

        ## trees of modules for the candidate libraries
        third_attr_info = {}
        with self.kg_querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
            for key, value in attr_query_dict.items():
                candidate_mids = candidate_top_modules[key]

                module_value, name_value = value
                
                max_hop = max([len(item.split('.')) for item in module_value]) - 1

                # submodules 
                submodule_dict = session.read_transaction(QueryApplication.get_submodules_by_module_list, candidate_mids, list(module_value), max_hop)
                
                # all submodule ids
                mid_list = []
                for module_tuple in submodule_dict.values():
                    mid_list.extend(module_tuple[0])
                
                attr_info = session.read_transaction(QueryApplication.get_attr_by_muilti_mid_list, list(set(mid_list)), list(name_value))
                
                for mid in candidate_mids:
                    tmp = []
                    if mid in submodule_dict:
                        mid_list, module_names = submodule_dict[mid]

                        tmp.extend(module_names)
                        for item in mid_list:
                            if item in attr_info:
                                tmp.extend(attr_info[item])

                    third_attr_info[mid] = tmp

        # Calculate the matching degree
        # {top module: {pkg: [(version, spec, repos_spec, matching_degree), ]}}
        candidate_pvs = {}
        # {top_module: {pkg: similarity}}
        pkg_module_dict = {}
        with self.kg_querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
            for top_module, mid_list in candidate_top_modules.items():
                # {pkg: [(version_obj, spec, repos_spec, matching_degree), ]}
                pv_tmp = {}
                similarity_tmp = {}

                cname = canonicalize_name(top_module)
                for mid in mid_list:
                    if top_module in attr_forest:
                        matching_degree = calculate_matching_degree(third_attr_info[mid], attr_forest[top_module])
                    else:
                        matching_degree = 0.0

                    pkg, v_info = session.read_transaction(QueryApplication.get_packages_and_versions_by_module, mid)
                    v_info.append(matching_degree)

                    if pkg not in pv_tmp:
                        pv_tmp[pkg] = []
                        score = self.calculator.max_ratio(cname, pkg)
                        if score == 1.0 and pkg != cname:
                            # distinguish the same name
                            score = 0.99
                        similarity_tmp[pkg] = score

                    pv_tmp[pkg].append(v_info)
                
                # sort versions by matching_degree, then by version
                for v_info in pv_tmp.values():
                    v_info.sort(key=lambda x:(x[-1], parse(x[0])), reverse=True)

                candidate_pvs[top_module] = pv_tmp
                pkg_module_dict[top_module] = similarity_tmp
        
        return candidate_pvs, pkg_module_dict, unknown_modules
    

    def third_whole_steps(self, parse_info):
        candidate_pvs, pkg_module_dict, unknown_modules = self.third_discovery(parse_info)

        # For unknown modules
        unknown_candidate_pvs, unknown_pkg_module_dict = get_similar_packages(self.kg_querier, self.calculator, unknown_modules)

        candidate_pvs.update(unknown_candidate_pvs)
        pkg_module_dict.update(unknown_pkg_module_dict)

        return candidate_pvs, pkg_module_dict

    

    def discover(self, python_parse_info, third_parse_info):
        candidate_releases = self.python_whole_steps(python_parse_info)
        third_candidates = self.third_whole_steps(third_parse_info)

        return candidate_releases, third_candidates