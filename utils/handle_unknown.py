import neo4j
import math
from heapq import nlargest as _nlargest
import math
from multiprocessing import Pool
from packaging.version import parse
from packaging.utils import canonicalize_name
import sys
sys.path.append("..")
from kg_api.kg_query import QueryApplication
from .variables import CANDIDATE_NUM


def get_close_matches(calculator, word, n, process_num=4):
    result = []

    seg_num = math.ceil(len(calculator.pkg_collections) / process_num)
    process_res = []
    process_pool = Pool(process_num)

    cname = canonicalize_name(word)

    for i in range(process_num):
        # split to each thread
        split_list = calculator.pkg_collections[seg_num*i:seg_num*(i+1)]
        process_res.append(process_pool.apply_async(calculator.get_ratios_for_pkgs, args=(cname, split_list)))

    process_pool.close()
    process_pool.join()

    for item in process_res:
        result.extend(item.get())

    # Move the best scorers to head of list
    result = _nlargest(n, result)

    # check the same name
    if cname in calculator.pkg_alias_collections and cname not in [x[1] for x in result]:
        result.pop()
        result.insert(0, (1.0, cname))
    
    return result

# def get_close_matches(calculator, word, n=5, process_num=4):
#     cname = canonicalize_name(word)
#     if cname in calculator.pkg_alias_collections:
#         return [(1.0, cname), ]
#     return []



def get_similar_packages(kg_querier, calculator, unknown_modules):
    candidate_pvs = {}  # {top module: {pkg: [(version, spec, repos_spec, matching_degree), ]}}
    pkg_module_dict = {}    # {top_module: {pkg: similarity}}

    with kg_querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
        for top_module in unknown_modules:
            cname = canonicalize_name(top_module)

            # possible packages
            pkg_list = get_close_matches(calculator, cname, CANDIDATE_NUM)
            tmp = {}
            similarity_tmp = {}
            for score, pkg in pkg_list:
                # all versions of the package
                v_info = session.read_transaction(QueryApplication.get_versions_lang_by_package, pkg)
                if len(v_info) > 0:
                    if pkg not in tmp:
                        tmp[pkg] = []
                        similarity_tmp[pkg] = score

                    for v_item in v_info:
                        if score == 1.0 and pkg != cname:
                            # distinguish the same name
                            score = 0.99
                        v_item.append(score)
                        tmp[pkg].append(v_item)
                        
            if len(tmp) > 0:
                # sort versions by version
                for v_info in tmp.values():
                    v_info.sort(key=lambda x:parse(x[0]), reverse=True)
                
                candidate_pvs[top_module] = tmp
                pkg_module_dict[top_module] = similarity_tmp
    
    return candidate_pvs, pkg_module_dict