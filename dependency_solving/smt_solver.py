from z3 import Optimize, Sum, Or, Bool, Int, Real, If


class DepOptimizer(object):
    def __init__(self):
        self.node_id = None

    
    def main(self, pkg_dict, similarity_dict):
        # {top_module: [pkg, ]}
        if len(pkg_dict) == 0:
            # no third-party packages
            return []

        opt_pkgs = {}   # candidate packages
        
        # special handle: for the modules like "tests" ...
        tmp_dict = {}
        MAX_LIMIT = 20

        # add constraints
        solver = Optimize()
        for top_module, pkg_list in pkg_dict.items():
            if len(pkg_list) == 0:
                # no candidate packages for one top module
                return None

            or_pkgs = []
            if len(pkg_list) >= MAX_LIMIT:
                max_pkg = max(pkg_list, key=lambda x:similarity_dict[top_module][x])
                tmp_dict[top_module] = [max_pkg, ]
            else:
                tmp_dict[top_module] = pkg_list

            for package in tmp_dict[top_module]:
                if package not in opt_pkgs:
                    opt_pkgs[package] = Bool(package)

                package_var = opt_pkgs[package]
                or_pkgs.append(package_var)
            
            # one module has at least one candidate package
            if len(or_pkgs) > 1:
                or_stat = Or(or_pkgs)
                solver.add(or_stat)
            else:
                solver.add(or_pkgs[0])
        
        # Objective 2: similarity
        sum_list = []
        for top_module, pkg_list in tmp_dict.items():
            for package in pkg_list:
                sum_list.append(If(opt_pkgs[package], similarity_dict[top_module][package], 0))
        
        o2 = Real('$O2')
        solver.add(o2 == -Sum(sum_list))
        
        # Objective 1
        o1 = Int('$O1')
        solver.add(o1 == 2*Sum([If(x, 1, 0) for x in opt_pkgs.values()]))
        solver.minimize(Sum(o1, o2))

        ret = []
        if solver.check():
            result = solver.model()

            for pkg, pkg_var in opt_pkgs.items():
                if result[pkg_var]:
                    ret.append(pkg)
            
        else:
            for top_module, pkg_list in tmp_dict.items():
                ret.append(max(pkg_list, key=lambda x:similarity_dict[top_module][x]))
        
        return ret
