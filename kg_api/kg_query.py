import neo4j


class QueryApplication(object):
    def __init__(self, uri='bolt://localhost:7687', user='neo4j', password='neo4j'):
        self.driver = neo4j.GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    @staticmethod
    def query_standard_libraries(tx):
        result = tx.run("MATCH (:Release)-[:has_module]->(n:Module) RETURN DISTINCT n.name;")
        ret = [record[0] for record in result]
        return ret
    
    @staticmethod
    def query_builtin_resources(tx):
        result = tx.run("MATCH (:Release)-[:has_attribute]->(n:Attribute) RETURN DISTINCT n.name;")
        ret = [record[0] for record in result]
        return ret
    
    @staticmethod
    def query_pvs4module(tx, module):
        result = tx.run("MATCH (p:Package)-[:has_version]->(v:Version)-[:has_module]->(m:Module {name:$module}) "
                        "RETURN p.name, v.name;", module=module)
        ret = {}
        for record in result:
            p, v = record
            if p not in ret:
                ret[p] = [v, ]
            else:
                ret[p].append(v)
                
        return ret

    @staticmethod
    def get_all_releases(tx):
        result = tx.run("MATCH (r:Release) RETURN r.release;")
        ret = [record[0] for record in result]
        return ret
    
    @staticmethod
    def get_all_packages(tx):
        result = tx.run("MATCH (p:Package)-[:has_version]->() RETURN DISTINCT p.name;")
        ret = [record[0] for record in result]
        return ret


    @staticmethod
    def get_standard_modules_by_module(tx, top_module, module_list, max_hop, ret_info):
        result = tx.run("MATCH (r:Release)-[:has_module]->(m:Module {name:$top_module})-[:has_module*0..%d]\
            ->(s:Module) WHERE s.name in $module_list RETURN r.release, id(m), s.name;" \
            % max_hop, top_module=top_module, module_list=module_list)

        for record in result:
            release, mid, module = record
            if release not in ret_info:
                ret_info[release] = {}
            if mid not in ret_info[release]:
                ret_info[release][mid] = []
            
            ret_info[release][mid].append(module)
    

    @staticmethod
    def get_third_modules_by_module(tx, top_module, module_list, max_hop):
        result = tx.run("MATCH (m:Module {name:$top_module})-[:has_module*0..%d]\
            ->(s:Module) WHERE s.name in $module_list RETURN id(m), s.name;" \
            % max_hop, top_module=top_module, module_list=module_list)

        ret = {}
        for record in result:
            mid, module = record
            if mid not in ret:
                ret[mid] = []
            
            ret[mid].append(module)
        
        return ret
    

    @staticmethod
    def get_submodules_by_module_id(tx, module_id, module_list, max_hop):
        result = tx.run("MATCH (m:Module)-[:has_module*0..%d]->(s:Module) \
            WHERE id(m)=$module_id AND s.name in $module_list \
            RETURN s;" \
            % max_hop, module_id=module_id, module_list=module_list)
        
        ret = {}
        for record in result:
            submodule = record[0]
            ret[submodule.id] = submodule['name']
        
        return ret


    @staticmethod
    def get_submodules_by_module_list(tx, mid_list, module_list, max_hop):
        result = tx.run("MATCH (m:Module)-[:has_module*0..%d]->(s:Module) \
            WHERE id(m) in $mid_list AND s.name in $module_list \
            RETURN id(m), id(s), s.name;" \
            % max_hop, mid_list=mid_list, module_list=module_list)
        
        ret = {}
        for record in result:
            mid, sid, name = record
            if mid not in ret:
                ret[mid] = [[], []]

            ret[mid][0].append(sid)
            ret[mid][1].append(name)
        
        return ret


    @staticmethod
    def get_attr_by_module_id_list(tx, module_id_list, attr_list):
        result = tx.run("MATCH (m:Module)-[:has_attribute]->(a1:Attribute) WHERE id(m) in $module_id_list AND a1.name in $attr_list "
                        "OPTIONAL MATCH (a1)-[:has_attribute]->(a2:Attribute) WHERE a2.name in $attr_list "
                        "RETURN m.name, a1.name, a2.name;", module_id_list=module_id_list, attr_list=attr_list)
        
        ret = []
        for record in result:
            module, cls, name = record
            
            attr = '{}.{}'.format(module, cls)
            ret.append(attr)

            if name is not None:
                ret.append('{}.{}'.format(attr, name))

        return set(ret)
    

    @staticmethod
    def get_attr_by_muilti_mid_list(tx, module_id_list, attr_list):
        result = tx.run("MATCH (m:Module)-[:has_attribute]->(a1:Attribute) WHERE id(m) in $module_id_list AND a1.name in $attr_list "
                        "OPTIONAL MATCH (a1)-[:has_attribute]->(a2:Attribute) WHERE a2.name in $attr_list "
                        "RETURN id(m), m.name, a1.name, a2.name;", module_id_list=module_id_list, attr_list=attr_list)
        
        ret = {}
        for record in result:
            mid, module, cls, name = record

            if mid not in ret:
                ret[mid] = []
            
            attr = '{}.{}'.format(module, cls)
            ret[mid].append(attr)

            if name is not None:
                ret[mid].append('{}.{}'.format(attr, name))

        ret = {k: set(v) for k,v in ret.items()}

        return ret

    
    @staticmethod
    def _get_attributes_by_release_and_seed(tx, release, func_list, attr_list):
        result = tx.run("MATCH (r:Release {release:$release})-[:has_attribute]->(a1:Attribute) WHERE a1.name in $func_list "
                        "OPTIONAL MATCH (a1)-[:has_attribute]->(a2:Attribute) WHERE a2.name in $attr_list "
                        "RETURN a1.name, a2.name", release=release, func_list=func_list, attr_list=attr_list)
        
        ret = []
        for record in result:
            cls, name  = record
            
            ret.append(cls)
            if name is not None:
                ret.append('{}.{}'.format(cls, name))
            
        return set(ret)

    @staticmethod
    def get_packages_and_versions_by_module(tx, module_id):
        result = tx.run("MATCH (p:Package)-[:has_version]->(v:Version)-[:has_module]->(m:Module) "
                        "WHERE id(m)=$module_id "
                        "MATCH (v)-[r:requires_lang]->() "
                        "RETURN p.name, v.version, r;", module_id=module_id)

        package, version, rel_obj = result.single()
        return package, [version, rel_obj['specifier'], rel_obj['repos_spec']]
    

    @staticmethod
    def get_versions_lang_by_package(tx, package):
        result = tx.run("MATCH (:Package {name:$package})-[:has_version]->(v:Version {removal:FALSE})-[r:requires_lang]->() "
                        "RETURN v.version, r;", package=package)
        
        ret = []
        for record in result:
            version, rel_obj = record
            ret.append([version, rel_obj['specifier'], rel_obj['repos_spec']])

        return ret
    

    @staticmethod
    def get_versions_by_package(tx, package):
        result = tx.run("MATCH (p:Package {name:$package})-[:has_version]->(v:Version {removal:FALSE}) "
                        "RETURN v.version;", package=package)
        
        ret = []
        for record in result:
            version = record[0]
            ret.append(version)
        
        return ret
    

    @staticmethod
    def get_direct_dependencies_by_package(tx, package):
        result = tx.run("MATCH (:Package {name:$package})-[:has_version]->()-[:requires_pkg]->(p:Package) "
                        "RETURN DISTINCT p.name;", package=package)
        
        ret = []
        for record in result:
            pkg = record[0]
            ret.append(pkg)
        
        return ret


    @staticmethod
    def get_requirements4version(tx, package, version):
        result = tx.run("MATCH (:Package {name:$name})-[:has_version]->(v:Version {version:$version}) "
                        "OPTIONAL MATCH (v)-[r:requires_pkg]->(p:Package) "
                        "RETURN r, p.name;", name=package, version=version)
        
        ret = []
        for record in result:
            rel, pkg_name = record
            if pkg_name:
                ret.append((pkg_name, rel))

        return ret


    @staticmethod
    def get_versions4package(tx, package):
        result = tx.run("MATCH (:Package {name:$name})-[:has_version]->(v:Version {removal:FALSE}) "
                        "OPTIONAL MATCH (v)-[r:requires_lang]->() "
                        "RETURN v, r;", name=package)
        
        ret = []
        for record in result:
            obj_version, rel = record
            ret.append((obj_version, rel))
        
        return ret
    

    '''
    Next part for analysis
    '''
    @staticmethod
    def exist_package(tx, package):
        result = tx.run("MATCH (p:Package {name:$name}) RETURN COUNT(p);", name=package)
        return result.single()[0] > 0

    @staticmethod
    def exist_module(tx, module):
        result = tx.run("MATCH (m:Module {name:$name}) RETURN COUNT(m);", name=module)
        return result.single()[0] > 0