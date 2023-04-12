# File directory structure, import relationships
from .pyfile_parse import PythonParser
import os
import re


def _find_prefix_items(prefixes, src_list):
    ret = set()
    if len(prefixes) == 0:
        return ret 

    for item in src_list:
        # is or startswith one prefix
        if item in prefixes:
            ret.add(item)
        else:
            for prefix in prefixes:
                if item.startswith(prefix+'.'):
                    ret.add(item)
                    break
    
    return ret


class projectParser(object):
    def __init__(self, languages_dir, standard_libs, builtin_funcs):
        self.standard_libs = standard_libs
        self.pyfile_parser = PythonParser(languages_dir, builtin_funcs)
        self.iden_pattern = re.compile(r'[^\w\-]')
    

    def _clear_relative_resources(self, info_dict, local_modules=None):
        # remove local modules
        module_dict = {}
        for module in info_dict['imported_module']:
            top_module = module.split('.')[0]
            if top_module not in module_dict:
                module_dict[top_module] = []
            module_dict[top_module].append(module)
        
        # relative import
        local_top_module = {''}
        if local_modules is not None:
            for top_module, module_list in module_dict.items():
                is_local = True
                # check if all modules are local
                for module in module_list:
                    if module not in local_modules:
                        is_local = False
                        break
                
                if is_local:
                    local_top_module.add(top_module)

        for name_set in info_dict.values():
            for item in list(name_set):
                top_module = item.split('.')[0]
                if top_module in local_top_module:
                    # local modules
                    name_set.remove(item)


    def _split_parse_info(self, parse_info):
        imported_modules = parse_info['imported_module']
        imported_resources = parse_info['imported_resource']
        imported_attrs = parse_info['imported_attr']
        builtin_attrs = parse_info['builtin_attr']
        python_sytax = parse_info['python_syntax']

        top_modules = set()
        for item in imported_modules:
            tmp = item.split('.')[0]
            if len(tmp) > 0:
                top_modules.add(tmp)
        
        # Get the standard top module in the code
        stand_prefix = top_modules & self.standard_libs

        imported_stand_modules = _find_prefix_items(stand_prefix, imported_modules)
        imported_stand_resources = _find_prefix_items(stand_prefix, imported_resources)
        imported_stand_attrs = _find_prefix_items(stand_prefix, imported_attrs)

        python_parse_info = {'imported_module': imported_stand_modules, 'imported_resource': imported_stand_resources,\
                            'imported_attr': imported_stand_attrs, 'builtin_attr': builtin_attrs, 'python_syntax': python_sytax}
        
        third_parse_info = {'imported_module': imported_modules - imported_stand_modules,\
                            'imported_resource': imported_resources - imported_stand_resources,\
                            'imported_attr': imported_attrs - imported_stand_attrs}
        
        return python_parse_info, third_parse_info


    def _get_module_name(self, fpath, isfile=False):
        if len(fpath) == 0:
            return None
        
        ret = fpath.replace(os.sep, '.')
        if isfile:
            ret = ret[:-3]

        return ret


    def _get_all_local_module_name(self, root_dir):
        # Get all Python files and local module names
        dir_list = [root_dir,]
        py_files = []
        module_list = []

        base_path = os.path.dirname(root_dir)
        index = len(base_path) + 1
        while len(dir_list) > 0:
            py_dir = dir_list.pop()
            for item in os.listdir(py_dir):
                fpath = os.path.join(py_dir, item)
                if os.path.isdir(fpath):
                    # dir
                    if re.search(self.iden_pattern, item) is None:
                        dir_list.append(fpath)
                        module_list.append(self._get_module_name(fpath[index:]))

                elif os.path.isfile(fpath):
                    if fpath.endswith('.py') or fpath.endswith('.so'):
                        # py file
                        py_name = item[:-3]
                        if re.search(self.iden_pattern, py_name) is None:
                            if not py_name.startswith('__'):
                                # a module
                                module_list.append(self._get_module_name(fpath[index:], True))

                            if fpath.endswith('.py'):
                                py_files.append(fpath)

        # generate all partial module names
        ret = set()
        for module in module_list:
            split_info = module.split('.')
            length = len(split_info)
            for i in range(length):
                tmp = split_info[i]
                ret.add(tmp)
                for j in range(i+1, length):
                    tmp = f'{tmp}.{split_info[j]}'
                    ret.add(tmp)

        return py_files, ret
    

    def parse(self, source_code, not_file=False):
        project_path = os.path.abspath(source_code)

        parse_info = {'imported_module': set(), 'imported_resource': set(), 'imported_attr': set(), 'builtin_attr': set(), 'python_syntax': set()}
        local_modules = None

        if not_file:
            # only string
            parse_info = self.pyfile_parser.parse(source_code, not_file)

        elif os.path.isdir(project_path):
            # directory
            py_files, local_modules = self._get_all_local_module_name(project_path)
            parse_dict = {}
            for fpath in py_files:
                # all python files
                parse_dict[fpath] = self.pyfile_parser.parse(fpath)
            
            for value in parse_dict.values():
                if value:
                    for key in parse_info:
                        parse_info[key] |= value[key]

        elif os.path.isfile(project_path) and project_path.endswith('.py'):
            # single Python file
            parse_info = self.pyfile_parser.parse(project_path)
        
        # split to Python-related and thir-related info
        python_parse_info, third_parse_info = self._split_parse_info(parse_info)
        
        # remove local modules from the third-party info
        self._clear_relative_resources(third_parse_info, local_modules)

        return python_parse_info, third_parse_info
