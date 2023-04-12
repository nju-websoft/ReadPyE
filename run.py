import os
import time
import sys
import neo4j
import json
import argparse
from python_parser.project_parser import projectParser
from library_discovery.candidate_discover import DiscoveryApplication
from dependency_solving.generate_env import EnvGenerator
from kg_api.kg_query import QueryApplication

from env_validation.template import match_templates
from env_validation.validate import Validator
from utils.handle_unknown import get_similar_packages
from utils.variables import VALIDATION_NUM, NEO4J_URI, NEO4J_USER, NEO4J_PWD


class AutomaticInference(object):
    def __init__(self, languages_dir):
        self.kg_querier = QueryApplication(NEO4J_URI, NEO4J_USER, NEO4J_PWD)

        with self.kg_querier.driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
            standard_libs = set(session.read_transaction(QueryApplication.query_standard_libraries))
            builtin_funcs = set(session.read_transaction(QueryApplication.query_builtin_resources))

        self.code_parser = projectParser(languages_dir, standard_libs, builtin_funcs)
        self.candidate_discovery = DiscoveryApplication(self.kg_querier, standard_libs, builtin_funcs)
        self.ratio_calculator = self.candidate_discovery.load_all_pks()
        self.env_generator = EnvGenerator(self.kg_querier, self.ratio_calculator)
        self.env_validator = Validator()

        self.val_stime = None
        self.infer_res = []

        self.related_exceptions = {'ImportError', 'ModuleNotFoundError', 'SyntaxError', 'AttributeError'}
    

    def _clean_states(self):
        self.exps_count = [0, 0, 0] # ImportError, SyntaxError, AttributeError
        self.adjust_count = [0, 0]  # Python, third-party

    
    def close(self):
        self.kg_querier.close()
        self.env_validator.close()


    def has_related_exceptions(self, logs):
        for exc in self.related_exceptions:
            if f'{exc}: ' in logs:
                return True
        
        return False


    def is_empty_info(self, parse_info):

        if 'imported_module' in parse_info and len(parse_info['imported_module']) > 0:
            return False
        
        if 'builtin_attr' in parse_info and len(parse_info['builtin_attr']) > 0:
            return False
        
        return True

    
    def check_parse_info(self, parse_info, check_info):
        ret = {}

        module, attr = check_info
        if module is not None:
            # check module
            imported_modules = parse_info['imported_module']
            if module in imported_modules:
                ret['imported_module'] = {module}
                return ret
            
            part_module = f'.{module}'
            for item in imported_modules:
                if part_module in item:
                    ret['imported_module'] = {item}
                    return ret
        
        else:
            # check attr
            imported_resources = parse_info['imported_resource']
            part_attr = f'.{attr}'
            for item in imported_resources:
                if item.endswith(part_attr):
                    ret['imported_module'] = {item[:-len(part_attr)]}
                    ret['imported_resource'] = {item}
                    return ret
        
        return ret
    

    def handle_match_info(self, match_info):
        # Get parse info for the matched info
        if match_info is None:
            return None

        exception_type = match_info['exception']
        check_info = match_info.get('check', None)

        for code_item in match_info['code_list']:
            python_parse_info, third_parse_info = self.code_parser.parse(code_item, not_file=True)

            if exception_type == 'SyntaxError':
                if len(python_parse_info['python_syntax']) > 0:
                    return python_parse_info, {}
            
            else:
                if check_info is not None:
                    python_parse_info = self.check_parse_info(python_parse_info, check_info)
                    third_parse_info = self.check_parse_info(third_parse_info, check_info)
                
                if self.is_empty_info(python_parse_info) and self.is_empty_info(third_parse_info):
                    # no info
                    continue
                
                return python_parse_info, third_parse_info
        
        return None
    

    def adjust_candidate_env(self, logs, build_info=None, parent=None):
        # match templates
        match_pattern = match_templates(logs)
        if match_pattern is not None:
            exp = match_pattern['exception']
            if exp == 'ImportError':
                self.exps_count[0] += 1
            elif exp == 'SyntaxError':
                self.exps_count[1] += 1
            elif exp == 'AttributeError':
                self.exps_count[2] += 1

        parse_info = self.handle_match_info(match_pattern)

        if parse_info is None:
            return False
        
        ret = False

        # Handle matched parse info
        python_parse_info, third_parse_info = parse_info
        if not self.is_empty_info(python_parse_info):
            # For Python
            release_score = self.candidate_discovery.python_discovery(python_parse_info)
            python_candidates = self.candidate_discovery.get_top_candidates(release_score)
            ret = self.env_generator.add_python_constraint(python_candidates)
            if ret:
                self.adjust_count[0] += 1

        else:
            # For third-party package
            module_forest = self.candidate_discovery.generate_forest_info(third_parse_info)[0]

            if build_info is not None:
                # check build logs
                build_fail_logs = self.env_validator.comment_cmds

                for top_module in module_forest:
                    installed_pkgs = self.env_generator.installed_module_pkgs.get(top_module, set())
                    if len(installed_pkgs) > 0 and len(installed_pkgs - set(build_fail_logs)) == 0:
                        # all pkgs for this top_module are failed to be installed
                        for pkg in installed_pkgs:
                            if self.adjust_candidate_env(build_fail_logs[pkg], parent=pkg):
                                return True

            # reselect env
            candidate_pvs, pkg_module_dict, unknown_modules = self.candidate_discovery.third_discovery(third_parse_info)
            # only save the max pvs
            for top_module, pv_info in candidate_pvs.items():
                # Get maximum mathing degree
                max_score = 0
                for pkg, v_list in pv_info.items():
                    max_score = max(max_score, v_list[0][-1])
                
                # Get pv with maximum mathing degree
                del_pkgs = []
                for pkg, v_list in pv_info.items():
                    index = 0
                    while index < len(v_list):
                        item = v_list[index]
                        if item[-1] != max_score:
                            pv_info[pkg] = v_list[:index]
                            break
                        index += 1
                    
                    # remove other pvs in pv_candidates
                    if index == 0:
                        del_pkgs.append(pkg)
                
                for pkg in del_pkgs:
                    pv_info.pop(pkg)
                    pkg_module_dict[top_module].pop(pkg)
            
            # For unknown modules
            unknown_candidate_pvs, unknown_pkg_module_dict = get_similar_packages(self.kg_querier, self.ratio_calculator, unknown_modules)
            candidate_pvs.update(unknown_candidate_pvs)
            pkg_module_dict.update(unknown_pkg_module_dict)

            # add new candidates to generate environment
            ret = self.env_generator.add_pv_constraint(candidate_pvs, pkg_module_dict, parent)
            if ret:
                self.adjust_count[1] += 1
        
        return ret

    
    def env_iterate(self, install_info, dockerfile_dir, source_name, cmd, extra_cmd=None, count=0):
        pyver, install_list = install_info

        validation_res = self.env_validator.validate_inferred_env(pyver, install_list, dockerfile_dir, source_name, cmd, extra_cmd)

        if count > 0:
            self.infer_res.append([install_info, round(time.time() - self.val_stime, 3)])

        build_info, exec_info = validation_res
        if exec_info is None:
            return False, install_info, count
        
        if exec_info['status'] == 0 or not self.has_related_exceptions(exec_info['log']):
            return True, install_info, count
        
        if count < VALIDATION_NUM:
            if self.adjust_candidate_env(exec_info['log'], build_info):
                # adjust success
                new_install_info = self.env_generator.generate_candidate_environment()
                if new_install_info is not None:
                    # validate the new environment
                    return self.env_iterate(new_install_info, dockerfile_dir, source_name, cmd, extra_cmd, count+1)
        
            elif pyver.startswith('3') and self.env_generator.add_python_constraint('<3'):
                new_install_info = self.env_generator.generate_candidate_environment()
                if new_install_info is not None:
                    # validate the new environment
                    self.adjust_count[0] += 1
                    return self.env_iterate(new_install_info, dockerfile_dir, source_name, cmd, extra_cmd, count+1)
        
        return False, install_info, count
    

    def clean_dockerfile(self, dockerfile_dir):
        dockerfile_path = os.path.join(dockerfile_dir, 'Dockerfile')
        if os.path.isfile(dockerfile_path):
            os.remove(dockerfile_path)
        
    

    def main(self, src_path, validation_setting=None, existing_env=None):
        '''
        validation_setting: {}
        existing_env: (pyver, {pkg: version})
        '''
        time_list = [0.0, 0.0, 0.0, 0.0]
        src_path = os.path.abspath(src_path)

        stime = time.time()
        python_parse_info, third_parse_info = self.code_parser.parse(src_path)
        time_list[0] = round(time.time() - stime, 3)

        # For the fairness of experiments
        python_parse_info['python_syntax'].add('<3.10')

        install_info = None
        # [release, ], {top_module: {package: [version_obj, ]}}
        stime = time.time()
        python_candidates, third_candidates = self.candidate_discovery.discover(python_parse_info, third_parse_info)
        time_list[1] = round(time.time() - stime, 3)

        validation_info = None
        stime = time.time()
        if self.env_generator.set_candidates(python_candidates, third_candidates, existing_env):
            install_info = self.env_generator.generate_candidate_environment()
            time_list[2] = round(time.time() - stime, 3)

            if VALIDATION_NUM > 0 and validation_setting is not None and install_info is not None:
                stime = time.time()
                # the parms for validation
                dockerfile_dir = validation_setting['dockerfile_dir']
                source_name = validation_setting['source_name']
                cmd = validation_setting['cmd']
                extra_cmd = validation_setting.get('extra_cmd', None)

                self.val_stime = stime
                self.infer_res = [[install_info, 0.0],]

                self._clean_states()
                status, success_env, count = self.env_iterate(install_info, dockerfile_dir, source_name, cmd, extra_cmd)
                time_list[3] = round(time.time() - stime, 3)
                
                self.env_validator.clean_dangling_images()
                self.clean_dockerfile(dockerfile_dir)

                validation_info = {'success': status, 'count': count, 'env': success_env, 'exps': self.exps_count, 'adjust': self.adjust_count, 'all-res': self.infer_res}
                
        return install_info, time_list, validation_info


def generate_dockerfile(env):
    pyver, install_list = env
    ret = []
    ret.append(f'FROM python:{pyver}')
    if len(install_list) > 0:
        ret.append('RUN pip install --upgrade pip')
        for item in install_list:
            ret.append(f'RUN pip install {item}')
    
    ret.append('# Please complete the execution commands')
    return '\n'.join(ret)


if __name__ == '__main__':
    # argparse
    parser = argparse.ArgumentParser(description='Runtime environment inference for Python programs.')

    parser.add_argument('--langdir', '-l', help='The language dir for tree-sitter.')
    parser.add_argument('--program', '-p', help='The target program.')

    parser.add_argument('--setting', '-s', help='Option: the Json file of validation settings.')
    parser.add_argument('--output', '-o', help='Option: the output file.')
    parser.add_argument('--env', '-e', help='Option: the Json file of local environments for code integration.')

    parse_res = vars(parser.parse_args(sys.argv[1:]))

    # check necessary arguments
    if parse_res['langdir'] is None or parse_res['program'] is None:
        print("Arguments '--langdir' and  '--program' are necessary.")
        exit(-1)
    
    lang_dir = os.path.abspath(parse_res['langdir'])
    program_path = os.path.abspath(parse_res['program'])

    validation_setting = None
    setting_file = parse_res['setting']
    if setting_file is not None and os.path.isfile(setting_file):
        with open(setting_file, 'r') as f:
            validation_setting = json.load(f)

        if len(set(validation_setting) & {'dockerfile_dir', 'source_name', 'cmd'}) < 3:
            print('Unvalid Json file of validation settings.')
            validation_setting = None

    local_env = None
    env_file = parse_res['env']
    if env_file is not None and os.path.isfile(env_file):
        with open(env_file, 'r') as f:
            local_env = json.load(f)
    
    # One-time use via the command line is inefficient, as some resources are required to be loaded.
    obj = AutomaticInference(lang_dir)
    install_info, _, validation_info = obj.main(program_path, validation_setting, local_env)
    obj.close()

    if validation_info is None:
        inferred_env = install_info
    else:
        inferred_env = validation_info['env']
    
    if inferred_env is None:
        print(f'Fail to infer runtime environment for {program_path}!')
    else:
        dockerfile_content = generate_dockerfile(inferred_env)

        outpath = parse_res['output']
        if outpath is None:
            print(dockerfile_content)
        else:
            with open(outpath, 'w') as f:
                f.write(dockerfile_content)
