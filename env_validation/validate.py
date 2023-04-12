import os
import docker
import json
import time
import subprocess
import shlex
import re


class Validator(object):
    def __init__(self):
        # The latest pip version that supports the Python version
        self.pip_version = {'2.7': '20.3.4', '3.2': '7.1.2', '3.3': '10.0.1', '3.4': '19.1.1', '3.5': '20.3.4', '3.6': '21.3.1', '3.7': '22.2.2', '3.8': '22.2.2', '3.9': '22.2.2', '3.10': '22.2.2'}

        self._client = docker.from_env()
        self._low_client = docker.APIClient('unix:///var/run/docker.sock')
        self._image_tag = 'python:validation'

        # timeout limit for one package installation: do not validate for the overlong installations
        self.INSTALL_TIMEOUT = 1200

        # num limit for dangling images
        self.DANGLING_LIMIT = 20

        self.installed_pattern = re.compile(r'pip install (?P<pkg>\S+?)==(?P<ver>\S+?)')
        self.comment_cmds = {}

        self.network_count = 0
    

    def close(self):
        self._client.close()
        self._low_client.close()
    

    def clean_dangling_images(self):
        self._client.images.prune(filters={'dangling': True})


    def _generate_dockerfile(self, dockerfile_path, pyver, install_list, source_name, cmd, extra_cmd=None):
        minor_version = pyver[:-len(pyver.split('.')[-1])-1]
        with open(dockerfile_path, 'w') as f:
            f.write(f'FROM python:{pyver}\n')
            if len(install_list) > 0:
                
                f.write(f'RUN pip install --upgrade pip=={self.pip_version[minor_version]}')
                f.write(' \\\n\t&& pip config set global.timeout 120\n')
                
                for item in install_list:
                    f.write(f'RUN timeout {self.INSTALL_TIMEOUT} pip install {item}\n')
            
            f.write(f'ADD {source_name} {source_name}\n')
            
            if extra_cmd is not None:
                for line in extra_cmd:
                    if not line.startswith("RUN "):
                        f.write(f'RUN {line}\n')
                    else:
                        f.write(f'{line}\n')

            f.write(f'CMD {cmd}')


    def _parse_logs(self, dockerfile_path, output):
        info_dict = json.loads(output[-1])

        if 'error' in info_dict:
            # check the timeout for pip install
            error_code = info_dict['errorDetail'].get('code', None)
            if error_code == 124:
                return 'Timeout'

            source_error = False
            error_log = ''
            for i in range(2, len(output)+1):
                line_info = json.loads(output[-i])['stream']
                error_log = line_info + error_log

                if 'ERROR: ResolutionImpossible:' in line_info:
                    return 'Conflict'
                
                elif 'ReadTimeoutError: HTTPSConnectionPool' in line_info:
                    return 'NetworkError'

                elif 'ERROR: THESE PACKAGES DO NOT MATCH THE HASHES' in line_info:
                    return 'NetworkError'
                
                elif 'ERROR: HTTP error 403 while getting' in line_info:
                    source_error = True

                elif line_info.startswith('Step '):
                    command_info = line_info.split('RUN')
                    if len(command_info) != 2:
                        return 'BuildFail'

                    # Comment on the statement with errors
                    command = command_info[1].strip()
                    match_obj = re.search(self.installed_pattern, command)
                    if match_obj is None:
                        return 'ParseFail'

                    # complete command
                    cmd_line = 'RUN {}'.format(command)
                    with open(dockerfile_path, 'r') as f:
                        lines = f.readlines()
                    
                    for i, line in enumerate(lines):
                        if line.startswith('ADD '):
                            # avoid commenting the extra cmds
                            return 'BuildFail'

                        if line.strip() == cmd_line:
                            status = None
                            if source_error:
                                # HTTP 403
                                return 'Skip'
                            else:
                                # comment
                                self.comment_cmds[match_obj.group('pkg')] = error_log
                                lines[i] = f'# {line}'
                                status = 'PackageFail'
                            
                            with open(dockerfile_path, 'w') as f:
                                for line in lines:
                                    f.write(line)

                            return status
                    
                    return 'BuildFail'
            
            return 'ParseFail'
            
        else:
            return 'Success'
    

    def _build_image(self, dockerfile_dir, count=0):
        ret = {count: {}}

        response = self._low_client.build(tag=self._image_tag, path=dockerfile_dir, dockerfile='Dockerfile', forcerm=True, network_mode='host')
        output = [line.decode(encoding='UTF-8', errors='ignore') for line in response]

        status = self._parse_logs(os.path.join(dockerfile_dir, 'Dockerfile'), output)
        ret[count]['status'] = status

        count += 1
        if status in ['PackageFail', 'NetworkError', 'HttpError']:
            if status == 'NetworkError':
                self.network_count += 1
                if self.network_count > 3:
                    return ret

            sub_ret = self._build_image(dockerfile_dir, count)
            ret.update(sub_ret)
        
        return ret
    

    def _run_container(self):
        # run the container
        container = self._client.containers.run(image=self._image_tag, detach=True, network_mode='host', mem_limit='8g')
        exit_code = -124
        try:
            exit_code = container.wait(timeout=60, condition='not-running')['StatusCode']
        except Exception:
            exit_code = 'Timeout'
            container.kill()
            time.sleep(15)
        finally:
            # get output from container
            run_logs = None
            try:
                run_logs = container.logs(stdout=True, stderr=True, tail=1000).decode(encoding='UTF-8', errors='ignore').strip()
            except docker.errors.APIError:
                pass

        container.remove(v=True, force=True)

        # judge the dangling images
        dangling_images = self._client.images.list(all=True, filters={'dangling': True})
        if len(dangling_images) >= self.DANGLING_LIMIT:
            self.clean_dangling_images()

        # save untagged parents for next validation
        self._client.images.remove(image=self._image_tag, noprune=True)

        return {'status': exit_code, 'log': run_logs}


    def validate_inferred_env(self, pyver, install_list, dockerfile_dir, source_name, cmd, extra_cmd=None):
        self.comment_cmds = {}
        self.network_count = 0
        
        # build image
        dockerfile_path = os.path.join(dockerfile_dir, 'Dockerfile')
        self._generate_dockerfile(dockerfile_path, pyver, install_list, source_name, cmd, extra_cmd)

        build_info = self._build_image(dockerfile_dir)

        if os.path.isfile(dockerfile_path):
            os.remove(dockerfile_path)
        
        exec_info = None
        for value in build_info.values():
            if value['status'] == 'Success':
                # build successfully
                exec_info = self._run_container()
                break
        
        return build_info, exec_info