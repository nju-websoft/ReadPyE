import re

PATTERN_NAME = '[\'\"]?[^\s\'\"]+[\'\"]?'
PATTERN_SPACE = '[^\S\r\n]+'


re_moduleNotFoundError = re.compile(r'ModuleNotFoundError: No module named (?P<module>{})'.format(PATTERN_NAME))
re_importError = re.compile(r'ImportError: No module named (?P<module>{})'.format(PATTERN_NAME))
re_importNameError = re.compile(r'ImportError: cannot import name (?P<attr>{})(?: from (?P<module>{}))?'.format(PATTERN_NAME, PATTERN_NAME))
re_moduleAttributeError = re.compile(r'AttributeError: module (?P<module>{}) has no attribute (?P<attr>{})'.format(PATTERN_NAME, PATTERN_NAME))
re_objectAttributeError = re.compile(r'AttributeError: (?P<object>{}) object has no attribute (?P<attr>{})'.format(PATTERN_NAME, PATTERN_NAME))
re_syntaxError = re.compile(r'SyntaxError: ')

re_importStat = re.compile(r'^(?:from .+ )?import ')

import_patterns = [re_moduleNotFoundError, re_importError, re_importNameError]
re_normalImportError = re.compile(r'(?:(?:ImportError)|(?:ModuleNotFoundError)): ')

MAX_STACK_LENGTH = 100
MAX_SYNTAX_LOG_LENGTH = 5


def _get_clean_name(name):
    if name is None:
        return None

    return name.strip('\'\"')


def find_syntax_error(logs):
    match_info = re_syntaxError.search(logs)
    if match_info is not None:
        # the match position
        pos = match_info.start()

        preceding_content = logs[:pos]
        lines = preceding_content.split('\n')

        length = min(len(lines), MAX_SYNTAX_LOG_LENGTH)
        error_code = None
        for i in range(4, length+1):
            line = lines[-i].strip()
            if line.startswith('File '):
                error_code = '\n'.join([line.strip() for line in lines[-i+1: -2]])
                return {'exception': 'SyntaxError', 'pos': pos, 'code_list': [error_code, ]}
    
    return None


def find_module_attribute_error(logs):
    # AttributeError by module
    match_info = re_moduleAttributeError.search(logs)
    if match_info is not None:
        module = _get_clean_name(match_info.group('module'))
        attr = _get_clean_name(match_info.group('attr'))

        return {'exception': 'AttributeError', 'pos': match_info.start(), 'code_list': [f'from {module} import {attr}', ]}
    
    return None


def find_object_attribute_error(logs):
    # AttributeError by object: maybe built-in functions
    match_info = re_objectAttributeError.search(logs)
    if match_info is not None:
        obj = _get_clean_name(match_info.group('object'))

        if obj != 'NoneType':
            attr = _get_clean_name(match_info.group('attr'))
            return {'exception': 'AttributeError', 'pos': match_info.start(), 'code_list': [f'{obj}.{attr}', ]}
    
    return None


def _get_exception_stack(content):
    # Get the last exception stack from the proceding content
    lines = content.split('\n')

    length = min(len(lines), MAX_STACK_LENGTH)
    stack_lines = []
    for i in range(2, length+1):
        # reverse
        line = lines[-i].strip()
        if line == 'Traceback (most recent call last):':
            return stack_lines
        
        stack_lines.append(line)
    
    return []


def _get_import_statements(logs):
    import_stats = [line for line in logs if re_importStat.search(line) is not None]
    # remove parentheses: avoid incomplete import statement
    return [re.sub(r'[\(\)]', '', x).strip(' ,') for x in import_stats]


def find_import_error(logs):
    match_results = []
    for pattern in import_patterns:
        match_info = pattern.search(logs)
        if match_info is not None:
            # the match position
            pos = match_info.start()

            match_dict = match_info.groupdict()
            module = _get_clean_name(match_dict.get('module', None))
            attr = _get_clean_name(match_dict.get('attr', None))

            ret = {'exception': 'ImportError', 'pos': pos}
            if module is not None and attr is not None:
                # exact semantic
                ret['code_list'] = [f'from {module} import {attr}', ]
            else:

                exception_stack = _get_exception_stack(logs[:pos])
                import_stats = _get_import_statements(exception_stack)
                if len(import_stats) == 0 and module is None:
                    # no specified module and no import statement
                    ret = None
                else:
                    # maybe need to check the module
                    if module is not None:
                        import_stats.append(f'import {module}')

                    ret['check'] = (module, attr)
                    ret['code_list'] = import_stats


            if ret is not None:
                match_results.append(ret)
    
    ret = None
    if len(match_results) > 0:
        # the first one
        ret = min(match_results, key=lambda x:x['pos'])
    
    # check for normal ImportError
    match_info = re_normalImportError.search(logs)
    if match_info is not None:
        # the match position
        pos = match_info.start()
        if ret is None or pos < ret['pos']:
            exception_stack = _get_exception_stack(logs[:pos])
            import_stats = _get_import_statements(exception_stack)
            if len(import_stats) > 0:
                # must have import stats
                ret = {'exception': 'ImportError', 'pos': pos, 'code_list': import_stats}
        
    return ret


match_functions = [find_syntax_error, find_module_attribute_error, find_object_attribute_error, find_import_error]
def match_templates(logs):
    match_results = []
    for func in match_functions:
        match_info = func(logs)
        if match_info is not None:
            match_results.append(match_info)
    
    if len(match_results) > 0:
        first_match = min(match_results, key=lambda x:x['pos'])
        first_match.pop('pos')
        return first_match
    
    return None