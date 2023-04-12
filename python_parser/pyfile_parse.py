import os
from tree_sitter import Language, Parser


class astVisiter(object):
    def __init__(self, builtin_funcs):
        self.builtin_funcs = builtin_funcs
        self.judge_prefix = set()

        self.imported_modules = set()       # modules
        self.imported_resources = set()     # from xxx import resources (full name)

        self.import_names = set()   
        self.alias_mappings = {}            # {used_name: full_name}

        self.called_attributes = []      # all attributes called in the code
        self.assign_mappings = {}           # {variable: name}
        self.imported_attrs = set()         # imported attributes called in the code
        self.builtin_attrs = set()          # built-in attributes called in the code

        self.has_error = False
        self.python_sytax = set()
    

    def clear(self):
        self.imported_modules = set()
        self.imported_resources = set()
        self.import_names = set()   
        self.alias_mappings = {}
        self.called_attributes = []
        self.assign_mappings = {}
        self.imported_attrs = set()
        self.builtin_attrs = set()
        self.has_error = False
        self.python_sytax = set()
    

    def print_all(self):
        if self.has_error:
            print('Syntax Error!')
            return

        print('{} imported modules: {}\n'.format(len(self.imported_modules), self.imported_modules))
        print('{} imported resources: {}\n'.format(len(self.imported_resources), self.imported_resources))
        print('{} imported names: {}\n'.format(len(self.import_names), self.import_names))
        print('{} alias mappings: {}\n'.format(len(self.alias_mappings), self.alias_mappings))
        print('{} called attributes: {}\n'.format(len(self.called_attributes), self.called_attributes))
        print('{} assign mappings: {}\n'.format(len(self.assign_mappings), self.assign_mappings))
        print('{} imported attrs: {}\n'.format(len(self.imported_attrs), self.imported_attrs))
        print('{} built-in attrs: {}\n'.format(len(self.builtin_attrs), self.builtin_attrs))
        if len(self.python_sytax) > 0:
            print('Syntax features: {}'.format(','.join(self.python_sytax)))
    

    def get_info(self):
        return {'imported_module': self.imported_modules, 'imported_resource': self.imported_resources,\
                'imported_attr': self.imported_attrs, 'builtin_attr': self.builtin_attrs,\
                'python_syntax': self.python_sytax}
    

    def handle_attr(self):
        # get the fully qualified name for called attributes
        sorted_keys = sorted(self.import_names, key=lambda item:len(item), reverse=True)
        for item in self.called_attributes:
            is_imported = False
            for key in sorted_keys:
                if item == key or item.startswith(key+'.'):
                    if key in self.alias_mappings:
                        item = '{}{}'.format(self.alias_mappings[key], item[len(key):])
                    self.imported_attrs.add(item)

                    is_imported = True
                    break
            
            if not is_imported:
                self.builtin_attrs.add(item)


    def visit_root(self, root_node):
        # for Python syntax
        self._walk_ast_for_syntax(root_node)

        # for global infomation
        self._get_global_imports(root_node)

        self.judge_prefix = self.builtin_funcs | self.import_names

        # for used attributes
        self.walk_ast(root_node)
        
    

    def _get_all_child_type(self, root):
        ret = set()

        if isinstance(root, list):
            st = root[:]
        else:
            st = [root, ]

        while st:
            node = st.pop()
            ret.add(node.type)
            st.extend(node.children)
        
        return ret


    def _walk_ast_for_syntax(self, node):
        node_type = node.type

        # if node_type != 'comment':
        #     print(node_type)

        if node_type == 'ERROR':
            self.has_error = True

        elif node_type == 'print':
            self.python_sytax.add('<3')
        
        elif node_type == 'exec':
            self.python_sytax.add('<3')
        
        elif node_type == 'async' or node_type == 'await':
            self.python_sytax.add('>=3.5')
        
        elif node_type == '<>':
            self.python_sytax.add('<3')
        
        elif node_type == 'nonlocal':
            self.python_sytax.add('>=3')

        elif node_type == 'except_clause':
            # except exc, var
            for child in node.children:
                if ',' == child.type:
                    self.python_sytax.add('<3')
                    break

        elif node_type == 'integer':
            # long number
            i = node.text.decode()
            if i.endswith('l') or i.endswith('L'):
                self.python_sytax.add('<3')
            
            if '_' in i:
                self.python_sytax.add('>=3.6')
            
            # octal number: starts with 0 and are numbers (not 0)
            if i.startswith('0'):
                i = '0{}'.format(i.lstrip('0'))
                if len(i) > 1 and i[1] > '1' and i[1] < '9':
                    self.python_sytax.add('<3')
        
        elif node_type == 'string':
            s = node.text.decode().lower()
            if s.startswith('`'):
                self.python_sytax.add('<3')

            elif s.startswith('u'):
                # s = u'unicode'
                self.python_sytax.add('!=3.0.*,!=3.1.*,!=3.2.*')

            elif s.startswith('f'):
                self.python_sytax.add('>=3.6')
            
            elif s.startswith('rb'):
                self.python_sytax.add('>=3.3')

        elif node_type == 'function_definition':
            ret_type = node.child_by_field_name('return_type')
            if ret_type:
                # def func() -> int:
                self.python_sytax.add('>=3')
            
            para = node.child_by_field_name('parameters')
            for child in para.children:
                child_type = child.type
                if child_type == 'typed_parameter' or child_type == 'typed_default_parameter' or child_type == 'keyword_separator':
                    # def func(parm1:int): / def func(parm1, *, parm2)
                    self.python_sytax.add('>=3')
                elif child_type == 'tuple_pattern':
                    # def func((parm1, parm2))
                    self.python_sytax.add('<3')
                elif child_type == 'positional_separator':
                    # def func(parm1, /, parm2)
                    self.python_sytax.add('>=3.8')
            
            if node.children[0].type == 'async':
                child_types = self._get_all_child_type(node)
                if 'yield' in child_types:
                    # 'yield' inside async function
                    self.python_sytax.add('>=3.6')

        elif node_type == 'class_definition':
            # class A(a=object, *b, **c):
            arg_list = node.child_by_field_name('superclasses')
            if arg_list:
                for child in arg_list.children:
                    child_type = child.type
                    if child_type == 'list_splat' or child_type == 'dictionary_splat' or child_type == 'keyword_argument':
                        self.python_sytax.add('>=3')
                        break
        
        elif node_type == 'assignment':
            # a, *b = 
            left_node = node.child_by_field_name('left')
            child_types = self._get_all_child_type(left_node)
            if 'list_splat_pattern' in child_types:
                self.python_sytax.add('>=3')
            
            # a: str
            type_node = node.child_by_field_name('type')
            if type_node:
                self.python_sytax.add('>=3.6')
        
        elif node_type == 'raise_statement':
            # raise E, V, T
            if len(node.children) > 1 and node.children[1].type == 'expression_list':
                self.python_sytax.add('<3')

            # raise EXCEPTION from CAUSE
            cause_node = node.child_by_field_name('cause')
            if cause_node:
                if cause_node.type == 'none':
                    # raise EXCEPTION from None
                    self.python_sytax.add('>=3.3')
                else:
                    self.python_sytax.add('>=3')
        
        elif node_type == 'yield':
            # yield from
            for child in node.children:
                if child.type == 'from':
                    self.python_sytax.add('>=3.3')
                    break
        
        elif node_type == 'binary_operator' or node_type == 'augmented_assignment':
            op = node.child_by_field_name('operator')
            if op.type == '@' or op.type == '@=':
                self.python_sytax.add('>=3.5')
        
        elif node_type == 'generator_expression':
            check_nodes = []
            is_first = True
            for child in node.children:
                if is_first and child.type == 'for_in_clause':
                    # aside from the iterable expression in the leftmost for clause
                    is_first = False
                    right_node = child.child_by_field_name('right')
                    p = child.children[0]
                    while p and p != right_node:
                        check_nodes.append(p)
                        p = p.next_sibling
                else:
                    check_nodes.append(child)
            
            check_types = self._get_all_child_type(check_nodes)
            if 'yield' in check_types:
                self.python_sytax.add('<3.8')

            for child in node.children:
                if child.type == 'if_clause':
                    # Unparenthesized lambda expressions can no longer be the expression part in an if clause in comprehensions and generator expressions.
                    for p in child.children:
                        if p.type == 'lambda':
                            self.python_sytax.add('<3.9')
                            break
        
        elif node_type in {'list', 'dictionary', 'set', 'tuple'}:
            for child in node.children:
                if child.type == 'list_splat' or child.type == 'dictionary_splat':
                    self.python_sytax.add('>=3.5')
                    break
        
        elif node_type in {'list_comprehension', 'dictionary_comprehension', 'set_comprehension'}:
            if node_type == 'list_comprehension':
                for child in node.children:
                    if child.type == 'for_in_clause':
                        p = child.child_by_field_name('right')
                        while p:
                            # [... for var in item1, item2, ...]
                            if p.type == ',':
                                self.python_sytax.add('<3')
                                break
                            p = p.next_sibling
            
            check_nodes = []
            is_first = True
            for child in node.children:
                if is_first and child.type == 'for_in_clause':
                    # aside from the iterable expression in the leftmost for clause
                    is_first = False
                    right_node = child.child_by_field_name('right')
                    p = child.children[0]
                    while p and p != right_node:
                        check_nodes.append(p)
                        p = p.next_sibling
                else:
                    check_nodes.append(child)
            
            check_types = self._get_all_child_type(check_nodes)
            if 'yield' in check_types:
                self.python_sytax.add('<3.8')

            child_types = self._get_all_child_type(node)
            # await expressions in all kinds of comprehensions
            if 'await' in child_types:
                self.python_sytax.add('>=3.6')

            for child in node.children:
                if child.type == 'if_clause':
                    for p in child.children:
                        if p.type == 'lambda':
                            self.python_sytax.add('<3.9')
                            break
        
        elif node_type == 'for_in_clause':
            if node.children[0].type == 'async':
                # async for in list, set, dict comprehensions and generator expressions
                self.python_sytax.add('>=3.6')
        
        elif node_type == 'named_expression':
            # :=
            self.python_sytax.add('>=3.8')
        
        elif node_type == 'finally_clause':
            block_node = node.children[-1]
            if block_node.type == 'block':
                for child in block_node.children:
                    # continue statement direct in the finally clause
                    if child.type == 'continue_statement':
                        self.python_sytax.add('>=3.8')
                        break
        
        elif node_type == 'with_item':
            val_node = node.child_by_field_name('value')
            if val_node.type == 'tuple':
                self.python_sytax.add('>=3.9')
        
        elif node_type == 'match_statement':
            self.python_sytax.add('>=3.10')

        for child in node.children:
            self._walk_ast_for_syntax(child)


    def _get_import_list(self, node, prefix_module=None):
        '''
        _import_list: $ => seq(
            commaSep1(field('name', choice(
                $.dotted_name,
                $.aliased_import
            ))),
            optional(',')
        )
        '''
        if node.type == 'dotted_name':
            name = node.text.decode()
            self.import_names.add(name)

            if prefix_module is not None:
                # from a import b
                full_name = '{}.{}'.format(prefix_module, name)
                self.alias_mappings[name] = full_name
                self.imported_resources.add(full_name)
            else:
                self.imported_modules.add(name)
        elif node.type == 'aliased_import':
            '''
            aliased_import: $ => seq(
                field('name', $.dotted_name),
                'as',
                field('alias', $.identifier)
            )
            '''
            name = node.child_by_field_name('name').text.decode()
            alias_name = node.child_by_field_name('alias').text.decode()

            self.import_names.add(alias_name)
            if prefix_module is not None:
                full_name = '{}.{}'.format(prefix_module, name)
                self.alias_mappings[alias_name] = full_name
                self.imported_resources.add(full_name)
            else:
                self.alias_mappings[alias_name] = name
                self.imported_modules.add(name)


    def _get_global_imports(self, root_node):
        for node in root_node.children:
            node_type = node.type
            if node_type == 'future_import_statement':
                '''
                future_import_statement: $ => seq(
                    'from',
                    '__future__',
                    'import',
                    choice(
                        $._import_list,
                        seq('(', $._import_list, ')'),
                    )
                )
                '''
                self.imported_modules.add('__future__')
                for child in node.children:
                    self._get_import_list(child, '__future__')

            elif node_type == 'import_statement':
                '''
                import_statement: $ => seq(
                    'import',
                    $._import_list
                )
                '''
                for child in node.children:
                    self._get_import_list(child)

            elif node_type == 'import_from_statement':
                '''
                import_from_statement: $ => seq(
                    'from',
                    field('module_name', choice(
                        $.relative_import,
                        $.dotted_name
                    )),
                    'import',
                    choice(
                        $.wildcard_import,
                        $._import_list,
                        seq('(', $._import_list, ')')
                    )
                )
                '''
                module = node.child_by_field_name('module_name').text.decode()
                self.imported_modules.add(module)

                children = node.children
                for i in range(3, len(children)):
                    child = children[i]
                    if child.type != 'wildcard_import':
                        self._get_import_list(child, module)
    

    def walk_ast(self, node):
        # for the called attributes: contain the assign mappings
        node_type = node.type    

        if node_type == 'assignment':
            '''
            assignment: $ => seq(
                field('left', $._left_hand_side),
                choice(
                    seq('=', field('right', $._right_hand_side)),
                    seq(':', field('type', $.type)),
                    seq(':', field('type', $.type), '=', field('right', $._right_hand_side))
                )
            )
            '''
            # get all left variables
            left_variables = set()
            p = node
            while p and p.type == 'assignment':
                left_node = p.child_by_field_name('left')
                if left_node.type == 'identifier':
                    left_name = left_node.text.decode()
                    if left_name not in self.import_names:
                        left_variables.add(left_name)

                elif left_node.type == 'pattern_list':
                    for item in left_node.children:
                        if item.type == 'identifier':
                            self.assign_mappings.pop(item.text.decode(), None)

                p = p.child_by_field_name('right')
            
            # record the mapping of variable
            if len(left_variables) > 0:
                right_attr = None
                if p:
                    right_attr = self._get_primary_expression(p)
                else:
                    type_name = node.child_by_field_name('type').text.decode()
                    if type_name in {'bool', 'dict', 'float', 'int', 'list', 'set', 'str', 'tuple'}:
                        right_attr = type_name
                
                if right_attr and self._save_attribute(right_attr):
                    for item in left_variables:
                        self.assign_mappings[item] = right_attr
                else:
                    # clear the mappings of all left variables
                    for item in left_variables:
                        self.assign_mappings.pop(item, None)

            for child in node.children:
                self.walk_ast(child)


        elif node_type == 'attribute' or node_type == 'call':
            attr = self._get_primary_expression(node)
            if attr:
                self._save_attribute(attr)

        for child in node.children:
                self.walk_ast(child)
    

    def _get_primary_expression(self, node):
        # identifier, attribute, call
        node_type = node.type
        if node_type == 'identifier':
            return node.text.decode()

        elif node_type == 'attribute':
            '''
            attribute: $ => prec(PREC.call, seq(
                field('object', $.primary_expression),
                '.',
                field('attribute', $.identifier)
            ))
            '''
            object_node = node.child_by_field_name('object')
            attr = node.child_by_field_name('attribute').text.decode()

            prefix_name = self._get_primary_expression(object_node)
            if prefix_name is not None:
                attribute = '{}.{}'.format(prefix_name, attr)
                return attribute

        elif node_type == 'call':
            '''
            call: $ => prec(PREC.call, seq(
                field('function', $.primary_expression),
                field('arguments', choice(
                    $.generator_expression,
                    $.argument_list
                ))
            ))
            '''
            function_node = node.child_by_field_name('function')
            function_name = self._get_primary_expression(function_node)
            if function_name is not None:
                return function_name
        
        elif node_type == 'string' or node_type == 'concatenated_string':
            return 'str'
        
        elif node_type == 'integer':
            return 'int'
        
        elif node_type == 'float':
            return 'float'
        
        elif node_type == 'true' or node_type == 'false':
            return 'bool'
        
        elif node_type == 'list' or node_type == 'list_comprehension':
            return 'list'
        
        elif node_type == 'dictionary' or node_type == 'dictionary_comprehension':
            return 'dict'
        
        elif node_type == 'set' or node_type == 'set_comprehension':
            return 'set'
        
        elif node_type == 'tuple':
            return 'tuple'

        return None
    

    def _save_attribute(self, attr):
        # replace attr by the assign mappings
        sorted_keys = sorted(self.assign_mappings, key=lambda item:len(item), reverse=True)

        item = attr
        while True:
            has_mapping = False
            for key in sorted_keys:
                if item == key or item.startswith(key+'.'):
                    # replace by the assign mappings
                    item = '{}{}'.format(self.assign_mappings[key], item[len(key):])
                    has_mapping = True

                    sorted_keys.remove(key)
                    break
            
            if not has_mapping:
                break
        
        # imported names or built-in functions
        need_save = False
        for key in self.judge_prefix:
            if item == key or item.startswith(key+'.'):
                need_save = True
                break
        
        if not need_save:
            return False
        
        for i in range(1, len(self.called_attributes)+1):
            if self.called_attributes[-i] == item or self.called_attributes[-i].startswith(item+'.'):
                return False
        
        self.called_attributes.append(item)



class PythonParser(object):
    def __init__(self, languages_dir, builtin_funcs):
        language_file = os.path.join(languages_dir, 'build/my-languages.so')
        Language.build_library(
            # Store the library in the `build` directory
            language_file,
    
            # Include one or more languages
            [
                os.path.join(languages_dir, 'vendor/tree-sitter-python'),
            ]
        )
        PY_LANGUAGE = Language(language_file, 'python')
        self.parser = Parser()
        self.parser.set_language(PY_LANGUAGE)

        self.visiter = astVisiter(builtin_funcs)
    

    def parse(self, source_code, not_file=False):
        self.visiter.clear()

        if not_file:
            # string to bytes
            tree = self.parser.parse(source_code.encode('utf-8'))
        else:
            with open(source_code, 'rb') as f:
                tree = self.parser.parse(f.read())
        
        try:
            self.visiter.visit_root(tree.root_node)
        except Exception:
            pass

        self.visiter.handle_attr()
        return self.visiter.get_info()