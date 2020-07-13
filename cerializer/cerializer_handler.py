import os

import jinja2

import cerializer.schema_handler
import constants.constants
import cerializer.compiler


'''
This module deals with schema handeling. 
The user should only iteract with the update_schemata method.
'''


def render_code(schema):
    '''
    Renders code for a given schema into a .pyx file.
    '''
    # TODO path needs to be fixed - failing tests
    env = jinja2.Environment(
        loader = jinja2.FileSystemLoader(searchpath = '../templates')
    )
    env.globals['env'] = env
    location = 'data'
    code_generator = cerializer.schema_handler.code_generator(buffer_name = 'buffer', read_var_name = 'res')
    serialization_code = code_generator.generate_serialization_code(
        schema = schema,
        location = location,
        jinja_env = env
    )
    cdefs = '\n'.join(code_generator.cdefs)

    template = env.get_template('template.jinja2')
    rendered_template = template.render(
        location = location,
        cdefs = cdefs,
        buffer_name = code_generator.buffer_name,
        serialization_code = serialization_code
    )
    return rendered_template



def get_compiled_code(schema):
    code = render_code(schema)
    return cerializer.compiler.compile(code)


class Cerializer:

    def __init__(self, schema_roots):
        self.schema_roots = schema_roots
        self.code = {}
        self.update_code()


    def update_code(self):
        '''
        Generates code for all schemata in all schema roots and then compiles it.
        '''
        for schema_root in self.schema_roots:
            schema_root = os.fsencode(schema_root)
            for namespace in os.listdir(schema_root):
                for schema_name in os.listdir(os.path.join(schema_root, namespace)):
                    for version in os.listdir(os.path.join(schema_root, namespace, schema_name)):
                        schema_path = os.path.join(schema_root, namespace, schema_name, version, b'schema.yaml')
                        schema = cerializer.schema_handler.parse_schema_from_file(schema_path.decode())
                        schema_identifier = f'{schema_name.decode()}_{version.decode()}'
                        self.code[schema_identifier] = get_compiled_code(schema = schema)
