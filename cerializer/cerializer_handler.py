import os

import jinja2

import cerializer.schema_handler
import constants.constants
import cerializer.compiler


'''
This module deals with schema handeling. 
The user should only iteract with the update_schemata method.
'''


def render_code(schema, code_generator):
    '''
    Renders code for a given schema into a .pyx file.
    '''
    # TODO path needs to be fixed - failing tests
    location = 'data'
    serialization_code = code_generator.generate_serialization_code(
        schema = schema,
        location = location
    )
    cdefs = '\n'.join(code_generator.cdefs)

    template = code_generator.jinja_env.get_template('template.jinja2')
    rendered_template = template.render(
        location = location,
        cdefs = cdefs,
        buffer_name = code_generator.buffer_name,
        serialization_code = serialization_code
    )
    return rendered_template



def get_compiled_code(schema, code_generator):
    code_generator.cdefs = []
    code = render_code(schema, code_generator)
    return cerializer.compiler.compile(code)


class Cerializer:

    def __init__(self, schema_roots):
        self.schema_roots = schema_roots
        self.code = {}
        self.env = jinja2.Environment(
            loader = jinja2.FileSystemLoader(searchpath = '../templates')
        )
        self.env.globals['env'] = self.env
        self.code_generator = cerializer.schema_handler.CodeGenerator(self.env, self.schema_roots, 'buffer', 'res')
        self.update_code()


    def update_code(self):
        '''
        Generates code for all schemata in all schema roots and then compiles it.
        '''
        for schema_path, schema_identifier in iterate_over_schema_roots(self.schema_roots):
            schema = cerializer.schema_handler.parse_schema_from_file(schema_path.decode())
            self.code[schema_identifier] = get_compiled_code(schema, self.code_generator)



def iterate_over_schema_roots(schema_roots):
    for schema_root in schema_roots:
        schema_root = os.fsencode(schema_root)
        for namespace in os.listdir(schema_root):
            for schema_name in os.listdir(os.path.join(schema_root, namespace)):
                for version in os.listdir(os.path.join(schema_root, namespace, schema_name)):
                    schema_path = os.path.join(schema_root, namespace, schema_name, version, b'schema.yaml')
                    schema_identifier = get_schema_identifier(namespace.decode(), schema_name.decode(), version.decode())
                    yield (schema_path, schema_identifier)



def get_schema_identifier(namespace, schema_name, schema_version):
    return f'{namespace}.{schema_name}:{schema_version}'