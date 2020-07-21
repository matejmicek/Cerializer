import os

import jinja2


import cerializer.compiler
import cerializer.schema_handler
import fastavro


'''
This module deals with schema handeling. 
The user should only iteract with the update_schemata method.
'''


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
            # TODO REMOVE - for testing purposes only
            fastavro._schema_common.SCHEMA_DEFS[schema_identifier] = schema
            self.code[schema_identifier] = self.get_compiled_code(schema)


    def get_compiled_code(self, schema):
        self.code_generator.cdefs = []
        self.code_generator.necessary_defs = []
        code = self.code_generator.render_code(schema)
        return cerializer.compiler.compile(code)



def iterate_over_schema_roots(schema_roots):
    for schema_root in schema_roots:
        schema_root = os.fsencode(schema_root)
        for namespace in [f for f in os.listdir(schema_root) if not f.startswith(b'.')]:
            for schema_name in [f for f in os.listdir(os.path.join(schema_root, namespace)) if not f.startswith(b'.')]:
                for version in [f for f in os.listdir(os.path.join(schema_root, namespace, schema_name)) if not f.startswith(b'.')]:
                    schema_path = os.path.join(schema_root, namespace, schema_name, version, b'schema.yaml')
                    schema_identifier = get_schema_identifier(namespace.decode(), schema_name.decode(), version.decode())
                    yield schema_path, schema_identifier



def get_schema_identifier(namespace, schema_name, schema_version):
    return f'{namespace}.{schema_name}:{schema_version}'