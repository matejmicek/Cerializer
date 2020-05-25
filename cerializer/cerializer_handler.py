import os

import jinja2

import cerializer.schema_parser
import constants.constants



'''
This module deals with schema handeling. 
The user should only iteract with the update_schemata method.
'''


def render_code_for_schema(rendered_filename, schema):
    '''
    Renders code for a given schema into a .pyx file.
    '''
    env = jinja2.Environment(
        loader = jinja2.FileSystemLoader(searchpath = 'cerializer/templates')
    )
    env.globals['serialization_code'] = cerializer.schema_parser.generate_serialization_code
    env.globals['write_prefix'] = constants.constants.WRITE_PREFIX
    env.globals['prepare_prefix'] = constants.constants.PREPARE_PREFIX
    env.globals['write_location'] = constants.constants.WRITE_LOCATION
    env.globals['env'] = env

    template = env.get_template('template.jinja2')
    rendered_template = template.render(schema = schema)
    output = open(os.path.join('cerializer', rendered_filename), 'w')
    output.write(rendered_template)
    output.close()


def update_cerializer(schema_roots):
    '''
    Generates code for all schemata in all schema roots and then compiles it.
    '''
    code_base_path = 'cerializer_base'
    try:
        os.mkdir(os.path.join('cerializer', code_base_path))
    except OSError:
        pass
    for schema_root in schema_roots:
        schema_root = os.fsencode(schema_root)
        for namespace in os.listdir(schema_root):
            for schema_name in os.listdir(os.path.join(schema_root, namespace)):
                for version in os.listdir(os.path.join(schema_root, namespace, schema_name)):
                    schema_path = os.path.join(schema_root, namespace, schema_name, version, b'schema.yaml')
                    filename = f'{schema_name.decode()}_{version.decode()}.pyx'
                    code_path = os.path.join(code_base_path, filename)
                    schema = cerializer.schema_parser.parse_schema_from_file(schema_path.decode())
                    render_code_for_schema(code_path, schema = schema)
    os.system('python setup.py build_ext --inplace')
