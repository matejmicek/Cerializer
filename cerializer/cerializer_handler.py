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
    env.globals['env'] = env
    location = 'data'
    code_generator = cerializer.schema_parser.code_generator(buffer_name = 'buffer')
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
    output = open(rendered_filename, 'w')
    output.write(rendered_template)
    output.close()


def update_cerializer(schema_roots):
    '''
    Generates code for all schemata in all schema roots and then compiles it.
    '''
    code_base_path = 'cerializer/cerializer_base'
    try:
        os.mkdir(os.path.join(code_base_path))
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
    os.system(f'python setup.py build_ext --inplace')


update_cerializer(['cerializer/tests/schemata'])
