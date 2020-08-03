import jinja2
import schema_parser
import pprint



RENDERING_FILENAME = 'serializer.pyx'



def get_deserialization_function(_type):
    return f'projekt.read_{_type}(read_buffer)'



def dict_serialization(dictionary, buffer_name, relative_directory):
    return '\n'.join(dict_serialization_generator(
        dictionary = dictionary,
        prefix = '',
        buffer_name = buffer_name,
        relative_directory = relative_directory)
    )



def dict_serialization_generator(dictionary, prefix, buffer_name, relative_directory):
    for key, value in dictionary.items():
        _type = type(value)
        if _type is dict:
            yield from dict_serialization_generator(value, f'{prefix}[\'{key}\']', buffer_name, relative_directory)
        else:
            yield f'{relative_directory}.write_{value}({buffer_name}, data{prefix}[\'{key}\'])'



def render_schema(rendered_filename, schema):
    env = jinja2.Environment(
        loader = jinja2.PackageLoader('serializer', 'templates')
    )
    env.globals['get_deserialization_function'] = get_deserialization_function
    env.globals['dict_serialization'] = dict_serialization

    template = env.get_template('template.jinja2')
    rendered_template = template.render(schema = schema)
    output = open(rendered_filename, 'w')
    output.write(rendered_template)
    output.close()


def update_schemata():
    schema = schema_parser.parse_schema_from_file('schemata/array_schema.avsc')
    result = schema_parser.generate_serialization_code(schema, 'DATAAAA', 'BUFFFEEEER')
    pprint.pprint(result)


update_schemata()