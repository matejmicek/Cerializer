from constants import *
import avro.schema
import pprint
import jinja2


COMPLEX_TYPES = {'record', 'enum'}
NULL_TYPE = 'null'

def get_serialization_function(type_: str, location, buffer_name: str):
	return f'write.write_{type_}({buffer_name}, {location})'



def get_array_serialization(schema, location, buffer_name):
	item_deserialization_code = generate_serialization_code(schema['items'], 'item', buffer_name)
	env = jinja2.Environment(
		loader = jinja2.PackageLoader('serializer', 'templates')
	)
	template = env.get_template('array.jinja2')
	return template.render(
		location = location,
		buffer_name = buffer_name,
		item_deserialization_code = item_deserialization_code
	)




def generate_serialization_code(schema, location, buffer_name: str):
	print(schema)
	type_ = schema['type']
	print(type_)
	if type_ == RECORD:
		return '\n'.join((generate_serialization_code(field, location, buffer_name)) for field in schema['fields'])
	elif type_ == ARRAY:
		return get_array_serialization(schema, location, buffer_name)
	elif type(type_) is dict:
		name = schema['name']
		new_location = f'{location}[\'{name}\']'
		return generate_serialization_code(type_, new_location, buffer_name)
	elif type_ is list:
		pass
	elif type_ in BASIC_TYPES:
		name = schema['name']
		new_location = f'{location}[\'{name}\']'
		return get_serialization_function(type_, new_location, buffer_name)




# TODO REMOVE?????
'''
def parse_old(schema):
	if type(schema) is str:
		return schema

	if type(schema['type']) is str:
		type_ = schema['type']
		if type_ == 'record':
			result = {}
			for field in schema['fields']:
				result[field['name']] = parse(field)
			return result
		elif type_ == 'enum':
			pass
		elif type_ == 'array':
			return {
				'type': type_,
				'default': schema.get('default', None),
				'items': parse(schema['items'])
			}
		elif type_ == 'map':
			pass
		elif type_ == 'fixed':
			pass
		else:
			return {
				'type': type_,
				'default': schema.get('default', None)
			}

	elif type(schema['type']) is dict:
		return parse(schema['type'])


	elif type(schema['type']) is list:
		return schema['type']
'''


def parse_schema_from_file(filename: str):
	return avro.schema.parse(open(filename, 'rb').read()).to_json()
