from constants.constants import *
import avro.schema
import jinja2
import os
import yaml
import json
import pprint
from types import MappingProxyType




COMPLEX_TYPES = {'record', 'enum'}
NULL_TYPE = 'null'

def get_serialization_function(type_: str, location, buffer_name: str):
	if type_ == 'null':
		return f'write.write_null({buffer_name})'
	return f'write.write_{type_}({buffer_name}, {location})'


def get_union_index_function(index: int, buffer_name: str):
	return f'write.write_long({buffer_name}, {index})'



def get_array_serialization(schema, location, buffer_name, env):
	item_deserialization_code = generate_serialization_code(schema['items'], 'item', buffer_name)
	template = env.get_template('array.jinja2')
	return template.render(
		location = location,
		buffer_name = buffer_name,
		item_deserialization_code = item_deserialization_code
	)


def get_enum_serialization(schema, location, buffer_name):
	symbols = schema['symbols']
	return f'write.write_int({buffer_name}, {symbols}.index({location}))'



def generate_serialization_code(schema, location, buffer_name: str):
	jinja_env = jinja2.Environment(
		loader = jinja2.PackageLoader('cerializer', 'templates')
	)
	if type(schema) is str:
		return get_serialization_function(schema, location, buffer_name)
	type_ = schema['type']
	if type_ == RECORD:
		return '\n'.join((generate_serialization_code(field, location, buffer_name)) for field in schema['fields'])
	elif type_ == ARRAY:
		return get_array_serialization(schema, location, buffer_name, jinja_env)
	elif type_ == ENUM:
		return get_enum_serialization(schema, location, buffer_name)
	elif type_ == MAP:
		pass
	elif type_ == FIXED:
		pass
	elif type(type_) is dict:
		name = schema['name']
		new_location = f'{location}[\'{name}\']'
		return generate_serialization_code(type_, new_location, buffer_name)
	elif type(type_) is list:
		name = schema['name']
		new_location = f'{location}[\'{name}\']'
		possible_types_and_code = []
		# we need to ensure that null is checked first
		if 'null' in type_:
			possible_types_and_code.append(
				(
					'null',
					get_union_index_function(type_.index('null'), buffer_name),
					get_serialization_function('null', new_location, buffer_name)
				)
			)
		for possible_type in type_:
			if possible_type == 'null': continue
			possible_types_and_code.append(
				(
					possible_type,
					get_union_index_function(type_.index(possible_type), buffer_name),
					get_serialization_function(possible_type, new_location, buffer_name)
				)
			)
		template = jinja_env.get_template('union.jinja2')
		return template.render(types = possible_types_and_code, location = location, name = name)

	# TODO needs to be fixed
	elif type(type_) is type(MappingProxyType({'a':'b'})):
		name = schema['name']
		new_location = f'{location}[\'{name}\']'
		return generate_serialization_code(dict(type_), new_location, buffer_name)

	elif type_ in BASIC_TYPES:
		name = schema['name']
		new_location = f'{location}[\'{name}\']'
		return get_serialization_function(type_, new_location, buffer_name)



def parse_schema_from_file(path):
	json_object = yaml.safe_load(open(path))
	return avro.schema.parse(json.dumps(json_object)).to_json()
