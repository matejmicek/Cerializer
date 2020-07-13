import json
from types import MappingProxyType

import avro.schema
import fastavro
import yaml
import pprint
import cerializer.cerializer_handler

import constants.constants



class CodeGenerator:
	def __init__(self, jinja_env, schema_roots, buffer_name: str, read_var_name):
		self.buffer_name = buffer_name
		self.read_var_name = read_var_name
		self.cdefs = []
		#TODO will be changed to only one generator, but for now this is more readable
		self.dict_name_generator = name_generator('d_dict')
		self.val_name_generator = name_generator('val')
		self.key_name_generator = name_generator('key')
		self.schema_database = cerializer.schema_handler.get_subschemata(schema_roots)
		self.jinja_env = jinja_env

	def prepare(self, logical_type, data_type, location, schema):
		'''
		Returns a serialization function string for logical types that need to be prepared first.
		'''
		if logical_type == 'decimal':
			params = {
				'scale': schema.get('scale', 0),
				'size': schema.get('size', 0)
			}
			return f'write.write_{data_type}({self.buffer_name}, prepare.prepare_{data_type}_{logical_type}({location}, {params}))'
		return f'write.write_{data_type}({self.buffer_name}, prepare.prepare_{logical_type}({location}))'



	def get_serialization_function(self, type_: str, location: str):
		'''
		Returns the corresponding serialization function call string.
		'''
		if type_ == 'null':
			return f'write.write_null({self.buffer_name})'
		if type_ in self.schema_database:
			return self.generate_serialization_code(self.schema_database[type_], location)
		return f'write.write_{type_}({self.buffer_name}, {location})'


	def get_deserialization_function(self, type_: str, location: str):
		'''
		Returns the corresponding serialization function call string.
		'''
		if type_ == 'null':
			return f'{location} = None'
		return f'{location} = read.read_{type_}({self.buffer_name})'




	def get_union_index_function(self, index: int):
		'''
		Returns a function call string for union index.
		'''
		return f'write.write_long({self.buffer_name}, {index})'



	def get_array_serialization(self, schema, location):
		'''
		Return array serialization string.
		'''
		print()
		print()
		print('printing from array ')
		print(schema)
		item_serialization_code = self.generate_serialization_code(
			schema['items'],
			'item'
		)
		template = self.jinja_env.get_template('array.jinja2')
		return template.render(
			location = location,
			buffer_name = self.buffer_name,
			item_serialization_code = item_serialization_code
		)


	def get_enum_serialization(self, schema, location):
		'''
		Return enum serialization string.
		'''
		symbols = schema['symbols']
		return f'write.write_int({self.buffer_name}, {symbols}.index({location}))'


	def get_union_serialization(self, schema, location):
		'''
		Return union serialization string.
		'''
		name = schema['name']
		type_ = schema['type']
		new_location = f'{location}[\'{name}\']'
		possible_types_and_code = []
		# we need to ensure that null is checked first
		if 'null' in type_:
			possible_types_and_code.append(
				(
					'null',
					self.get_union_index_function(type_.index('null')),
					self.get_serialization_function('null', new_location)
				)
			)
		for possible_type in type_:
			if possible_type == 'null':
				continue
			possible_types_and_code.append(
				(
					possible_type,
					self.get_union_index_function(type_.index(possible_type)),
					self.get_serialization_function(possible_type, new_location)
				)
			)
		template = self.jinja_env.get_template('union.jinja2')
		return template.render(types = possible_types_and_code, location = location, name = name)



	def get_map_serialization(self, schema, location):
		'''
		Return map serialization string.
		'''
		dict_name = next(self.dict_name_generator)
		self.cdefs.append(get_cdef('dict', dict_name))
		template = self.jinja_env.get_template('map.jinja2')
		return template.render(
			dict_name = dict_name,
			location = location,
			buffer_name = self.buffer_name,
			schema = schema,
			generate_serialization_code = self.generate_serialization_code,
			key_name = next(self.key_name_generator),
			val_name = next(self.val_name_generator)
		)



	def generate_serialization_code(self, schema, location):
		'''
		Driver function to handle code generation for a schema.
		'''
		self.jinja_env.globals['correct_type'] = correct_type
		if type(schema) is str:
			return self.get_serialization_function(schema, location)

		type_ = schema['type']

		if f'logicalType' in schema:
			prepared = self.prepare(schema['logicalType'].replace('-', '_'), type_, location, schema)
			return prepared
		elif type_ == constants.constants.RECORD:
			return '\n'.join(
				(
					self.generate_serialization_code(
						field,
						location
					)
				) for field in schema['fields'])
		elif type_ == constants.constants.ARRAY:
			return self.get_array_serialization(schema, location)
		elif type_ == constants.constants.ENUM:
			return self.get_enum_serialization(schema, location)
		elif type_ == constants.constants.MAP:
			return self.get_map_serialization(
				schema,
				location
			)
		elif type_ == constants.constants.FIXED:
			return self.get_serialization_function(type_, location)
		elif type(type_) is dict:
			name = schema['name']
			new_location = f'{location}[\'{name}\']'
			return self.generate_serialization_code(type_, new_location)
		elif type(type_) is list:
			return self.get_union_serialization(schema, location)

		# TODO needs to be fixed
		elif type(type_) is type(MappingProxyType({'a':'b'})):
			name = schema['name']
			new_location = f'{location}[\'{name}\']'
			return self.generate_serialization_code(dict(type_), new_location)

		elif type_ in constants.constants.BASIC_TYPES:
			name = schema.get('name')
			if name:
				location = f'{location}[\'{name}\']'
			return self.get_serialization_function(type_, location)



def name_generator(prefix: str):
	i = 0
	while True:
		yield f'{prefix}_{i}'
		i += 1



def parse_schema_from_file(path):
	'''
	Wrapper for loading schemata from yaml
	'''
	json_object = yaml.safe_load(open(path))
	return fastavro.parse_schema(json_object, expand = True)


def correct_type(type_):
	'''
	Corrects the nuances between Avro type definitions and actual python type names.
	'''
	if type(type_) is dict:
		return 'dict'
	if type_ == 'string':
		return 'str'
	if type_ == 'boolean':
		return 'bool'
	if type_ == 'long':
		# TODO since python 3.4 there is only int, but do we distinguish????
		return 'int'
	if type_ == 'double':
		# TODO float is already double precision - fix for a compile error
		return 'float'
	return type_


def get_cdef(type_: str, name: str):
	return f'cdef {type_} {name}'



def get_subschemata(schema_roots):
	schema_database = {}
	for schema_path, schema_identifier in cerializer.cerializer_handler.iterate_over_schema_roots(schema_roots):
		namespace = schema_identifier.split('.')[0] # TODO change
		schema = parse_schema_from_file(schema_path)
		scan_schema_for_subschemas(schema, namespace, schema_database)
	return schema_database



def scan_schema_for_subschemas(schema, namespace, schema_database):
	if type(schema) is dict:
		name = schema.get('name')
		if name:
			schema_database[name] = schema
		for _, subschema in schema.items():
			scan_schema_for_subschemas(subschema, namespace, schema_database)
	if type(schema) in (list, dict):
		for subschema in schema:
			scan_schema_for_subschemas(subschema, namespace, schema_database)