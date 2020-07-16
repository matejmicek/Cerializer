from types import MappingProxyType

import fastavro
import yaml
import copy
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
		self.necessary_defs = []
		self.cycle_starting_nodes = {}
		self.init_cycles()


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


	def get_logical_type_constraint(self, schema, location):
		logical_type = schema['logicalType'].replace('-', '_')
		data_type = schema['type']
		if logical_type == 'decimal':
			params = {
				'scale': schema.get('scale', 0),
				'size': schema.get('size', 0)
			}
			return f'type(prepare.prepare_{data_type}_{logical_type}({location}, {params})) is {data_type}'
		return f'type(prepare.prepare_{logical_type}({location})) is {data_type}'


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


	def get_union_serialization(self, schema, location, is_from_array = False):
		'''
		Return union serialization string.
		'''
		if is_from_array:
			type_ = schema
			name = None
			new_location = location
		else:
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
					self.generate_serialization_code(possible_type, new_location)
				)
			)
		template = self.jinja_env.get_template('union.jinja2')
		if is_from_array:
			return template.render(types = possible_types_and_code, location = location, name = name, value = location)
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


	def init_cycles(self):
		cycle_starting_nodes = set()
		for schema_name, schema in self.schema_database.items():
			visited = set()
			cycle_detection(schema, visited, cycle_starting_nodes, self.schema_database)
			for starting_node in cycle_starting_nodes:
				self.cycle_starting_nodes[starting_node] = ''
				self.cycle_starting_nodes[starting_node] =self.render_code(
					self.schema_database[starting_node]
				)



	def render_code(self, schema):
		'''
		Renders code for a given schema into a .pyx file.
		'''
		# TODO path needs to be fixed - failing tests
		location = 'data'
		serialization_code = self.generate_serialization_code(
			schema = schema,
			location = location
		)
		cdefs = '\n'.join(self.cdefs)

		template = self.jinja_env.get_template('template.jinja2')
		rendered_template = template.render(
			location = location,
			cdefs = cdefs,
			buffer_name = self.buffer_name,
			serialization_code = serialization_code,
			necessary_defs = '\n'.join([i for i in self.necessary_defs if i != ''])
		)
		return rendered_template


	def generate_serialization_code(self, schema, location):
		'''
		Driver function to handle code generation for a schema.
		'''
		self.jinja_env.globals['correct_type'] = self.correct_type
		self.jinja_env.globals['correct_constraint'] = self.correct_constraint
		if type(schema) is str:
			if schema in self.cycle_starting_nodes:
				return self.handel_cycle(schema, location)
			return self.get_serialization_function(schema, location)
		if type(schema) is list:
			return self.get_union_serialization(schema, location, is_from_array = True)
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

		elif type(type_) is str and type_ in self.schema_database:
			if type_ in self.cycle_starting_nodes:
				return self.handel_cycle(type_, location)
			name = schema['name']
			new_location = f'{location}[\'{name}\']'
			return self.generate_serialization_code(self.schema_database[type_], new_location)
		elif type_ in constants.constants.BASIC_TYPES:
			name = schema.get('name')
			if name:
				location = f'{location}[\'{name}\']'
			return self.get_serialization_function(type_, location)


	def handel_cycle(self, schema, location):
		normalised_type = schema.replace(':', '_').replace('.', '_')
		serialization_function = f'serialize_{normalised_type}(data, output)'
		self.necessary_defs.append(
			self.cycle_starting_nodes[schema].replace(
				'cpdef serialize(data, output)',
				f'def {serialization_function}'
			).replace(
				'def serialize(data, output)',
				f'def {serialization_function}'
			)
		)
		serialization_function_call = serialization_function.replace('(data,', f'({location},')
		return f'output.write(buffer)\nbuffer = bytearray()\n{serialization_function_call}'

	def correct_type(self, type_):
		'''
		Corrects the nuances between Avro type definitions and actual python type names.
		'''
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
		if type(type_) is str and type_ in ('int', 'null', 'float', 'bytes'):
			return type_
		if type(type_) is dict and type_.get('type') == 'array':
			return 'list'



	def correct_constraint(self, type_, types, location, key, first: bool, value=None):
		# TODO consider removing types from arguments
		# value is filled when we are passing in a name of a local variable rather then a dict and a string
		if value:
			full_location = value
		else:
			full_location = f'{location}["{key}"]'
		correct_type_ = self.correct_type(type_)
		constraint = None

		if correct_type_:
			if correct_type_ == 'null':
				if value:
					constraint = f'{full_location} is None'
				else:
					constraint = f'"{key}" not in {location}'
			else:
				constraint = f'type({full_location}) is {self.correct_type(type_)}'

		elif type(type_) is dict and type_.get('type') == 'array':
			constraint = f'type({full_location}) is list'

		elif type(type_) is dict and type_.get('type') == 'map':
			constraint = f'type({full_location}) is dict'

		elif type(type_) is dict and type_.get('type') == 'enum':
			constraint = f'type({full_location}) is str and {full_location} in {type_["symbols"]}'

		elif type(type_) is dict and type_.get('logicalType') is not None:
			constraint = self.get_logical_type_constraint(type_, full_location)

		elif type(type_) is str and type_ in self.schema_database:
			return self.correct_constraint(self.schema_database[type_], types, location, key, first, value)

		elif type(type_) is dict and type_['type'] == 'record':
			# TODO adjust for different dict types
			constraint = f'type({full_location}) is dict'

		if constraint:
			return f'{"if" if first else "elif"} {constraint}:'
		raise RuntimeError(f'invalid constraint for type == {type_}')


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
	while True:
		try:
			parsed =  fastavro.parse_schema(json_object)
			return parsed
		except fastavro.schema.UnknownType as e:
			fastavro._schema_common.SCHEMA_DEFS[e.name] = {}



def get_cdef(type_: str, name: str):
	return f'cdef {type_} {name}'



def get_subschemata(schema_roots):
	schema_database = {}
	for schema_path, schema_identifier in cerializer.cerializer_handler.iterate_over_schema_roots(schema_roots):
		schema = parse_schema_from_file(schema_path)
		schema_database[schema_identifier] = schema
		scan_schema_for_subschemas(schema, schema_database)
	return schema_database



def scan_schema_for_subschemas(schema, schema_database):
	if type(schema) is dict:
		name = schema.get('name')
		if name:
			schema_database[name] = schema
		for _, subschema in schema.items():
			scan_schema_for_subschemas(subschema, schema_database)
	if type(schema) in (list, dict):
		for subschema in schema:
			scan_schema_for_subschemas(subschema, schema_database)



def cycle_detection(parsed_schema, visited, cycle_starting_nodes, schema_database):
	if type(parsed_schema) is str and parsed_schema in visited:
		cycle_starting_nodes.add(parsed_schema)
	elif type(parsed_schema) is dict:
		name = parsed_schema.get('name')
		type_ = parsed_schema.get('type')
		if type(type_) is str and type_ in visited:
			cycle_starting_nodes.add(type_)
		elif name:
			visited.add(name)
			new_visited = copy.deepcopy(visited)
			if 'fields' in parsed_schema:
				for field in parsed_schema['fields']:
					cycle_detection(field, new_visited, cycle_starting_nodes, schema_database)
			if type(type_) is dict:
				cycle_detection(type_, new_visited, cycle_starting_nodes, schema_database)
			if type(type_) is list:
				for element in type_:
					cycle_detection(element, new_visited, cycle_starting_nodes, schema_database)
			elif type(type_) is str and type_ in schema_database:
				cycle_detection(schema_database[type_], new_visited, cycle_starting_nodes, schema_database)