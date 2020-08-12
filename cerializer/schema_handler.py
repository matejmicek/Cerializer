# pylint: disable=protected-access
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import jinja2

import cerializer.cerializer_handler
import cerializer.utils
import constants.constants



class CodeGenerator:
	def __init__(
		self,
		jinja_env: jinja2.environment.Environment,
		schemata: List[Tuple[str, Union[Dict[str, Any]]]],
		buffer_name: str,
	) -> None:
		self.buffer_name = buffer_name
		self.cdefs: List[str] = []
		self.dict_name_generator = cerializer.utils.name_generator('d_dict')
		self.val_name_generator = cerializer.utils.name_generator('val')
		self.key_name_generator = cerializer.utils.name_generator('key')
		self.type_name_generator = cerializer.utils.name_generator('type')
		self.int_name_generator = cerializer.utils.name_generator('i')
		self.schema_database = cerializer.utils.get_subschemata(schemata)
		self.jinja_env = jinja_env
		self.necessary_defs: Set[str] = set()
		self.cycle_starting_nodes: Dict[str, str] = {}
		self.init_cycles()

	def prepare(
		self,
		mode: constants.constants.SerializationMode,
		logical_type: str,
		data_type: str,
		location: str,
		schema: Dict[str, Any],
	) -> str:
		'''
		Returns a de/serialization function string for logical types that need to be prepared first.
		'''
		logical_type = logical_type.replace('-', '_')
		params = {
			'scale': schema.get('scale', 0),
			'size': schema.get('size', 0),
			'precision': schema.get('precision'),
		}

		read_params = f'fo, {params}' if data_type == 'fixed' else 'fo'
		if mode is constants.constants.SerializationMode.MODE_SERIALIZE:
			prepare_params = f'{location}, {params}' if logical_type == 'decimal' else location
			prepare_type = f'{data_type}_{logical_type}' if logical_type == 'decimal' else logical_type
			return f'write.write_{data_type}({self.buffer_name}, prepare.prepare_{prepare_type}({prepare_params}))'
		else:
			prepare_params = (
				f'read.read_{data_type}({read_params}), {params}'
				if logical_type == 'decimal'
				else f'read.read_{data_type}({read_params})'
			)
			return f'{location} = prepare.read_{logical_type}({prepare_params})'

	def get_serialization_function(self, type_: str, location: str) -> str:
		'''
		Returns the corresponding serialization function call string.
		'''
		if type_ == 'null':
			return f'write.write_null({self.buffer_name})'
		if type_ in self.schema_database and type_ not in constants.constants.BASIC_TYPES:
			return self.generate_serialization_code(self.schema_database[type_], location)
		return f'write.write_{type_}({self.buffer_name}, {location})'

	def get_deserialization_function(self, type_: str, location: str, schema: Optional[Dict[str, Any]] = None) -> str:
		'''
		Returns the corresponding deserialization function call string.
		'''
		if type_ == 'null':
			return f'{location} = None'
		if type_ in self.schema_database and type_ not in constants.constants.BASIC_TYPES:
			return self.generate_deserialization_code(self.schema_database[type_], location)
		read_params = f'fo, {schema}' if type_ == 'fixed' else 'fo'
		return f'{location} = read.read_{type_}({read_params})'

	def get_union_index_function(self, index: int) -> str:
		'''
		Returns a function call string for union index.
		'''
		return f'write.write_long({self.buffer_name}, {index})'

	def get_array_serialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Return array serialization string.
		'''
		item_name = next(self.val_name_generator)
		item_serialization_code = self.generate_serialization_code(schema['items'], item_name)
		template = self.jinja_env.get_template('array_serialization.jinja2')
		return template.render(
			location = location,
			buffer_name = self.buffer_name,
			item_serialization_code = item_serialization_code,
			item_name = item_name,
		)

	def get_array_deserialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Return array deserialization string.
		'''
		index_name = next(self.int_name_generator)
		block_count_name = next(self.int_name_generator)
		potential_item_name = next(self.val_name_generator)
		self.add_cdef('long long', index_name)
		self.add_cdef('long long', block_count_name)
		template = self.jinja_env.get_template('array_deserialization.jinja2')
		return template.render(
			location = location,
			buffer_name = self.buffer_name,
			items = schema['items'],
			index_name = index_name,
			block_count_name = block_count_name,
			potential_item_name = potential_item_name,
		)

	def get_enum_serialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Return enum serialization string.
		'''
		symbols = schema['symbols']
		return f'write.write_int({self.buffer_name}, {symbols}.index({location}))'

	def get_enum_deserialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Return enum deserialization string.
		'''
		symbols = schema['symbols']
		return f'{location} = {symbols}[read.read_int(fo)]'

	def get_union_serialization(self, schema: Union[List, Dict[str, Any]], location: str, is_from_array: bool = False) -> str:
		'''
		Return union serialization string.
		'''
		if is_from_array:
			# this is in case a union is not specified as a standalone type but is declared in array items
			type_ = list(schema) # since this union schema came from an array, it has to be in list form
			name = None
			new_location = location
		elif isinstance(schema, dict):
			name = schema['name']
			type_ = list(schema['type']) # schema['type'] has to be list since its a union schema
			new_location = f"{location}['{name}']"

		else:
			raise NotImplementedError(f'Cant handle schema = {schema}')

		if len([item for item in type_ if (isinstance(item, dict) and item.get('type') == 'array')]) > 1:
			# this is documented in task CL-132
			raise NotImplementedError(
				'One of your schemas contains a union of more than one array types. This is not yet implemented.'
			)

		possible_types_and_code = []
		# we need to ensure that null is checked first
		if 'null' in type_:
			possible_types_and_code.append(
				(
					'null',
					self.get_union_index_function(type_.index('null')),
					self.get_serialization_function('null', new_location),
				)
			)
		for possible_type in type_:
			if possible_type == 'null':
				continue
			possible_types_and_code.append(
				(
					possible_type,
					self.get_union_index_function(type_.index(possible_type)),
					self.generate_serialization_code(possible_type, new_location),
				)
			)
		type_name = next(self.type_name_generator)
		self.add_cdef('str', type_name)
		data_name = next(self.val_name_generator)
		template = self.jinja_env.get_template('union_serialization.jinja2')
		if is_from_array:
			return template.render(
				types = possible_types_and_code,
				location = location,
				name = name,
				type_name = type_name,
				data_name = data_name,
				value = location,
			)
		return template.render(
			types = possible_types_and_code,
			location = location,
			name = name,
			type_name = type_name,
			data_name = data_name,
		)

	def get_union_deserialization(
		self,
		schema: Union[List, Dict[str, Any]],
		location: str,
		is_from_array: bool = False,
	) -> str:
		'''
		Return union serialization string.
		'''
		index_name = next(self.int_name_generator)
		self.add_cdef('long', index_name)
		if is_from_array:
			types = schema
			new_location = location
		elif isinstance(schema, dict):
			name = schema['name']
			types = schema['type']
			new_location = f"{location}['{name}']"
		else:
			raise NotImplementedError(f'Cant handle schema = {schema}')
		template = self.jinja_env.get_template('union_deserialization.jinja2')
		return template.render(index_name = index_name, types = types, location = new_location)

	def get_map_serialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Return map serialization string.
		'''
		dict_name = next(self.dict_name_generator)
		self.add_cdef('dict', dict_name)
		template = self.jinja_env.get_template('map_serialization.jinja2')
		values = schema['values']
		key_name = next(self.key_name_generator)
		val_name = next(self.val_name_generator)
		self.add_cdef('str', key_name)
		return template.render(
			location = location,
			buffer_name = self.buffer_name,
			values = values,
			key_name = key_name,
			val_name = val_name,
		)

	def get_map_deserialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Return map deserialization string.
		'''
		key_name = next(self.key_name_generator)
		self.add_cdef('unicode', key_name)
		block_count_name = next(self.int_name_generator)
		self.add_cdef('long', block_count_name)
		index_name = next(self.int_name_generator)
		self.add_cdef('long', index_name)

		template = self.jinja_env.get_template('map_deserialization.jinja2')
		values = schema['values']
		return template.render(
			location = location,
			values = values,
			key_name = key_name,
			block_count_name = block_count_name,
			index_name = index_name,
		)

	def init_cycles(self) -> None:
		cycle_starting_nodes: Set[str] = set()
		for _, schema in self.schema_database.items():
			visited: Set[str] = set()
			cerializer.utils.cycle_detection(schema, visited, cycle_starting_nodes, self.schema_database)
			for starting_node in cycle_starting_nodes:
				# we need to first put in something for each cycle starting node so that
				# the 'render code' function does not end up in an infinite cycle
				self.cycle_starting_nodes[starting_node] = ''
			for starting_node in cycle_starting_nodes:
				self.cycle_starting_nodes[starting_node] = self.render_code(self.schema_database[starting_node])

	def render_code(self, schema: Dict[str, Any]) -> str:
		'''
		Renders code for a given schema into a .pyx file.
		'''
		self.jinja_env.globals['correct_type'] = cerializer.utils.correct_type
		self.jinja_env.globals['correct_constraint'] = self.correct_constraint
		self.jinja_env.globals['generate_serialization_code'] = self.generate_serialization_code
		self.jinja_env.globals['generate_deserialization_code'] = self.generate_deserialization_code
		self.jinja_env.globals['get_type_name'] = cerializer.utils.get_type_name
		schema = cerializer.utils.parse_schema(schema)
		location = 'data'
		# This is here because if schema name XYZ is defined in this file and also
		# somewhere else in the schema repo, the definition from this file has to be considered first
		cerializer.utils.scan_schema_for_subschemata(schema, self.schema_database)
		serialization_code = self.generate_serialization_code(schema = schema, location = location)
		serialization_code = '\n'.join(self.cdefs) + '\n' + serialization_code
		self.cdefs = []
		deserialization_code = self.generate_deserialization_code(schema = schema, location = location)
		deserialization_code = '\n'.join(self.cdefs) + '\n' + deserialization_code

		template = self.jinja_env.get_template('template.jinja2')
		rendered_template = template.render(
			location = location,
			buffer_name = self.buffer_name,
			serialization_code = serialization_code,
			deserialization_code = deserialization_code,
			necessary_defs = '\n\n\n\n'.join([i for i in self.necessary_defs if i != '']),
		)
		self.cdefs = []
		self.necessary_defs = set()
		return rendered_template

	def generate_serialization_code(self, schema: Union[str, List, Dict[str, Any]], location: str) -> str:
		'''
		Driver function to handle code generation for a schema.
		'''
		if isinstance(schema, str):
			if schema in self.cycle_starting_nodes and schema not in constants.constants.BASIC_TYPES:
				return self.handle_cycle(constants.constants.SerializationMode.MODE_SERIALIZE, schema, location)
			return self.get_serialization_function(schema, location)
		if isinstance(schema, list):
			return self.get_union_serialization(schema, location, is_from_array = True)
		type_ = schema['type']
		if 'logicalType' in schema:
			prepared = self.prepare(
				constants.constants.SerializationMode.MODE_SERIALIZE,
				schema['logicalType'],
				type_,
				location,
				schema,
			)
			return prepared
		elif type_ == constants.constants.RECORD:
			return '\n'.join((self.generate_serialization_code(field, location)) for field in schema['fields'])
		elif type_ == constants.constants.ARRAY:
			return self.get_array_serialization(schema, location)
		elif type_ == constants.constants.ENUM:
			return self.get_enum_serialization(schema, location)
		elif type_ == constants.constants.MAP:
			return self.get_map_serialization(schema, location)
		elif type_ == constants.constants.FIXED:
			return self.get_serialization_function(type_, location)
		elif type(type_) is dict:
			name = schema['name']
			new_location = f"{location}['{name}']"
			default_if_necessary = cerializer.utils.default_if_necessary(new_location, schema.get('default'))
			return str(default_if_necessary + '\n' + self.generate_serialization_code(type_, new_location))
		elif type(type_) is list:
			return self.get_union_serialization(schema, location)
		elif type(type_) is str and type_ in constants.constants.BASIC_TYPES:
			name = schema.get('name')
			if name:
				location = f"{location}['{name}']"
			default_if_necessary = cerializer.utils.default_if_necessary(location, schema.get('default'))
			return str(default_if_necessary + '\n' + self.get_serialization_function(type_, location))
		elif type(type_) is str and type_ in self.schema_database:
			if type_ in self.cycle_starting_nodes:
				return self.handle_cycle(constants.constants.SerializationMode.MODE_SERIALIZE, type_, location)
			name = schema['name']
			new_location = f"{location}['{name}']"
			return self.generate_serialization_code(self.schema_database[type_], new_location)
		raise NotImplementedError(f'Cant handle schema = {schema}')


	def generate_deserialization_code(self, schema: Union[Dict[str, Any], list, str], location: str) -> str:
		'''
		Driver function to handle code generation for a schema.
		'''
		if isinstance(schema, str):
			if schema in self.cycle_starting_nodes and schema not in constants.constants.BASIC_TYPES:
				return self.handle_cycle(constants.constants.SerializationMode.MODE_DESERIALIZE, schema, location)
			return self.get_deserialization_function(schema, location)
		if isinstance(schema, list):
			return self.get_union_deserialization(schema, location, is_from_array = True)
		if isinstance(schema, dict):
			type_ = schema['type']
			if 'logicalType' in schema:
				prepared = self.prepare(
					constants.constants.SerializationMode.MODE_DESERIALIZE,
					schema['logicalType'],
					type_,
					location,
					schema,
				)
				return prepared
			elif type_ == constants.constants.RECORD:
				field_deserialization = '\n'.join(
					(self.generate_deserialization_code(field, location))
					for field in schema['fields']
				)
				return location + ' = {}\n' + field_deserialization
			elif type_ == constants.constants.ARRAY:
				return self.get_array_deserialization(schema, location)
			elif type_ == constants.constants.ENUM:
				return self.get_enum_deserialization(schema, location)
			elif type_ == constants.constants.MAP:
				return self.get_map_deserialization(schema, location)
			elif type_ == constants.constants.FIXED:
				return self.get_deserialization_function(type_, location, schema = schema)
			elif type(type_) is dict:
				name = schema['name']
				new_location = f"{location}['{name}']"
				return self.generate_deserialization_code(type_, new_location)
			elif type(type_) is list:
				return self.get_union_deserialization(schema, location)
			elif type(type_) is str and type_ in constants.constants.BASIC_TYPES:
				name = schema.get('name')
				if name:
					location = f"{location}['{name}']"
				return self.get_deserialization_function(type_, location, schema = schema)
			elif type(type_) is str and type_ in self.schema_database:
				if type_ in self.cycle_starting_nodes:
					return self.handle_cycle(constants.constants.SerializationMode.MODE_DESERIALIZE, type_, location)
				name = schema['name']
				new_location = f"{location}['{name}']"
				return self.generate_deserialization_code(self.schema_database[type_], new_location)
		raise NotImplementedError(f'Cant handle schema = {schema}')


	def handle_cycle(self, mode: constants.constants.SerializationMode, schema: str, location: str) -> str:
		normalised_type = schema.replace(':', '_').replace('.', '_')
		serialization_function = (
			f'{constants.constants.SerializationMode.MODE_SERIALIZE}_{normalised_type}(data, output)'
		)
		deserialization_function = f'{constants.constants.SerializationMode.MODE_DESERIALIZE}_{normalised_type}(fo)'
		self.necessary_defs.add(
			self.cycle_starting_nodes[schema]
			.replace(
				f'cpdef {constants.constants.SerializationMode.MODE_SERIALIZE}(data, output)',
				f'def {serialization_function}',
			)
			.replace(
				f'def {constants.constants.SerializationMode.MODE_SERIALIZE}(data, output)',
				f'def {serialization_function}',
			)
			.replace(
				f'cpdef {constants.constants.SerializationMode.MODE_DESERIALIZE}(fo)',
				f'def {deserialization_function}',
			)
			.replace(
				f'def {constants.constants.SerializationMode.MODE_DESERIALIZE}(fo)',
				f'def {deserialization_function}',
			)
		)
		serialization_function_call = serialization_function.replace('(data,', f'({location},')
		if mode is constants.constants.SerializationMode.MODE_SERIALIZE:
			return f'output.write(buffer)\nbuffer = bytearray()\n{serialization_function_call}'
		else:
			return f'{location} = {deserialization_function}'

	def add_cdef(self, type_: str, name: str) -> None:
		cdef = f'cdef {type_} {name}'
		self.cdefs.append(cdef)

	def correct_constraint(
		self,
		type_: Union[Dict, str],
		location: str,
		key: str,
		first: bool,
		value: Optional[str] = None,
	) -> str:
		# value is filled when we are passing in a name of a local variable rather then a dict and a string
		if value:
			full_location = value
		else:
			full_location = f'{location}["{key}"]'
		correct_type_ = cerializer.utils.correct_type(type_)
		constraint = None

		if correct_type_:
			if correct_type_ == 'null':
				if value:
					constraint = f'{full_location} is None'
				else:
					constraint = f'"{key}" not in {location} or {location}["{key}"] is None'
			else:
				constraint = f'type({full_location}) is {cerializer.utils.correct_type(type_)}'

		elif isinstance(type_, dict) and type_.get('type') == 'fixed':
			constraint = f'type({full_location}) is bytes'

		elif isinstance(type_, dict) and type_.get('type') == 'array':
			constraint = f'type({full_location}) is list'

		elif isinstance(type_, dict) and type_.get('type') == 'map':
			constraint = f'type({full_location}) is dict'

		elif isinstance(type_, dict) and type_.get('type') == 'enum':
			constraint = f'type({full_location}) is str and {full_location} in {type_["symbols"]}'

		elif isinstance(type_, dict) and type_.get('logicalType') is not None:
			constraint = cerializer.utils.get_logical_type_constraint(type_, full_location)

		elif isinstance(type_, str) and type_ in self.schema_database:
			return self.correct_constraint(self.schema_database[type_], location, key, first, value)

		elif isinstance(type_, dict) and type_['type'] == 'record':
			# TODO adjust for different dict types
			constraint = f'type({full_location}) is dict'

		if constraint:
			return f'{"if" if first else "elif"} {constraint}:'
		raise RuntimeError(f'invalid constraint for type == {type_}')


	def acknowledge_new_schemata(self, schemata: List[Tuple[str, Dict[str, Any]]]) -> None:
		new_subschemata = cerializer.utils.get_subschemata(schemata)
		self.schema_database = {**self.schema_database, **new_subschemata}
