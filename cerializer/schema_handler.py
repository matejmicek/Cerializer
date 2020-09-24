# pylint: disable=protected-access
import copy
import os
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import jinja2

import cerializer.utils
from cerializer import constants


class CerializerSchemata:
	'''
	Storage class for schemata.
	'''

	def __init__(self, schemata: List[Tuple[str, Any]]):
		self._schema_database: Dict[str, Union[str, List[Any], Dict[str, Any]]] = cerializer.utils.get_subschemata(
			schemata
		)
		self._cycle_starting_nodes: Set[str] = set()
		self._init_cycles()

	def __contains__(self, item: str) -> bool:
		return item in self._schema_database

	def load_schema(
		self,
		schema_identifier: str,
		context_schema_identifier: Optional[str] = None,
	) -> Union[str, List, Dict[str, Any]]:
		# we first check whether the schema we are looking for is not defined in the same big schema
		# this would mean the schema is redefined and that the local version has to be used
		if context_schema_identifier:
			context_schema = self._schema_database[context_schema_identifier]
			# mypy needs this type annotation
			local_schema_database: Dict[str, Union[str, List[Any], Dict[str, Any]]] = cerializer.utils.get_subschemata(
				[(context_schema_identifier, context_schema)]
			)
			if schema_identifier in local_schema_database:
				return local_schema_database[schema_identifier]
		if schema_identifier in self._schema_database:
			return self._schema_database[schema_identifier]
		else:
			raise RuntimeError(f'Schema with identifier = {schema_identifier} not found in schema database.')

	def is_cycle_starting(self, schema_identifier: str) -> bool:
		return schema_identifier in self._cycle_starting_nodes

	def _init_cycles(self) -> None:
		for _, schema in self._schema_database.items():
			visited: Set[str] = set()
			self._cycle_detection(schema, visited, self._cycle_starting_nodes)

	def _cycle_detection(
		self,
		parsed_schema: Union[str, List, Dict[str, Any]],
		visited: Set[str],
		cycle_starting_nodes: Set[str],
	) -> None:
		'''
		Detects cycles in schemata.
		This can happen when for example a schema is defined using itself eg. a tree schema.
		This method add all cycle starting nodes in all schemata_database to cycle_starting_nodes set.
		'''
		if isinstance(parsed_schema, str) and parsed_schema in visited:
			cycle_starting_nodes.add(parsed_schema)
		elif isinstance(parsed_schema, dict):
			name = parsed_schema.get('name')
			type_ = parsed_schema['type']
			if type(type_) is str and type_ in visited:
				cycle_starting_nodes.add(type_)
			elif name:
				visited.add(name)
				new_visited = copy.deepcopy(visited)
				if 'fields' in parsed_schema:
					for field in parsed_schema['fields']:
						self._cycle_detection(field, new_visited, cycle_starting_nodes)
				if type(type_) is dict:
					self._cycle_detection(type_, new_visited, cycle_starting_nodes)
				if type(type_) is list:
					for element in type_:
						self._cycle_detection(element, new_visited, cycle_starting_nodes)
				elif type(type_) is str and type_ in self:
					self._cycle_detection(self.load_schema(type_), new_visited, cycle_starting_nodes)


class CodeGenerator:
	'''
	Driver class for code generation.
	'''

	def __init__(self, schemata: CerializerSchemata, schema_identifier: str) -> None:
		self.context_schema = schema_identifier
		self.buffer_name = 'buffer'
		self.cdefs: List[str] = []
		self.schemata = schemata
		self.dict_name_generator = cerializer.utils.name_generator('d_dict')
		self.val_name_generator = cerializer.utils.name_generator('val')
		self.key_name_generator = cerializer.utils.name_generator('key')
		self.type_name_generator = cerializer.utils.name_generator('type')
		self.int_name_generator = cerializer.utils.name_generator('i')
		self.jinja_env = jinja2.Environment(
			loader = jinja2.FileSystemLoader(
				searchpath = os.path.join(constants.PROJECT_ROOT, 'cerializer', 'templates')
			),
		)
		self.jinja_env.globals['env'] = self.jinja_env
		# this is a bool flag for turning on the DictWrapper feature
		self.jinja_env.globals['quantlane'] = constants.QUANTLANE
		self.necessary_defs: Set[str] = set()
		self.handled_cycles: Set[str] = set()

	def prepare(
		self,
		mode: constants.SerializationMode,
		logical_type: str,
		data_type: str,
		location: str,
		schema: Dict[str, Any],
	) -> str:
		'''
		Returns a de/serialization function string for logical types that need to be prepared first.
		Will raise RuntimeError
		'''
		logical_type = logical_type.replace('-', '_')

		if 'nano_time' in logical_type and not constants.QUANTLANE:
			raise RuntimeError('Trying to serialize Nano Time logical type without the flag quantlane being True. ')
		params = {
			'scale': schema.get('scale', 0),
			'size': schema.get('size', 0),
			'precision': schema.get('precision'),
		}

		read_params = f'fo, {params}' if data_type == 'fixed' else 'fo'
		if mode is constants.SerializationMode.MODE_SERIALIZE:
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
		if type_ in self.schemata and type_ not in constants.BASIC_TYPES:
			schema = self.load_with_context(type_)
			old_context = self.context_schema
			self.context_schema = type_
			code = self.generate_serialization_code(schema, location)
			self.context_schema = old_context
			return code
		return f'write.write_{type_}({self.buffer_name}, {location})'

	def get_deserialization_function(
		self,
		type_: str,
		location: str,
		schema: Optional[Dict[str, Any]] = None,
	) -> str:
		'''
		Returns the corresponding deserialization function call string.
		'''
		if type_ == 'null':
			return f'{location} = None'
		if type_ in self.schemata and type_ not in constants.BASIC_TYPES:
			loaded_schema = self.load_with_context(type_)
			old_context = self.context_schema
			self.context_schema = type_
			code = self.generate_deserialization_code(loaded_schema, location)
			self.context_schema = old_context
			return code
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

	def get_union_serialization(
		self,
		schema: Union[List, Dict[str, Any]],
		location: str,
		is_from_array: bool = False,
	) -> str:
		'''
		Return union serialization string.
		'''
		if is_from_array:
			# this is in case a union is not specified as a standalone type but is declared in array items
			type_ = list(schema)  # since this union schema came from an array, it has to be in list form
			name = None
			new_location = location
		elif isinstance(schema, dict):
			name = schema['name']
			type_ = list(schema['type'])  # schema['type'] has to be list since its a union schema
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
		return template.render(index_name = index_name, types = types, location = new_location,)

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

	def render_code_with_wraparounds(self, schema: Union[str, List[Any], Dict[str, Any]]) -> str:
		code = self.render_code(schema = schema)
		meta_template = self.jinja_env.get_template('meta_template.jinja2')
		rendered_code = meta_template.render(code = code,)
		return rendered_code

	def render_code(self, schema: Union[str, List, Dict[str, Any]]) -> str:
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
		serialization_code = self.generate_serialization_code(schema = schema, location = location)
		serialization_code = '\n'.join(self.cdefs) + '\n' + serialization_code
		self.cdefs = []
		deserialization_code = self.generate_deserialization_code(schema = schema, location = location)
		deserialization_code = '\n'.join(self.cdefs) + '\n' + deserialization_code

		template = self.jinja_env.get_template('template.jinja2')
		rendered_body = template.render(
			location = location,
			buffer_name = self.buffer_name,
			serialization_code = serialization_code,
			deserialization_code = deserialization_code,
			necessary_defs = '\n\n\n\n'.join([i for i in self.necessary_defs if i != '']),
		)
		self.cdefs = []
		self.necessary_defs = set()
		return rendered_body

	def generate_serialization_code(self, schema: Union[str, List, Dict[str, Any]], location: str) -> str:
		'''
		Driver function to handle code generation for a schema.
		'''
		if isinstance(schema, str):
			if self.schemata.is_cycle_starting(schema) and schema not in constants.BASIC_TYPES:
				return self.handle_cycle(constants.SerializationMode.MODE_SERIALIZE, schema, location)
			return self.get_serialization_function(schema, location)
		if isinstance(schema, list):
			return self.get_union_serialization(schema, location, is_from_array = True)
		type_ = schema['type']
		if 'logicalType' in schema:
			prepared = self.prepare(
				constants.SerializationMode.MODE_SERIALIZE,
				schema['logicalType'],
				type_,
				location,
				schema,
			)
			return prepared
		elif type_ == constants.RECORD:
			return '\n'.join((self.generate_serialization_code(field, location)) for field in schema['fields'])
		elif type_ == constants.ARRAY:
			return self.get_array_serialization(schema, location)
		elif type_ == constants.ENUM:
			return self.get_enum_serialization(schema, location)
		elif type_ == constants.MAP:
			return self.get_map_serialization(schema, location)
		elif type_ == constants.FIXED:
			return self.get_serialization_function(type_, location)
		elif type(type_) is dict:
			name = schema['name']
			new_location = f"{location}['{name}']"
			default_if_necessary = cerializer.utils.default_if_necessary(new_location, schema.get('default'))
			default_if_necessary = (default_if_necessary + '\n') if default_if_necessary else ''
			return str(default_if_necessary + self.generate_serialization_code(type_, new_location))
		elif type(type_) is list:
			return self.get_union_serialization(schema, location)
		elif type(type_) is str and type_ in constants.BASIC_TYPES:
			name = schema.get('name')
			if name:
				location = f"{location}['{name}']"
			default_if_necessary = cerializer.utils.default_if_necessary(location, schema.get('default'))
			default_if_necessary = (default_if_necessary + '\n') if default_if_necessary else ''
			return str(default_if_necessary + self.get_serialization_function(type_, location))
		elif type(type_) is str and type_ in self.schemata:
			loaded_schema = self.load_with_context(type_)
			old_context = self.context_schema
			self.context_schema = type_
			if self.schemata.is_cycle_starting(type_):
				return self.handle_cycle(constants.SerializationMode.MODE_SERIALIZE, type_, location)
			name = schema['name']
			new_location = f"{location}['{name}']"
			code = self.generate_serialization_code(loaded_schema, new_location)
			self.context_schema = old_context
			return code
		raise NotImplementedError(f'Cant handle schema = {schema}')

	def load_with_context(self, schema_identifier: str) -> Union[str, List, Dict[str, Any]]:
		return self.schemata.load_schema(schema_identifier, self.context_schema)

	def generate_deserialization_code(self, schema: Union[Dict[str, Any], list, str], location: str) -> str:
		'''
		Driver function to handle code generation for a schema.
		'''
		if isinstance(schema, str):
			if self.schemata.is_cycle_starting(schema) and schema not in constants.BASIC_TYPES:
				return self.handle_cycle(constants.SerializationMode.MODE_DESERIALIZE, schema, location)
			return self.get_deserialization_function(schema, location)
		if isinstance(schema, list):
			return self.get_union_deserialization(schema, location, is_from_array = True)
		if isinstance(schema, dict):
			type_ = schema['type']
			if 'logicalType' in schema:
				prepared = self.prepare(
					constants.SerializationMode.MODE_DESERIALIZE,
					schema['logicalType'],
					type_,
					location,
					schema,
				)
				return prepared
			elif type_ == constants.RECORD:
				field_deserialization = '\n'.join(
					(self.generate_deserialization_code(field, location))
					for field in schema['fields']
				)
				return location + ' = {}\n' + field_deserialization
			elif type_ == constants.ARRAY:
				return self.get_array_deserialization(schema, location)
			elif type_ == constants.ENUM:
				return self.get_enum_deserialization(schema, location)
			elif type_ == constants.MAP:
				return self.get_map_deserialization(schema, location)
			elif type_ == constants.FIXED:
				return self.get_deserialization_function(type_, location, schema = schema)
			elif type(type_) is dict:
				name = schema['name']
				new_location = f"{location}['{name}']"
				return self.generate_deserialization_code(type_, new_location)
			elif type(type_) is list:
				return self.get_union_deserialization(schema, location)
			elif type(type_) is str and type_ in constants.BASIC_TYPES:
				name = schema.get('name')
				if name:
					location = f"{location}['{name}']"
				return self.get_deserialization_function(type_, location, schema = schema)
			elif type(type_) is str and type_ in self.schemata:
				loaded_schema = self.load_with_context(type_)
				old_context = self.context_schema
				self.context_schema = type_
				if self.schemata.is_cycle_starting(type_):
					return self.handle_cycle(constants.SerializationMode.MODE_DESERIALIZE, type_, location)
				name = schema['name']
				new_location = f"{location}['{name}']"
				code = self.generate_deserialization_code(loaded_schema, new_location)
				self.context_schema = old_context
				return code
		raise NotImplementedError(f'Cant handle schema = {schema}')

	def handle_cycle(self, mode: constants.SerializationMode, schema: str, location: str) -> str:
		normalised_type = schema.replace(':', '_').replace('.', '_')
		serialization_function = (
			f'{constants.SerializationMode.MODE_SERIALIZE.value}_{normalised_type}(data, output)'
		)
		deserialization_function = f'{constants.SerializationMode.MODE_DESERIALIZE.value}_{normalised_type}(fo)'
		if schema not in self.handled_cycles:
			self.handled_cycles.add(schema)
			code = self.render_code(self.load_with_context(schema))
			self.necessary_defs.add(
				code.replace(
					f'cpdef {constants.SerializationMode.MODE_SERIALIZE.value}(data, output)',
					f'def {serialization_function}',
				)
				.replace(
					f'def {constants.SerializationMode.MODE_SERIALIZE.value}(data, output)',
					f'def {serialization_function}',
				)
				.replace(
					f'cpdef {constants.SerializationMode.MODE_DESERIALIZE.value}(fo)',
					f'def {deserialization_function}',
				)
				.replace(
					f'def {constants.SerializationMode.MODE_DESERIALIZE.value}(fo)',
					f'def {deserialization_function}',
				)
			)
		serialization_function_call = serialization_function.replace('(data,', f'({location},')
		if mode is constants.SerializationMode.MODE_SERIALIZE:
			return f'output.write(buffer)\nbuffer = bytearray()\n{serialization_function_call}'
		else:
			return f'{location} = {deserialization_function}'

	def add_cdef(self, type_: str, name: str) -> None:
		cdef = f'cdef {type_} {name}'
		self.cdefs.append(cdef)

	def correct_constraint(
		self,
		type_: Union[Dict[str, Any], str, List],
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

		elif isinstance(type_, str) and type_ in self.schemata:
			return self.correct_constraint(self.load_with_context(type_), location, key, first, value)

		elif isinstance(type_, dict) and type_['type'] == 'record':
			# TODO adjust for different dict types
			constraint = f'type({full_location}) is dict'

		if constraint:
			return f'{"if" if first else "elif"} {constraint}:'
		raise RuntimeError(f'invalid constraint for type == {type_}')
