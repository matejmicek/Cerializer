import os
from typing import Any, Dict, List, Optional, Set, Union

import jinja2

import cerializer.utils
from cerializer import constants


class CodeGenerator:
	'''
	Driver class for code generation.
	This class should normally be only used from withing Cerializer.
	'''

	def __init__(self, schemata, schema_identifier: str) -> None:
		'''

		:param schemata:
		:param schema_identifier:
		'''
		self._context_schema = schema_identifier
		self._buffer_name = 'buffer'
		self._cdefs: List[str] = []
		self._schemata = schemata
		self._dict_name_generator = cerializer.utils.name_generator('d_dict')
		self._val_name_generator = cerializer.utils.name_generator('val')
		self._key_name_generator = cerializer.utils.name_generator('key')
		self._type_name_generator = cerializer.utils.name_generator('type')
		self._int_name_generator = cerializer.utils.name_generator('i')
		self._jinja_env = jinja2.Environment(
			loader = jinja2.FileSystemLoader(
				searchpath = os.path.join(constants.PROJECT_ROOT, 'cerializer', 'templates')
			),
		)
		# Jinja env needs to store a reference to itself for internal reasons.
		self._jinja_env.globals['env'] = self._jinja_env
		# this is a bool flag for turning on the DictWrapper feature
		self._jinja_env.globals['quantlane'] = constants.QUANTLANE
		self._necessary_defs: Set[str] = set()
		self._handled_cycles: Set[str] = set()

	def _prepare(
		self,
		mode: constants.SerializationMode,
		logical_type: str,
		data_type: str,
		location: str,
		schema: Dict[str, Any],
	) -> str:
		'''
		Returns a de/serialization function string for logical types that need to be prepared first.
		Will raise RuntimeError if company specific packages are missing.
		:param mode: serialization or deserialization
		:param logical_type: name of logical type
		:param data_type: name of data type
		:param location: location
		:param schema: schema
		:return: preparation string
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
		# preparation for serialization
		if mode is constants.SerializationMode.MODE_SERIALIZE:
			prepare_params = f'{location}, {params}' if logical_type == 'decimal' else location
			prepare_type = f'{data_type}_{logical_type}' if logical_type == 'decimal' else logical_type
			return f'write.write_{data_type}({self._buffer_name}, prepare.prepare_{prepare_type}({prepare_params}))'
		# preparation for deserialization
		else:
			prepare_params = (
				f'read.read_{data_type}({read_params}), {params}'
				if logical_type == 'decimal'
				else f'read.read_{data_type}({read_params})'
			)
			return f'{location} = prepare.read_{logical_type}({prepare_params})'

	def _get_serialization_function(self, type_: str, location: str) -> str:
		'''
		Returns the corresponding serialization function call string.
		:param type_: type of variable
		:param location: location
		:return: serialization string
		'''
		if type_ == 'null':
			return f'write.write_null({self._buffer_name})'
		if type_ in self._schemata and type_ not in constants.BASIC_TYPES:
			schema = self._load_with_context(type_)
			old_context = self._context_schema
			self._context_schema = type_
			code = self._generate_serialization_code(schema, location)
			self._context_schema = old_context
			return code
		return f'write.write_{type_}({self._buffer_name}, {location})'

	def _get_deserialization_function(
		self,
		type_: str,
		location: str,
		schema: Optional[Dict[str, Any]] = None,
	) -> str:
		'''
		Returns the corresponding deserialization function call string.
		:param type_: type of variable
		:param location: location
		:param schema: schema
		:return: deserialization string
		'''
		if type_ == 'null':
			return f'{location} = None'
		if type_ in self._schemata and type_ not in constants.BASIC_TYPES:
			loaded_schema = self._load_with_context(type_)
			old_context = self._context_schema
			self._context_schema = type_
			code = self._generate_deserialization_code(loaded_schema, location)
			self._context_schema = old_context
			return code
		read_params = f'fo, {schema}' if type_ == 'fixed' else 'fo'
		return f'{location} = read.read_{type_}({read_params})'

	def _get_union_index_function(self, index: int) -> str:
		'''
		Returns a function call string for union index.
		:param index: index in the union
		:return: serialization string
		'''
		return f'write.write_long({self._buffer_name}, {index})'

	def _get_array_serialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Returns array serialization string.
		:param schema: schema
		:param location: location
		:return: array serialization string
		'''
		item_name = next(self._val_name_generator)
		item_serialization_code = self._generate_serialization_code(schema['items'], item_name)
		template = self._jinja_env.get_template('array_serialization.jinja2')
		return template.render(
			location = location,
			buffer_name = self._buffer_name,
			item_serialization_code = item_serialization_code,
			item_name = item_name,
		)

	def _get_array_deserialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Returns array deserialization string.
		:param schema: schema
		:param location: location
		:return: array deserialization string
		'''
		index_name = next(self._int_name_generator)
		block_count_name = next(self._int_name_generator)
		potential_item_name = next(self._val_name_generator)
		self._add_cdef('long long', index_name)
		self._add_cdef('long long', block_count_name)
		template = self._jinja_env.get_template('array_deserialization.jinja2')
		return template.render(
			location = location,
			buffer_name = self._buffer_name,
			items = schema['items'],
			index_name = index_name,
			block_count_name = block_count_name,
			potential_item_name = potential_item_name,
		)

	def _get_enum_serialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Returns enum serialization string.
		:param schema: schema
		:param location: location
		:return: enum serialization string
		'''
		symbols = schema['symbols']
		return f'write.write_int({self._buffer_name}, {symbols}.index({location}))'

	def _get_enum_deserialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Returns enum deserialization string.
		:param schema: schema
		:param location: location
		:return: enum deserialization string
		'''
		symbols = schema['symbols']
		return f'{location} = {symbols}[read.read_int(fo)]'

	def _get_union_serialization(
		self,
		schema: Union[List, Dict[str, Any]],
		location: str,
		is_from_array: bool = False,
	) -> str:
		'''
		Returns union serialization string.
		:param schema: schema
		:param location: location
		:param is_from_array: whether this union is from an array
		:return: union serialization string
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
					self._get_union_index_function(type_.index('null')),
					self._get_serialization_function('null', new_location),
				)
			)
		for possible_type in type_:
			if possible_type == 'null':
				continue
			possible_types_and_code.append(
				(
					possible_type,
					self._get_union_index_function(type_.index(possible_type)),
					self._generate_serialization_code(possible_type, new_location),
				)
			)
		type_name = next(self._type_name_generator)
		self._add_cdef('str', type_name)
		data_name = next(self._val_name_generator)
		template = self._jinja_env.get_template('union_serialization.jinja2')
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

	def _get_union_deserialization(
		self,
		schema: Union[List, Dict[str, Any]],
		location: str,
		is_from_array: bool = False,
	) -> str:
		'''
		Returns union deserialization string.
		:param schema: schema
		:param location: location
		:param is_from_array: whether this union is from an array
		:return: union deserialization string
		'''
		index_name = next(self._int_name_generator)
		self._add_cdef('long', index_name)
		if is_from_array:
			types = schema
			new_location = location
		elif isinstance(schema, dict):
			name = schema['name']
			types = schema['type']
			new_location = f"{location}['{name}']"
		else:
			raise NotImplementedError(f'Cant handle schema = {schema}')
		template = self._jinja_env.get_template('union_deserialization.jinja2')
		return template.render(index_name = index_name, types = types, location = new_location,)

	def _get_map_serialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Returns map serialization string.
		:param schema: schema
		:param location: location
		:return: map serialization string
		'''
		dict_name = next(self._dict_name_generator)
		self._add_cdef('dict', dict_name)
		template = self._jinja_env.get_template('map_serialization.jinja2')
		values = schema['values']
		key_name = next(self._key_name_generator)
		val_name = next(self._val_name_generator)
		self._add_cdef('str', key_name)
		return template.render(
			location = location,
			buffer_name = self._buffer_name,
			values = values,
			key_name = key_name,
			val_name = val_name,
		)

	def _get_map_deserialization(self, schema: Dict[str, Any], location: str) -> str:
		'''
		Returns map deserialization string.
		:param schema: schema
		:param location: location
		:return: map deserialization string
		'''
		key_name = next(self._key_name_generator)
		self._add_cdef('unicode', key_name)
		block_count_name = next(self._int_name_generator)
		self._add_cdef('long', block_count_name)
		index_name = next(self._int_name_generator)
		self._add_cdef('long', index_name)

		template = self._jinja_env.get_template('map_deserialization.jinja2')
		values = schema['values']
		return template.render(
			location = location,
			values = values,
			key_name = key_name,
			block_count_name = block_count_name,
			index_name = index_name,
		)

	def render_code_with_wraparounds(self, schema: Union[str, List[Any], Dict[str, Any]]) -> str:
		'''
		Add Cython compiler directives to speed up the code and adds imports.
		:param schema: schema to render the code for.
		:return: rendered code string with wraparounds
		'''
		code = self._render_code(schema = schema)
		meta_template = self._jinja_env.get_template('meta_template.jinja2')
		rendered_code = meta_template.render(code = code,)
		return rendered_code

	def _render_code(self, schema: Union[str, List, Dict[str, Any]]) -> str:
		'''
		Renders Cython code for the given schema.
		:param schema: schema to render the code for.
		:return: rendered code string
		'''
		self._jinja_env.globals['correct_type'] = cerializer.utils.correct_type
		self._jinja_env.globals['correct_constraint'] = self._correct_constraint
		self._jinja_env.globals['generate_serialization_code'] = self._generate_serialization_code
		self._jinja_env.globals['generate_deserialization_code'] = self._generate_deserialization_code
		self._jinja_env.globals['get_type_name'] = cerializer.utils.get_type_name
		schema = cerializer.utils.parse_schema(schema)
		location = 'data'
		serialization_code = self._generate_serialization_code(schema = schema, location = location)
		serialization_code = '\n'.join(self._cdefs) + '\n' + serialization_code
		self._cdefs = []
		deserialization_code = self._generate_deserialization_code(schema = schema, location = location)
		deserialization_code = '\n'.join(self._cdefs) + '\n' + deserialization_code

		template = self._jinja_env.get_template('template.jinja2')
		rendered_body = template.render(
			location = location,
			buffer_name = self._buffer_name,
			serialization_code = serialization_code,
			deserialization_code = deserialization_code,
			necessary_defs = '\n\n\n\n'.join([i for i in self._necessary_defs if i != '']),
		)
		self._cdefs = []
		self._necessary_defs = set()
		return rendered_body

	def _generate_serialization_code(self, schema: Union[str, List, Dict[str, Any]], location: str) -> str:
		'''
		Generates the serialization part of code.
		:param schema: schema to render the code for.
		:param location: location
		:return: serialization string
		'''
		if isinstance(schema, str):
			if self._schemata.is_cycle_starting(schema) and schema not in constants.BASIC_TYPES:
				return self._handle_cycle(constants.SerializationMode.MODE_SERIALIZE, schema, location)
			return self._get_serialization_function(schema, location)
		if isinstance(schema, list):
			return self._get_union_serialization(schema, location, is_from_array = True)
		type_ = schema['type']
		if 'logicalType' in schema:
			prepared = self._prepare(
				constants.SerializationMode.MODE_SERIALIZE,
				schema['logicalType'],
				type_,
				location,
				schema,
			)
			return prepared
		elif type_ == constants.RECORD:
			return '\n'.join((self._generate_serialization_code(field, location)) for field in schema['fields'])
		elif type_ == constants.ARRAY:
			return self._get_array_serialization(schema, location)
		elif type_ == constants.ENUM:
			return self._get_enum_serialization(schema, location)
		elif type_ == constants.MAP:
			return self._get_map_serialization(schema, location)
		elif type_ == constants.FIXED:
			return self._get_serialization_function(type_, location)
		elif type(type_) is dict:
			name = schema['name']
			new_location = f"{location}['{name}']"
			default_if_necessary = cerializer.utils.default_if_necessary(new_location, schema.get('default'))
			default_if_necessary = (default_if_necessary + '\n') if default_if_necessary else ''
			return str(default_if_necessary + self._generate_serialization_code(type_, new_location))
		elif type(type_) is list:
			return self._get_union_serialization(schema, location)
		elif type(type_) is str and type_ in constants.BASIC_TYPES:
			name = schema.get('name')
			if name:
				location = f"{location}['{name}']"
			default_if_necessary = cerializer.utils.default_if_necessary(location, schema.get('default'))
			default_if_necessary = (default_if_necessary + '\n') if default_if_necessary else ''
			return str(default_if_necessary + self._get_serialization_function(type_, location))
		elif type(type_) is str and type_ in self._schemata:
			loaded_schema = self._load_with_context(type_)
			old_context = self._context_schema
			self._context_schema = type_
			if self._schemata.is_cycle_starting(type_):
				return self._handle_cycle(constants.SerializationMode.MODE_SERIALIZE, type_, location)
			name = schema['name']
			new_location = f"{location}['{name}']"
			code = self._generate_serialization_code(loaded_schema, new_location)
			self._context_schema = old_context
			return code
		raise NotImplementedError(f'Cant handle schema = {schema}')

	def _load_with_context(self, schema_identifier: str) -> Union[str, List, Dict[str, Any]]:
		return self._schemata.load_schema(schema_identifier, self._context_schema)

	def _generate_deserialization_code(self, schema: Union[Dict[str, Any], list, str], location: str) -> str:
		'''
		Generates the deserialization part of code.
		:param schema: schema to render the code for.
		:param location: location
		:return: deserialization string
		'''
		if isinstance(schema, str):
			if self._schemata.is_cycle_starting(schema) and schema not in constants.BASIC_TYPES:
				return self._handle_cycle(constants.SerializationMode.MODE_DESERIALIZE, schema, location)
			return self._get_deserialization_function(schema, location)
		if isinstance(schema, list):
			return self._get_union_deserialization(schema, location, is_from_array = True)
		if isinstance(schema, dict):
			type_ = schema['type']
			if 'logicalType' in schema:
				prepared = self._prepare(
					constants.SerializationMode.MODE_DESERIALIZE,
					schema['logicalType'],
					type_,
					location,
					schema,
				)
				return prepared
			elif type_ == constants.RECORD:
				field_deserialization = '\n'.join(
					(self._generate_deserialization_code(field, location))
					for field in schema['fields']
				)
				return location + ' = {}\n' + field_deserialization
			elif type_ == constants.ARRAY:
				return self._get_array_deserialization(schema, location)
			elif type_ == constants.ENUM:
				return self._get_enum_deserialization(schema, location)
			elif type_ == constants.MAP:
				return self._get_map_deserialization(schema, location)
			elif type_ == constants.FIXED:
				return self._get_deserialization_function(type_, location, schema = schema)
			elif type(type_) is dict:
				name = schema['name']
				new_location = f"{location}['{name}']"
				return self._generate_deserialization_code(type_, new_location)
			elif type(type_) is list:
				return self._get_union_deserialization(schema, location)
			elif type(type_) is str and type_ in constants.BASIC_TYPES:
				name = schema.get('name')
				if name:
					location = f"{location}['{name}']"
				return self._get_deserialization_function(type_, location, schema = schema)
			elif type(type_) is str and type_ in self._schemata:
				loaded_schema = self._load_with_context(type_)
				old_context = self._context_schema
				self._context_schema = type_
				if self._schemata.is_cycle_starting(type_):
					return self._handle_cycle(constants.SerializationMode.MODE_DESERIALIZE, type_, location)
				name = schema['name']
				new_location = f"{location}['{name}']"
				code = self._generate_deserialization_code(loaded_schema, new_location)
				self._context_schema = old_context
				return code
		raise NotImplementedError(f'Cant handle schema = {schema}')

	def _handle_cycle(self, mode: constants.SerializationMode, schema: str, location: str) -> str:
		'''
		For a cycle starting node, adds a function representing this schema that starts the cycle.
		Then, in the code, references this function and this breaks the cycle.
		:param mode: serialization or deserialization
		:param schema: schema name for cycle starting node
		:param location: location
		:return: cycle serialization string
		'''
		normalised_type = schema.replace(':', '_').replace('.', '_')
		serialization_function = (
			f'{constants.SerializationMode.MODE_SERIALIZE.value}_{normalised_type}(data, output)'
		)
		deserialization_function = f'{constants.SerializationMode.MODE_DESERIALIZE.value}_{normalised_type}(fo)'
		if schema not in self._handled_cycles:
			self._handled_cycles.add(schema)
			code = self._render_code(self._load_with_context(schema))
			self._necessary_defs.add(
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

	def _add_cdef(self, type_: str, name: str) -> None:
		'''
		Adds a cdef
		:param type_: type name
		:param name: name for the type
		:return:
		'''
		cdef = f'cdef {type_} {name}'
		self._cdefs.append(cdef)

	def _correct_constraint(
		self,
		type_: Union[Dict[str, Any], str, List],
		location: str,
		key: str,
		first: bool,
		value: Optional[str] = None,
	) -> str:
		'''
		Returns constraints for a certain type.
		:param type_: type for constraint
		:param location: location
		:param key: key in dict
		:param first: is this constraint first
		:param value: value is filled when we are passing in a name of a local variable rather then a dict and a string
		:return: constraint string
		'''
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

		elif isinstance(type_, str) and type_ in self._schemata:
			return self._correct_constraint(self._load_with_context(type_), location, key, first, value)

		elif isinstance(type_, dict) and type_['type'] == 'record':
			constraint = f'type({full_location}) is dict'

		if constraint:
			return f'{"if" if first else "elif"} {constraint}:'
		raise RuntimeError(f'invalid constraint for type == {type_}')
