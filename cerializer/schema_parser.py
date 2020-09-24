# Taken from Fastavro version 0.22.6 due to technical import reasons
# Original file: https://github.com/fastavro/fastavro/blob/master/fastavro/_logical_writers.pyx
from typing import Any, Dict, List, Optional, Set, Tuple, Union


PRIMITIVES = {
	'boolean',
	'bytes',
	'double',
	'float',
	'int',
	'long',
	'null',
	'string',
}

# A mapping of named schemas to their actual schema definition
SCHEMA_DEFS: Dict[str, Union[Dict[str, Any], str, List]] = {}

RESERVED_PROPERTIES = {
	'type',
	'name',
	'namespace',
	'fields',  # Record
	'items',  # Array
	'size',  # Fixed
	'symbols',  # Enum
	'values',  # Map
	'doc',
}

OPTIONAL_FIELD_PROPERTIES = {
	'doc',
	'aliases',
	'default',
}

RESERVED_FIELD_PROPERTIES = {'type', 'name'} | OPTIONAL_FIELD_PROPERTIES


class UnknownType(ValueError):
	def __init__(self, name: str) -> None:
		super(UnknownType, self).__init__(name)
		self.name = name


class SchemaParseException(Exception):
	pass


def parse_schema(
	schema: Union[Dict[str, Any], str, List],
	expand: bool = False,
	_write_hint: bool = True,
	_force: bool = False,
) -> Union[Dict[str, Any], str, List]:
	if _force or expand:
		return _parse_schema(schema, '', expand, _write_hint, set())
	elif isinstance(schema, dict) and '__fastavro_parsed' in schema:
		return schema
	elif isinstance(schema, list):
		# If we are given a list we should make sure that the immediate sub
		# schemas have the hint in them
		return [parse_schema(s, expand, _write_hint, _force) for s in schema]
	else:
		return _parse_schema(schema, '', expand, _write_hint, set())


def _parse_schema(
	schema: Union[Dict[str, Any], str, List],
	namespace: str,
	expand: bool,
	_write_hint: bool,
	named_schemas: Set[str],
) -> Union[Dict[str, Any], str, List]:
	# union schemas
	if isinstance(schema, list):
		return [_parse_schema(s, namespace, expand, False, named_schemas) for s in schema]

	# string schemas; this could be either a named schema or a primitive type
	elif not isinstance(schema, dict):
		if schema in PRIMITIVES:
			return schema

		if '.' not in schema and namespace:
			schema = namespace + '.' + schema

		if schema not in SCHEMA_DEFS:
			raise UnknownType(schema)
		elif expand:
			# If `name` is in the schema, it has been fully resolved and so we
			# can include the full schema. If `name` is not in the schema yet,
			# then we are still recursing that schema and must use the named
			# schema or else we will have infinite recursion when printing the
			# final schema
			if 'name' in SCHEMA_DEFS[schema]:
				return SCHEMA_DEFS[schema]
			else:
				return schema
		else:
			return schema

	else:
		# Remaining valid schemas must be dict types
		schema_type = schema['type']

		parsed_schema = {key: value for key, value in schema.items() if key not in RESERVED_PROPERTIES}
		parsed_schema['type'] = schema_type

		if 'doc' in schema:
			parsed_schema['doc'] = schema['doc']

		# Correctness checks for logical types
		logical_type = parsed_schema.get('logicalType')
		if logical_type == 'decimal':
			scale = parsed_schema.get('scale')
			if scale and not isinstance(scale, int):
				raise SchemaParseException('decimal scale must be a postive integer, ' + 'not {}'.format(scale))
			precision = parsed_schema.get('precision')
			if precision and not isinstance(precision, int):
				raise SchemaParseException('decimal precision must be a postive integer, ' + 'not {}'.format(precision))

		if schema_type == 'array':
			parsed_schema['items'] = _parse_schema(schema['items'], namespace, expand, False, named_schemas,)

		elif schema_type == 'map':
			parsed_schema['values'] = _parse_schema(schema['values'], namespace, expand, False, named_schemas,)

		elif schema_type == 'enum':
			_, fullname = schema_name(schema, namespace)
			if fullname in named_schemas:
				raise SchemaParseException('redefined named type: {}'.format(fullname))
			named_schemas.add(fullname)

			SCHEMA_DEFS[fullname] = parsed_schema

			parsed_schema['name'] = fullname
			parsed_schema['symbols'] = schema['symbols']

		elif schema_type == 'fixed':
			_, fullname = schema_name(schema, namespace)
			if fullname in named_schemas:
				raise SchemaParseException('redefined named type: {}'.format(fullname))
			named_schemas.add(fullname)

			SCHEMA_DEFS[fullname] = parsed_schema

			parsed_schema['name'] = fullname
			parsed_schema['size'] = schema['size']

		elif schema_type == 'record' or schema_type == 'error':
			# records
			namespace, fullname = schema_name(schema, namespace)
			if fullname in named_schemas:
				raise SchemaParseException('redefined named type: {}'.format(fullname))
			named_schemas.add(fullname)

			SCHEMA_DEFS[fullname] = parsed_schema

			fields = []
			for field in schema.get('fields', []):
				fields.append(parse_field(field, namespace, expand, named_schemas))

			parsed_schema['name'] = fullname
			parsed_schema['fields'] = fields

			# Hint that we have parsed the record
			if _write_hint:
				parsed_schema['__fastavro_parsed'] = True

		elif schema_type in PRIMITIVES:
			parsed_schema['type'] = schema_type

		else:
			raise UnknownType(str(schema))

		return parsed_schema


def parse_field(
	field: Dict[str, Any],
	namespace: str,
	expand: bool,
	named_schemas: Set[str],
) -> Dict[str, Any]:
	parsed_field = {key: value for key, value in field.items() if key not in RESERVED_FIELD_PROPERTIES}

	for prop in OPTIONAL_FIELD_PROPERTIES:
		if prop in field:
			parsed_field[prop] = field[prop]

	# Aliases must be a list
	aliases = parsed_field.get('aliases', [])
	if not isinstance(aliases, list):
		msg = 'aliases must be a list, not {}'.format(aliases)
		raise SchemaParseException(msg)

	parsed_field['name'] = field['name']
	parsed_field['type'] = _parse_schema(field['type'], namespace, expand, False, named_schemas)

	return parsed_field


def schema_name(schema: Dict[str, Any], parent_ns: Optional[str]) -> Tuple[str, str]:
	try:
		name = schema['name']
	except KeyError:
		msg = '"name" is a required field missing from ' + 'the schema: {}'.format(schema)
		raise SchemaParseException(msg)

	namespace = schema.get('namespace', parent_ns)
	if not namespace:
		return namespace, name

	return namespace, '{}.{}'.format(namespace, name)
