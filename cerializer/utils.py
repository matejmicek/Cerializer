# pylint: disable=protected-access
import itertools
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import cerializer.schema_parser


def correct_type(type_: Union[Dict[str, Any], str, List[Any]]) -> Optional[str]:
	'''
	Corrects the nuances between Avro type definitions and actual python type names.
	'''
	if type_ == 'string':
		return 'str'
	if type_ == 'boolean':
		return 'bool'
	if type_ == 'long':
		return 'int'
	if type_ == 'double':
		return 'float'
	if isinstance(type_, str) and type_ in ('int', 'null', 'float', 'bytes'):
		return type_
	return None


def get_logical_type_constraint(schema: Dict[str, Any], location: str) -> str:
	logical_type = schema['logicalType'].replace('-', '_')
	data_type = schema['type']
	if logical_type == 'decimal':
		params = {'scale': schema.get('scale', 0), 'size': schema.get('size', 0)}
		return f'type(prepare.prepare_{data_type}_{logical_type}({location}, {params})) is {data_type}'
	return f'type(prepare.prepare_{logical_type}({location})) is {data_type}'


def name_generator(prefix: str) -> Iterator[str]:
	yield from (f'{prefix}_{i}' for i in itertools.count())


def parse_schema(schema: Union[Dict[str, Any], list, str]) -> Any:
	'''
	Wrapper for loading schemata from yaml
	'''
	while True:
		try:
			parsed = cerializer.schema_parser.parse_schema(schema)
			return parsed
		except cerializer.schema_parser.UnknownType as e:
			# we ignore missing schema errors since we are going to fill them in later
			cerializer.schema_parser.SCHEMA_DEFS[e.name] = {}


def get_subschemata(schemata: List[Tuple[str, Any]]) -> Dict[str, Union[str, List, Dict[str, Any]]]:
	schema_database: Dict[str, Union[str, List, Dict[str, Any]]] = {}
	for schema_identifier, schema in schemata:
		parsed_schema = parse_schema(schema)
		if '.' in schema_identifier:
			schema_database[schema_identifier] = cerializer.schema_parser.parse_schema(parsed_schema)
		scan_schema_for_subschemata(parsed_schema, schema_database)
	return schema_database


def scan_schema_for_subschemata(schema: Any, schema_database: Dict[str, Any]) -> None:
	if type(schema) is dict:
		name = schema.get('name')
		if name and '.' in name:
			schema_database[name] = schema
		for _, subschema in schema.items():
			scan_schema_for_subschemata(subschema, schema_database)
	if type(schema) in (list, dict):
		for subschema in schema:
			scan_schema_for_subschemata(subschema, schema_database)


def get_type_name(type_: Union[str, Dict[str, Any]]) -> Optional[str]:
	return type_ if isinstance(type_, str) else type_.get('name')


def default_if_necessary(location: str, default: Any) -> str:
	if default is None:
		return ''
	default = 'None' if default == 'null' else default
	if '[' not in location:
		constraint = f'if {location} is None:'
	else:
		r = location.rfind(']')
		l = location.rfind('[')
		constraint = f'if {location[:l]}.get({location[l+1:r]}) is None:'
	return f'{constraint}\n' f'    {location} = {default}'


def get_schema_identifier(namespace: str, schema_name: str) -> str:
	return f'{namespace}.{schema_name}'
