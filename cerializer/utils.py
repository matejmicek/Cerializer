# pylint: disable=protected-access
import copy
import os

import fastavro
import yaml

import cerializer.cerializer_handler


def iterate_over_schemata(schema_root):
	for namespace in [f for f in os.listdir(schema_root) if not f.startswith('.')]:
		for schema_name in [f for f in os.listdir(os.path.join(schema_root, namespace)) if not f.startswith('.')]:
			for version in [
				f
				for f in os.listdir(os.path.join(schema_root, namespace, schema_name))
				if not f.startswith('.')
			]:
				yield schema_name, int(version)


def correct_type(type_):
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
		# TODO float is already double precision - fix for a compile error
		return 'float'
	if type(type_) is str and type_ in ('int', 'null', 'float', 'bytes'):
		return type_


def get_logical_type_constraint(schema, location):
	logical_type = schema['logicalType'].replace('-', '_')
	data_type = schema['type']
	if logical_type == 'decimal':
		params = {'scale': schema.get('scale', 0), 'size': schema.get('size', 0)}
		return f'type(prepare.prepare_{data_type}_{logical_type}({location}, {params})) is {data_type}'
	return f'type(prepare.prepare_{logical_type}({location})) is {data_type}'


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
			parsed = fastavro.parse_schema(json_object)
			return parsed
		except fastavro.schema.UnknownType as e:
			# we ignore missing schema errors since we are going to fill them in later
			fastavro._schema_common.SCHEMA_DEFS[e.name] = {}


def get_subschemata(schema_roots):
	schema_database = {}
	for schema_path, schema_identifier in cerializer.cerializer_handler.iterate_over_schema_roots(schema_roots):
		schema = parse_schema_from_file(schema_path)
		if '.' in schema_identifier:
			schema_database[schema_identifier] = schema
		scan_schema_for_subschemas(schema, schema_database)
	return schema_database


def scan_schema_for_subschemas(schema, schema_database):
	if type(schema) is dict:
		name = schema.get('name')
		if name and '.' in name:
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


def get_type_name(type_):
	if type(type_) is str:
		return type_
	else:
		return type_.get('name')
