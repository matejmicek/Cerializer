import os
from typing import Any, Dict, Hashable, Iterator, List, Tuple, Union

import yaml



def schema_roots_to_schemata(schema_roots: List[str]) -> List[Tuple[str, Union[Dict[Hashable, Any], list, None]]]:
	schemata: List[Tuple[str, Union[Dict[Hashable, Any], list, None]]] = []
	for schema_path, schema_identifier in iterate_over_schema_roots(schema_roots):
		schema_with_identifier = schema_identifier, yaml.safe_load(open(schema_path))
		schemata.append(schema_with_identifier)
	return schemata



def iterate_over_schemata(schema_roots: List[str]) -> Iterator[Tuple[str, str, str, int]]:
	for schema_root in schema_roots:
		for namespace in [f for f in os.listdir(schema_root) if not f.startswith('.')]:
			for schema_name in [f for f in os.listdir(os.path.join(schema_root, namespace)) if not f.startswith('.')]:
				for version in [
					f
					for f in os.listdir(os.path.join(schema_root, namespace, schema_name))
					if not f.startswith('.')
				]:
					yield schema_root, namespace, schema_name, int(version)



# TODO Delete if not used
def iterate_over_schema_roots(schema_roots: List[str]) -> Iterator[Tuple[str, str]]:
	for schema_root, namespace, schema_name, version in iterate_over_schemata(schema_roots):
		path = os.path.join(schema_root, namespace, schema_name, str(version), 'schema.yaml')
		yield path, get_quantlane_schema_identifier(namespace, schema_name, version)



def get_quantlane_schema_identifier(namespace: str, schema_name: str, schema_version: int) -> str:
	return f'{namespace}.{schema_name}:{schema_version}'
