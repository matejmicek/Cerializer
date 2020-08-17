# from distutils.sysconfig import get_config_var
import contextlib
from typing import Any, Dict, Hashable, Iterator, List, Tuple, Union

import importlib.machinery
import os

import yaml

import cerializer.quantlane_cerializer
import cerializer.schema_handler
import constants.constants
import logging

MAGIC_BYTE = b'\x00'


def schema_roots_to_schemata(
	schema_roots: List[
		str
	],
) -> List[Tuple[str, Union[Dict[Hashable, Any], list, None]]]:
	schemata: List[Tuple[str, Union[Dict[Hashable, Any], list, None]]] = []
	for schema_path, schema_identifier in iterate_over_schema_roots(schema_roots):
		schema_with_identifier = schema_identifier, yaml.unsafe_load(open(schema_path))
		schemata.append(schema_with_identifier)
	return schemata


def iterate_over_schemata(schema_roots: List[str]) -> Iterator[Tuple[str, str, str, int]]:
	# in case there are tests or other non complient files in the schema folder
	for schema_root in schema_roots:
		with contextlib.suppress(NotADirectoryError):
			for namespace in [f for f in os.listdir(schema_root) if not f.startswith('.')]:
				with contextlib.suppress(NotADirectoryError):
					for schema_name in [f for f in os.listdir(os.path.join(schema_root, namespace)) if not f.startswith('.')]:
						with contextlib.suppress(NotADirectoryError):
							for version in [
								f
								for f in os.listdir(os.path.join(schema_root, namespace, schema_name))
								if not f.startswith('.')
							]:
								if os.path.isdir(os.path.join(schema_root, namespace, schema_name, version)) and version.isdigit():
									yield schema_root, namespace, schema_name, int(version)


def iterate_over_schema_roots(schema_roots: List[str]) -> Iterator[Tuple[str, str]]:
	for schema_root, namespace, schema_name, version in iterate_over_schemata(schema_roots):
		path = os.path.join(schema_root, namespace, schema_name, str(version), 'schema.yaml')
		yield path, get_quantlane_schema_identifier(namespace, schema_name, version)


def get_quantlane_schema_identifier(namespace: str, schema_name: str, schema_version: int) -> str:
	return f'{namespace}.{schema_name}:{schema_version}'


def add_compiled_cerializer_code(compiled_schema_roots: List[str], helper_schema_roots: List[str] = None) -> None:
	helper_schema_roots = helper_schema_roots if helper_schema_roots else []
	schemata: List[Tuple[str, Dict[str, Any]]] = []
	all_roots = compiled_schema_roots + helper_schema_roots
	for schema_root, namespace, schema_name, version in iterate_over_schemata(all_roots):
		schema_path = os.path.join(schema_root, namespace, schema_name, str(version), 'schema.yaml')
		schema_tuple = (
			get_quantlane_schema_identifier(namespace, schema_name, version),
			yaml.unsafe_load(open(schema_path)),
		)
		schemata.append(schema_tuple)

	cerializer_schemata = cerializer.schema_handler.CerializerSchemata(schemata)

	for schema_root, namespace, schema_name, version in iterate_over_schemata(compiled_schema_roots):
		code_generator = cerializer.schema_handler.CodeGenerator(
			schemata = cerializer_schemata,
			schema_identifier = get_quantlane_schema_identifier(namespace, schema_name, version),
		)
		folder_path = os.path.join(schema_root, namespace, schema_name, str(version))
		schema_path = os.path.join(folder_path, 'schema.yaml')
		file_path = os.path.join(folder_path, f'{schema_name}_{version}.pyx')
		schema = yaml.unsafe_load(open(schema_path))
		with open(file_path, mode = 'w+') as f:
			rendered_code = code_generator.render_code_with_wraparounds(schema)
			f.write(rendered_code)
		os.chdir(folder_path)
		os.system(f'python {os.path.join(constants.constants.PROJECT_ROOT, "build.py")} build_ext --inplace')


def get_module(schema_roots: List[str], namespace: str, name: str, version: int) -> Any:
	so_files: List[str] = []
	for schema_root in schema_roots:
		with contextlib.suppress(FileNotFoundError):
			folder_path = os.path.join(schema_root, namespace, name, str(version))
			so_files += [
				os.path.join(folder_path, file_name)
				for file_name in os.listdir(folder_path)
				if file_name.endswith('.so')
			]

	if len(so_files) != 1:
		if not so_files:
			raise cerializer.quantlane_cerializer.MissingCerializerCode(
				f'Missing Cerializer code for schema = {name} and version = {version}'
			)
		else:
			raise cerializer.quantlane_cerializer.MissingCerializerCode(
				f'More than one .so file for schema = {name} and version = {version}'
			)
	x = importlib.machinery.ExtensionFileLoader(f'{name}_{version}', so_files[0],).load_module()
	return x.__invoke()
