# from distutils.sysconfig import get_config_var
import importlib.machinery
import os
import io
import struct
from typing import Any, Dict, Hashable, Iterator, List, Tuple, Union

import schemachinery.codec.avro_codec
import schemachinery.codec.avro_schemata
import yaml
import constants.constants
import cerializer.schema_handler


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
	for schema_root in schema_roots:
		for namespace in [f for f in os.listdir(schema_root) if not f.startswith('.')]:
			for schema_name in [f for f in os.listdir(os.path.join(schema_root, namespace)) if not f.startswith('.')]:
				for version in [
					f
					for f in os.listdir(os.path.join(schema_root, namespace, schema_name))
					if not f.startswith('.')
				]:
					yield schema_root, namespace, schema_name, int(version)


def iterate_over_schema_roots(schema_roots: List[str]) -> Iterator[Tuple[str, str]]:
	for schema_root, namespace, schema_name, version in iterate_over_schemata(schema_roots):
		path = os.path.join(schema_root, namespace, schema_name, str(version), 'schema.yaml')
		yield path, get_quantlane_schema_identifier(namespace, schema_name, version)


def get_quantlane_schema_identifier(namespace: str, schema_name: str, schema_version: int) -> str:
	return f'{namespace}.{schema_name}:{schema_version}'


def add_compiled_cerializer_code(schema_roots: List[str]) -> None:
	schemata: List[Tuple[str, Dict[str, Any]]] = []

	for schema_root, namespace, schema_name, version in iterate_over_schemata(schema_roots):
		schema_path = os.path.join(schema_root, namespace, schema_name, str(version), 'schema.yaml')
		schema_tuple = (
			get_quantlane_schema_identifier(namespace, schema_name, version),
			yaml.unsafe_load(open(schema_path)),
		)
		schemata.append(schema_tuple)

	code_generator = cerializer.schema_handler.CodeGenerator(schemata = schemata)

	for schema_root, namespace, schema_name, version in iterate_over_schemata(schema_roots):
		folder_path = os.path.join(schema_root, namespace, schema_name, str(version))
		schema_path = os.path.join(folder_path, 'schema.yaml')
		file_path = os.path.join(folder_path, f'{schema_name}_{version}.pyx')
		schema = yaml.unsafe_load(open(schema_path))
		with open(file_path, mode = 'w+') as f:
			rendered_code = code_generator.render_code_with_wraparounds(schema)
			f.write(rendered_code)
		os.chdir(folder_path)
		os.system(f'python {os.path.join(constants.constants.PROJECT_ROOT, "build.py")} build_ext --inplace')


def get_module(schema_root: str, namespace: str, name: str, version: int) -> Any:
	folder_path = os.path.join(schema_root, namespace, name, str(version))
	so_files = [file_name for file_name in os.listdir(folder_path) if file_name.endswith('.so')]
	if len(so_files) != 1:
		return None
	x = importlib.machinery.ExtensionFileLoader(
		f'{name}_{version}',
		os.path.join(schema_root, namespace, name, str(version), so_files[0]),
	).load_module()
	return x.__invoke()


def _get_namespace_to_schema_root_mapping(schema_roots: List[str]) -> Dict[str, str]:
	mapping: Dict[str, str] = {}
	for schema_root, namespace, _, _ in iterate_over_schemata(schema_roots):
		mapping[namespace] = schema_root
	return mapping


class CerializerQuantaneCodec:
	def __init__(self, schema_roots: List[str], namespace: str, schema_name: str, schema_version: int) -> None:
		self.schema_version = schema_version
		self.namespace_to_schema_root_mapping = _get_namespace_to_schema_root_mapping(schema_roots)
		schema_root = self.namespace_to_schema_root_mapping[namespace]
		module = get_module(schema_root, namespace, schema_name, schema_version)
		if module:
			self.serialization_function = module['serialize']
			self.deserialization_function = module['deserialize']
		else:
			# this occurs when we did not find a corresponding compiled cerializer module
			avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(*schema_roots)
			avro_codec = schemachinery.codec.avro_codec.AvroCodec(
				avro_schemata = avro_schemata,
				namespace = namespace,
				schema_name = schema_name,
				expected_version = schema_version,
			)
			self.encode = avro_codec.encode
			self.decode = avro_codec.decode

	def encode(self, data: Any) -> bytes:
		output = io.BytesIO()
		self.serialization_function(data, output)
		return MAGIC_BYTE + struct.pack('>I', self.schema_version) + output.getvalue()

	def decode(self, data: bytes) -> Any:
		# we assume correct version of schema
		# removing magic byte and version prefix
		data_io = io.BytesIO(data[5:])
		return self.deserialization_function(data_io)
