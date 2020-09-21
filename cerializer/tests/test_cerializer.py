# pylint: disable=protected-access
import io
import logging
import os
from typing import Iterator, Tuple

import fastavro
import pytest
import yaml

import cerializer.cerializer_handler
import cerializer.schema_handler
import cerializer.schema_parser
import cerializer.utils



SCHEMA_ROOTS = [os.path.join(os.path.dirname(__file__), 'schemata')]


def iterate_over_schemata() -> Iterator[Tuple[str, str]]:
	for schema_root in SCHEMA_ROOTS:
		for schema_identifier in os.listdir(schema_root):
			if schema_identifier.startswith('.'):
				# in case of folders automatically added by macOS (.DS_Store)
				continue
			yield schema_identifier, os.path.join(schema_root, schema_identifier)


def init_fastavro() -> None:
	for identifier, schema_root in iterate_over_schemata():
		fastavro._schema_common.SCHEMA_DEFS[identifier] = cerializer.utils.parse_schema(
			# mypy things yaml has no attribute unsafe_load, which is not true
			yaml.unsafe_load(os.path.join(schema_root, 'schema.yaml')) # type: ignore
		)


@pytest.fixture(scope = 'module')
def schemata() -> cerializer.schema_handler.CerializerSchemata:
	schemata = []
	for schema_identifier, schema_root in iterate_over_schemata():
		# mypy things yaml has no attribute unsafe_load, which is not true
		schema_tuple = schema_identifier, yaml.unsafe_load( # type: ignore
			open(os.path.join(schema_root, 'schema.yaml'))
		)
		schemata.append(schema_tuple)
	return cerializer.schema_handler.CerializerSchemata(schemata)


@pytest.mark.parametrize(
	'schema_identifier, schema_root',
	iterate_over_schemata(),
)
def test_fastavro_compatibility_serialize(
	schema_root: str,
	schema_identifier: str,
	schemata: cerializer.schema_handler.CerializerSchemata
) -> None:
	# patch for not working avro codec
	init_fastavro()
	namespace = schema_identifier.split('.')[0]
	schema_name = schema_identifier.split('.')[1]
	cerializer_codec = cerializer.cerializer_handler.Cerializer(
		cerializer_schemata = schemata,
		namespace = namespace,
		schema_name = schema_name,
	)
	try:
		# mypy things yaml has no attribute unsafe_load_all, which is not true
		data_all = yaml.unsafe_load_all( # type: ignore
			open(os.path.join(schema_root, 'example.yaml')))
		SCHEMA_FAVRO = fastavro.parse_schema(
			yaml.load(
				open(os.path.join(schema_root, 'schema.yaml')), Loader = yaml.Loader
			)
		)
		for data in data_all:
			output_fastavro = io.BytesIO()
			fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
			output_cerializer = cerializer_codec.serialize(data)
			assert output_cerializer == output_fastavro.getvalue()
	except FileNotFoundError:
		logging.warning('Missing schema or Example file for schema == %s', schema_name)
		assert False


@pytest.mark.parametrize(
	'schema_identifier, schema_root',
	iterate_over_schemata(),
)
def test_fastavro_compatibility_deserialize(
	schema_root: str,
	schema_identifier: str,
	schemata: cerializer.schema_handler.CerializerSchemata
) -> None:
	# patch for not working avro codec
	init_fastavro()
	namespace = schema_identifier.split('.')[0]
	schema_name = schema_identifier.split('.')[1]
	cerializer_codec = cerializer.cerializer_handler.Cerializer(
		cerializer_schemata = schemata,
		namespace = namespace,
		schema_name = schema_name,
	)
	try:
		# mypy things yaml has no attribute unsafe_load_all, which is not true
		data_all = yaml.unsafe_load_all( # type: ignore
			open(os.path.join(schema_root, 'example.yaml'))
		)
		SCHEMA_FAVRO = yaml.load(
			open(os.path.join(schema_root, 'schema.yaml')), Loader = yaml.Loader
		)
		for data in data_all:
			output_fastavro = io.BytesIO()
			fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
			output_fastavro.seek(0)
			deserialized = cerializer_codec.deserialize(output_fastavro.getvalue())
			output_fastavro.seek(0)
			assert deserialized == fastavro.schemaless_reader(output_fastavro, SCHEMA_FAVRO)
	except FileNotFoundError:
		logging.warning(
			'Missing schema or Example file for schema == %s',
			schema_name,
		)
		assert False
