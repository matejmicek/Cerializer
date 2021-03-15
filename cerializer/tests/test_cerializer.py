# pylint: disable=protected-access
import io
import logging
import os

import fastavro
import pytest
import yaml

import cerializer.cerializer
import cerializer.schemata
import cerializer.schema_parser
import cerializer.utils
import cerializer.constants
import cerializer.tests.dev_utils


SCHEMA_ROOTS = cerializer.constants.TEST_SCHEMATA_ROOTS
SCHEMA_URL = 'http://localhost:8081'



@pytest.fixture(scope = 'module')
def schemata() -> cerializer.schemata.CerializerSchemata:
	schemata = []
	for schema_identifier, schema_root in cerializer.utils.iterate_over_schemata():
		if 'schemata_online' in schema_root:
			# we do not want to add these schemata directly to Cerializer, we want it to download it by itself
			continue
		# mypy things yaml has no attribute unsafe_load, which is not true
		schema_tuple = schema_identifier, yaml.unsafe_load( # type: ignore
			open(os.path.join(schema_root, 'schema.yaml'))
		)
		schemata.append(schema_tuple)
	return cerializer.schemata.CerializerSchemata(schemata, SCHEMA_URL)


@pytest.mark.parametrize(
	'schema_identifier, schema_root',
	cerializer.utils.iterate_over_schemata(),
)
def test_fastavro_compatibility_serialize(
	schema_root: str,
	schema_identifier: str,
	schemata: cerializer.schemata.CerializerSchemata
) -> None:
	# patch for not working avro codec
	cerializer.tests.dev_utils.init_fastavro()
	namespace = schema_identifier.split('.')[0]
	schema_name = schema_identifier.split('.')[1]
	cerializer_codec = cerializer.cerializer.Cerializer(
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
	cerializer.utils.iterate_over_schemata(),
)
def test_fastavro_compatibility_deserialize(
	schema_root: str,
	schema_identifier: str,
	schemata: cerializer.schemata.CerializerSchemata
) -> None:
	# patch for not working avro codec
	cerializer.tests.dev_utils.init_fastavro()
	namespace = schema_identifier.split('.')[0]
	schema_name = schema_identifier.split('.')[1]
	cerializer_codec = cerializer.cerializer.Cerializer(
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
