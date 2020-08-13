import io
import os
import struct

import fastavro
import pytest
import schemachinery.codec.avro_codec
import schemachinery.codec.avro_schemata
import yaml
import logging
import cerializer.cerializer_handler
import cerializer.compiler
import cerializer.quantlane_utils
import cerializer.utils


MAGIC_BYTE = b'\x00'

# developer specific path. Serves only as an example.
SCHEMA_ROOT1 = '/home/development/root_schemata'
SCHEMA_ROOT2 = '/home/development/work/Cerializer/cerializer/tests/schemata'


SCHEMA_ROOTS = [SCHEMA_ROOT2]


@pytest.fixture(scope = 'module')
def schemata():
	return cerializer.quantlane_utils.schema_roots_to_schemata(SCHEMA_ROOTS)


def prefix(version):
	return MAGIC_BYTE + struct.pack('>I', version)


def init_fastavro(schema_roots):
	schemata = cerializer.quantlane_utils.schema_roots_to_schemata(schema_roots)
	for schema_identifier, subschema in cerializer.utils.get_subschemata(schemata).items():
		fastavro._schema_common.SCHEMA_DEFS[schema_identifier] = subschema  # pylint: disable = protected-access


@pytest.mark.parametrize(
	'schema_root, namespace, schema_name,schema_version',
	cerializer.quantlane_utils.iterate_over_schemata(SCHEMA_ROOTS),
)
def test_fastavro_compatibility_serialize(schema_root, namespace, schema_name, schema_version, schemata):
	# patch for not working avro codec
	init_fastavro(SCHEMA_ROOTS)
	path = os.path.join(schema_root, namespace, schema_name, str(schema_version))
	cerializer_codec = cerializer.cerializer_handler.Cerializer(
		schemata = schemata,
		namespace = namespace,
		schema_name = f'{schema_name}:{schema_version}',
	)
	try:
		data_all = yaml.unsafe_load_all(open(os.path.join(path, 'example.yaml')))
		SCHEMA_FAVRO = fastavro.parse_schema(
			yaml.load(open(os.path.join(path, 'schema.yaml')), Loader = yaml.Loader)
		)
		for data in data_all:
			output_fastavro = io.BytesIO()
			fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
			schema_name = f'{schema_name}:{schema_version}' if ':' not in schema_name else schema_name
			output_cerializer = cerializer_codec.serialize(data)
			assert output_cerializer == output_fastavro.getvalue()
	except FileNotFoundError:
		logging.warning(
			'Missing schema or Example file for schema == %s and version == %s',
			schema_name,
			schema_version,
		)
		assert False


@pytest.mark.parametrize(
	'schema_root, namespace, schema_name,schema_version',
	cerializer.quantlane_utils.iterate_over_schemata(SCHEMA_ROOTS),
)
def test_fastavro_compatibility_deserialize(schema_root, namespace, schema_name, schema_version, schemata):
	# patch for not working avro codec
	init_fastavro(SCHEMA_ROOTS)
	path = os.path.join(schema_root, namespace, schema_name, str(schema_version))
	cerializer_codec = cerializer.cerializer_handler.Cerializer(
		schemata,
		namespace,
		f'{schema_name}:{schema_version}',
	)
	try:
		data_all = yaml.unsafe_load_all(open(os.path.join(path, 'example.yaml')))
		SCHEMA_FAVRO = yaml.load(open(os.path.join(path, 'schema.yaml')), Loader = yaml.Loader)
		for data in data_all:
			output_fastavro = io.BytesIO()
			fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
			output_fastavro.seek(0)
			schema_name = f'{schema_name}:{schema_version}' if ':' not in schema_name else schema_name
			deserialized_data = cerializer_codec.deserialize(output_fastavro.getvalue())
			output_fastavro.seek(0)
			assert deserialized_data == fastavro.schemaless_reader(output_fastavro, SCHEMA_FAVRO)
	except FileNotFoundError:
		logging.warning(
			'Missing schema or Example file for schema == %s and version == %s',
			schema_name,
			schema_version,
		)
		assert False


@pytest.mark.parametrize(
	'schema_root, namespace, schema_name,schema_version',
	cerializer.quantlane_utils.iterate_over_schemata(SCHEMA_ROOTS),
)
def test_codec_compatibility_serialize(schema_root, namespace, schema_name, schema_version, schemata):
	# patch for not working avro codec
	init_fastavro(SCHEMA_ROOTS)
	cerializer_codec = cerializer.cerializer_handler.Cerializer(
		schemata,
		namespace,
		f'{schema_name}:{schema_version}',
	)
	try:
		path = os.path.join(schema_root, namespace, schema_name, str(schema_version))
		data_all = yaml.unsafe_load_all(open(os.path.join(path, 'example.yaml')))
		avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(*SCHEMA_ROOTS)
		codec = schemachinery.codec.avro_codec.AvroCodec(avro_schemata, namespace, schema_name, schema_version)
		for data in data_all:
			encoded = codec.encode(data)
			assert not encoded == io.BytesIO()
			schema_name = f'{schema_name}:{schema_version}' if ':' not in schema_name else schema_name
			output_cerializer = cerializer_codec.serialize(data)
			assert encoded == prefix(schema_version) + output_cerializer
	except FileNotFoundError:
		logging.warning(
			'Missing schema or Example file for schema == %s and version == %s',
			schema_name,
			schema_version,
		)
		assert False


@pytest.mark.parametrize(
	'schema_root, namespace, schema_name, schema_version',
	cerializer.quantlane_utils.iterate_over_schemata(SCHEMA_ROOTS),
)
def test_codec_compatibility_deserialize(schema_root, namespace, schema_name, schema_version, schemata):
	# patch for not working avro codec
	init_fastavro(SCHEMA_ROOTS)
	cerializer_codec = cerializer.cerializer_handler.Cerializer(
		schemata,
		namespace,
		f'{schema_name}:{schema_version}',
	)
	try:
		path = os.path.join(schema_root, namespace, schema_name, str(schema_version))
		data_all = yaml.unsafe_load_all(open(os.path.join(path, 'example.yaml')))
		avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(*SCHEMA_ROOTS)
		codec = schemachinery.codec.avro_codec.AvroCodec(avro_schemata, namespace, schema_name, schema_version)
		for data in data_all:
			encoded = codec.encode(data)
			decoded = codec.decode(encoded)
			encoded = encoded[len(prefix(schema_version)) :]
			schema_name = f'{schema_name}:{schema_version}' if ':' not in schema_name else schema_name
			decoded_cerializer = cerializer_codec.deserialize(encoded)
			assert decoded == decoded_cerializer
	except FileNotFoundError:
		logging.warning(
			'Missing schema or Example file for schema == %s and version == %s',
			schema_name,
			schema_version,
		)
		assert False
