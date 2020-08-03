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

SCHEMA_ROOT1 = '/home/development/root_schemata'
SCHEMA_ROOT2 = '/home/development/work/Cerializer/cerializer/tests/schemata'

SCHEMA_ROOTS = [SCHEMA_ROOT1, SCHEMA_ROOT2]

@pytest.fixture(scope = 'module')
def schemata():
	return cerializer.quantlane_utils.schema_roots_to_schemata(SCHEMA_ROOTS)


def prefix(version):
	return MAGIC_BYTE + struct.pack('>I', version)


def init_fastavro(schema_roots):
	for schema_identifier, subschema in cerializer.utils.get_subschemata(schema_roots).items():
		fastavro._schema_common.SCHEMA_DEFS[schema_identifier] = subschema  # pylint: disable = protected-access


@pytest.fixture(scope = 'module')
def cerializer_instance(schemata):
	return cerializer.cerializer_handler.Cerializer(schemata)


@pytest.mark.parametrize(
	'schema_root, namespace, schema_name,schema_version',
	cerializer.quantlane_utils.iterate_over_schemata(SCHEMA_ROOTS),
)
def test_fastavro_compatibility_serialize(
	schema_root,
	namespace,
	schema_name,
	schema_version,
	cerializer_instance,
):
	# patch for not working avro codec
	init_fastavro(SCHEMA_ROOTS)
	path = os.path.join(schema_root, namespace, schema_name, str(schema_version))
	try:
		data_all = yaml.unsafe_load_all(open(os.path.join(path, 'example.yaml')))
		SCHEMA_FAVRO = fastavro.parse_schema(
			yaml.load(open(os.path.join(path, 'schema.yaml')), Loader = yaml.Loader)
		)
		for data in data_all:
			output_fastavro = io.BytesIO()
			output_cerializer = io.BytesIO()
			fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
			cerializer_instance.serialize(namespace, schema_name, schema_version, data, output_cerializer)
			assert output_cerializer.getvalue() == output_fastavro.getvalue()
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
def test_fastavro_compatibility_deserialize(
	schema_root,
	namespace,
	schema_name,
	schema_version,
	cerializer_instance,
):
	# patch for not working avro codec
	init_fastavro(SCHEMA_ROOTS)
	path = os.path.join(schema_root, namespace, schema_name, str(schema_version))
	try:
		data_all = yaml.unsafe_load_all(open(os.path.join(path, 'example.yaml')))
		SCHEMA_FAVRO = yaml.load(open(os.path.join(path, 'schema.yaml')), Loader = yaml.Loader)
		for data in data_all:
			output_fastavro = io.BytesIO()
			fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
			output_fastavro.seek(0)
			deserialized_data = cerializer_instance.deserialize(
				namespace,
				schema_name,
				schema_version,
				output_fastavro.getvalue(),
			)
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
def test_codec_compatibility_serialize(
	schema_root,
	namespace,
	schema_name,
	schema_version,
	cerializer_instance,
):
	# patch for not working avro codec
	init_fastavro(SCHEMA_ROOTS)
	try:
		path = os.path.join(schema_root, namespace, schema_name, str(schema_version))
		data_all = yaml.unsafe_load_all(open(os.path.join(path, 'example.yaml')))
		avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(*SCHEMA_ROOTS)
		codec = schemachinery.codec.avro_codec.AvroCodec(avro_schemata, namespace, schema_name, schema_version)
		for data in data_all:
			encoded = codec.encode(data)
			assert not encoded == io.BytesIO()
			output_cerializer = io.BytesIO()
			cerializer_instance.serialize(namespace, schema_name, schema_version, data, output_cerializer)
			assert encoded == prefix(schema_version) + output_cerializer.getvalue()
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
def test_codec_compatibility_deserialize(
	schema_root,
	namespace,
	schema_name,
	schema_version,
	cerializer_instance,
):
	# patch for not working avro codec
	init_fastavro(SCHEMA_ROOTS)
	try:
		path = os.path.join(schema_root, namespace, schema_name, str(schema_version))
		data_all = yaml.unsafe_load_all(open(os.path.join(path, 'example.yaml')))
		avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(*SCHEMA_ROOTS)
		codec = schemachinery.codec.avro_codec.AvroCodec(avro_schemata, namespace, schema_name, schema_version)
		for data in data_all:
			encoded = codec.encode(data)
			decoded = codec.decode(encoded)
			encoded = encoded[len(prefix(schema_version)) :]
			decoded_cerializer = cerializer_instance.deserialize(
				namespace,
				schema_name,
				schema_version,
				encoded,
			)
			assert decoded == decoded_cerializer
	except FileNotFoundError:
		logging.warning(
			'Missing schema or Example file for schema == %s and version == %s',
			schema_name,
			schema_version,
		)
		assert False
