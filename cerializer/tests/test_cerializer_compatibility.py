import io
import os
import struct

import fastavro
import pytest
import schemachinery.codec.avro_codec
import schemachinery.codec.avro_schemata
import yaml

import cerializer.cerializer_handler
import cerializer.compiler
import cerializer.utils


MAGIC_BYTE = b'\x00'
SCHEMA_ROOT = '/Users/matejmicek/PycharmProjects/schema_root'
SCHEMA_ROOT = '/Users/matejmicek/PycharmProjects/Cerializer/cerializer/tests/schemata'


def prefix(version):
	return MAGIC_BYTE + struct.pack('>I', version)


@pytest.fixture(scope = 'module')
def schema_roots():
	return [SCHEMA_ROOT]


@pytest.fixture(scope = 'module')
def cerializer_instance(schema_roots):
	return cerializer.cerializer_handler.Cerializer(schema_roots)


@pytest.mark.parametrize('schema_name,schema_version', cerializer.utils.iterate_over_schemata(SCHEMA_ROOT))
def test_fastavro_compatibility_serialize(schema_name, schema_version):
	namespace = 'messaging'
	path = f'{SCHEMA_ROOT}/{namespace}/{schema_name}/{schema_version}/'
	try:
		data = yaml.unsafe_load(open(path + 'example.yaml'))
		SCHEMA_FAVRO = yaml.load(open(path + 'schema.yaml'), Loader = yaml.Loader)
		output_fastavro = io.BytesIO()
		output_cerializer = io.BytesIO()
		serialize = cerializer.cerializer_handler.Cerializer([SCHEMA_ROOT]).code[
			cerializer.cerializer_handler.get_schema_identifier(namespace, schema_name, schema_version)
		]['serialize']
		fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
		serialize(data, output_cerializer)
		assert output_cerializer.getvalue() != io.BytesIO().getvalue()
		assert output_cerializer.getvalue() == output_fastavro.getvalue()
	except FileNotFoundError:
		# missing example file
		assert True


@pytest.mark.parametrize('schema_name,schema_version', cerializer.utils.iterate_over_schemata(SCHEMA_ROOT))
def test_fastavro_compatibility_deserialize(schema_name, schema_version):
	namespace = 'messaging'
	path = f'{SCHEMA_ROOT}/{namespace}/{schema_name}/{schema_version}/'
	try:
		data = yaml.unsafe_load(open(path + 'example.yaml'))
		SCHEMA_FAVRO = yaml.load(open(path + 'schema.yaml'), Loader = yaml.Loader)
		output_fastavro = io.BytesIO()
		deserialize = cerializer.cerializer_handler.Cerializer([SCHEMA_ROOT]).code[
			cerializer.cerializer_handler.get_schema_identifier(namespace, schema_name, schema_version)
		]['deserialize']
		fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
		output_fastavro.seek(0)
		deserialized_data = deserialize(output_fastavro)
		output_fastavro.seek(0)
		assert deserialized_data == fastavro.schemaless_reader(output_fastavro, SCHEMA_FAVRO)
	except FileNotFoundError:
		# missing example file
		assert True


@pytest.mark.parametrize('schema_name,schema_version', cerializer.utils.iterate_over_schemata(SCHEMA_ROOT))
def test_codec_compatibility_serialize(schema_name, schema_version, cerializer_instance):
	namespace = 'messaging'
	try:
		path = os.path.join(SCHEMA_ROOT, namespace, schema_name, str(schema_version))
		data = yaml.unsafe_load(open(os.path.join(path, 'example.yaml')))
		avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(SCHEMA_ROOT)
		codec = schemachinery.codec.avro_codec.AvroCodec(avro_schemata, namespace, schema_name, schema_version)
		encoded = codec.encode(data)
		assert not encoded == io.BytesIO()
		output_cerializer = io.BytesIO()
		cerializer_instance.serialize(namespace, schema_name, schema_version, data, output_cerializer)
		assert encoded == prefix(schema_version) + output_cerializer.getvalue()
	except FileNotFoundError:
		# missing example file
		print(f'missing file for {schema_name}:{schema_version}')
		assert True


@pytest.mark.parametrize('schema_name,schema_version', cerializer.utils.iterate_over_schemata(SCHEMA_ROOT))
def test_codec_compatibility_deserialize(schema_name, schema_version, cerializer_instance):
	namespace = 'messaging'
	try:
		path = os.path.join(SCHEMA_ROOT, namespace, schema_name, str(schema_version))
		data = yaml.unsafe_load(open(os.path.join(path, 'example.yaml')))
		avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(SCHEMA_ROOT)
		codec = schemachinery.codec.avro_codec.AvroCodec(avro_schemata, namespace, schema_name, schema_version)
		encoded = codec.encode(data)
		decoded = codec.decode(encoded)
		encoded = encoded[len(prefix(schema_version)) :]
		decoded_cerializer = cerializer_instance.deserialize(
			namespace,
			schema_name,
			schema_version,
			io.BytesIO(encoded),
		)
		assert decoded == decoded_cerializer
	except FileNotFoundError:
		# missing example file
		print(f'missing file for {schema_name}:{schema_version}')
		assert True
