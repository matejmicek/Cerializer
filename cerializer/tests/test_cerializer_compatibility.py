import io

import fastavro
import pytest
import yaml
import os

import cerializer.cerializer_handler
import cerializer.compiler
import constants.constants
from schemachinery.codec import avro_codec
import schemachinery.codec.avro_schemata



SCHEMA_ROOT = '/home/development/work/Cerializer/cerializer/tests/schemata'
SCHEMA_ROOT = '/home/development/root_schemata'



@pytest.mark.parametrize(
	'schema_name,schema_version',
	constants.constants.iterate_over_schemata(SCHEMA_ROOT)
)
def test_serialization_compatibility(schema_name, schema_version):
	namespace = 'messaging'
	path = f'{SCHEMA_ROOT}/{namespace}/{schema_name}/{schema_version}/'
	try:
		data = yaml.unsafe_load(open(path + 'example.yaml'))
		SCHEMA_FAVRO = yaml.load(open(path + 'schema.yaml'), Loader = yaml.Loader)
		output_fastavro = io.BytesIO()
		output_cerializer = io.BytesIO()
		serialize = cerializer.cerializer_handler.Cerializer(
			[SCHEMA_ROOT]
		).code[cerializer.cerializer_handler.get_schema_identifier(namespace, schema_name, schema_version)]['serialize']
		fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
		serialize(data, output_cerializer)
		assert output_cerializer.getvalue() != io.BytesIO().getvalue()
		assert output_cerializer.getvalue() == output_fastavro.getvalue()
	except FileNotFoundError:
		#missing example file
		assert True



@pytest.mark.parametrize(
	'schema_name,schema_version',
	constants.constants.iterate_over_schemata(SCHEMA_ROOT)
)
def test_deserialization_compatibility(schema_name, schema_version):
	namespace = 'messaging'
	path = f'{SCHEMA_ROOT}/{namespace}/{schema_name}/{schema_version}/'
	try:
		data = yaml.unsafe_load(open(path + 'example.yaml'))
		SCHEMA_FAVRO = yaml.load(open(path + 'schema.yaml'), Loader = yaml.Loader)
		output_fastavro = io.BytesIO()
		deserialize = cerializer.cerializer_handler.Cerializer(
			[SCHEMA_ROOT]
		).code[cerializer.cerializer_handler.get_schema_identifier(namespace, schema_name, schema_version)]['deserialize']
		fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
		output_fastavro.seek(0)
		deserialized_data = deserialize(output_fastavro)
		output_fastavro.seek(0)
		assert deserialized_data == fastavro.schemaless_reader(output_fastavro, SCHEMA_FAVRO)
	except FileNotFoundError:
		# missing example file
		assert True



@pytest.mark.parametrize(
	'schema_name,schema_version',
	constants.constants.iterate_over_schemata(SCHEMA_ROOT)
)
def test_cerializer_codec(schema_name, schema_version):
	namespace = 'messaging'
	try:
		path = os.path.join(SCHEMA_ROOT, namespace, schema_name, str(schema_version))
		data = yaml.unsafe_load(open(os.path.join(path, 'example.yaml')))
		avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(SCHEMA_ROOT)
		codec = avro_codec.AvroCodec(avro_schemata, namespace, schema_name, schema_version)
		output_cerializer = io.BytesIO()
		serialize = cerializer.cerializer_handler.Cerializer(
			[SCHEMA_ROOT]
		).code[cerializer.cerializer_handler.get_schema_identifier(namespace, schema_name, schema_version)]['serialize']
		serialize(data, output_cerializer)
		import fastavro
		encoded = codec.encode(data)
		assert output_cerializer.getvalue() != encoded
	except FileNotFoundError:
		# missing example file
		assert True
