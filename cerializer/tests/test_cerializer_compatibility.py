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



@pytest.mark.parametrize(
	'schema_name,schema_version',
	constants.constants.SCHEMATA
)
def test_serialization_compatibility(schema_name, schema_version):
	namespace = 'messaging'
	path = f'schemata/{namespace}/{schema_name}/{schema_version}/'
	data = yaml.unsafe_load(open(path + 'example.yaml'))
	SCHEMA_FAVRO = yaml.load(open(path + 'schema.yaml'), Loader = yaml.Loader)
	fastavro._schema_common.SCHEMA_DEFS['messaging.PlainInt'] = yaml.unsafe_load(open('/home/development/work/Cerializer/cerializer/tests/schemata/messaging/plain_int/1/schema.yaml'))
	fastavro._schema_common.SCHEMA_DEFS['messaging.Profit:1'] = yaml.unsafe_load(open('/home/development/work/Cerializer/cerializer/tests/schemata/messaging/map_schema/1/schema.yaml'))
	output_fastavro = io.BytesIO()
	fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
	output_cerializer = io.BytesIO()
	serialize = cerializer.cerializer_handler.Cerializer(
		['schemata']
	).code[cerializer.cerializer_handler.get_schema_identifier(namespace, schema_name, schema_version)]['serialize']
	serialize(data, output_cerializer)
	assert output_cerializer.getvalue() != io.BytesIO().getvalue()
	assert output_cerializer.getvalue() == output_fastavro.getvalue()



@pytest.mark.parametrize(
	'schema_name,schema_version',
	[('BookExposure', 1)]
)
def test_cerializer_codec(schema_name, schema_version):
	root_dir = '/home/development/root_schemata'
	namespace = 'messaging'
	avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(root_dir)
	codec = avro_codec.AvroCodec(avro_schemata, namespace, schema_name, schema_version)
	path = os.path.join(root_dir, namespace, schema_name, str(schema_version))
	data = yaml.unsafe_load(open(os.path.join(path, 'example.yaml')))
	output_cerializer = io.BytesIO()
	serialize = cerializer.cerializer_handler.Cerializer(
		[root_dir]
	).code[cerializer.cerializer_handler.get_schema_identifier(namespace, schema_name, schema_version)]['serialize']
	serialize(data, output_cerializer)
	encoded = codec.encode(data)
	assert output_cerializer.getvalue() != encoded