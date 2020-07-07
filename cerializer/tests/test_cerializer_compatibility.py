import importlib
import io

import fastavro
import pytest
import yaml

import constants.constants



@pytest.mark.parametrize(
	'schema_name,schema_version',
	constants.constants.SCHEMATA
)
def test_serialization_compatibility(schema_name, schema_version):
	path = f'schemata/messaging/{schema_name}/{schema_version}/'
	data = yaml.unsafe_load(open(path + 'example.yaml'))
	SCHEMA_FAVRO = yaml.load(open(path + 'schema.yaml'), Loader = yaml.Loader)
	output_fastavro = io.BytesIO() 
	fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
	c = importlib.import_module(f'cerializer.{schema_name}_{schema_version}', package = 'cerializer')
	output_cerializer = io.BytesIO()
	c.serialize(data, output_cerializer)
	assert output_cerializer.getvalue() != io.BytesIO().getvalue()
	assert output_cerializer.getvalue() == output_fastavro.getvalue()
