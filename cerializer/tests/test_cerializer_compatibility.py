import importlib
import io

import cython
import fastavro
import pytest
import yaml

import constants.constants
import cerializer.compiler


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
	c = importlib.import_module(f'{schema_name}_{schema_version}', package = 'cerializer')
	output_cerializer = io.BytesIO()
	c.serialize(data, output_cerializer)
	assert output_cerializer.getvalue() != io.BytesIO().getvalue()
	assert output_cerializer.getvalue() == output_fastavro.getvalue()



@pytest.mark.parametrize(
	'schema_name,schema_version',
	constants.constants.SCHEMATA
)
def test_string_compiling(schema_name, schema_version):
	path = f'schemata/messaging/{schema_name}/{schema_version}/'
	data = yaml.unsafe_load(open(path + 'example.yaml'))
	output = io.BytesIO()
	SCHEMA_FAVRO = yaml.load(open(path + 'schema.yaml'), Loader = yaml.Loader)
	output_fastavro = io.BytesIO()
	fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
	with open(f'../cerializer_base/{schema_name + "_" + str(schema_version)}.pyx') as f:
		code = f.read()
	serialize = cerializer.compiler.inline(code)['serialize']
	serialize(data, output)
	assert output.getvalue() == output_fastavro.getvalue()