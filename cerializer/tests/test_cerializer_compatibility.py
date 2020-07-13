import io

import fastavro
import pytest
import yaml

import constants.constants
import cerializer.compiler
import cerializer.cerializer_handler


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
	output_cerializer = io.BytesIO()
	serialize = cerializer.cerializer_handler.Cerializer(
		['schemata']
	).code[f'{schema_name}_{schema_version}']['serialize']
	serialize(data, output_cerializer)
	assert output_cerializer.getvalue() != io.BytesIO().getvalue()
	assert output_cerializer.getvalue() == output_fastavro.getvalue()
