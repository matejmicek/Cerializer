import io
import fastavro
import pytest
import yaml
from importlib import import_module


@pytest.mark.parametrize(
	'path,schema_name,schema_version',
	[
		('../schemata/messaging/array_schema/1/', 'array_schema', 1),
		('../schemata/messaging/union_schema/1/', 'union_schema', 1),
		('../schemata/messaging/BBGStockInfo/2/', 'BBGStockInfo', 2),
		('../schemata/messaging/string_schema/1/', 'string_schema', 1),
		('../schemata/messaging/enum_schema/1/', 'enum_schema', 1),
		('../schemata/messaging/map_schema/1/', 'map_schema', 1),
		('../schemata/messaging/fixed_schema/1/', 'fixed_schema', 1),
		('../schemata/messaging/timestamp_schema/1/', 'timestamp_schema', 1),
	]
)
def test_array_serialization_compatibility(path, schema_name, schema_version):
	data = yaml.safe_load(open(path + 'example.yaml'))
	SCHEMA_FAVRO = yaml.load(open(path + 'schema.yaml'), Loader = yaml.Loader)
	output_fastavro = io.BytesIO() 
	fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
	c = import_module(f'cerializer_base.{schema_name}_{schema_version}', package = 'cerializer')
	output_cerializer = io.BytesIO()
	c.serialize(data, output_cerializer)
	assert output_cerializer.getvalue() != io.BytesIO().getvalue()
	assert output_cerializer.getvalue() == output_fastavro.getvalue()
