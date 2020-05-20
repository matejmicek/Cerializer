import io
import fastavro
import pytest
import yaml
from importlib import import_module



@pytest.mark.parametrize(
	'schema_name,schema_version',
	[
		('array_schema', 1),
		('union_schema', 1),
		('BBGStockInfo', 2),
		('string_schema', 1),
		('enum_schema', 1),
		('map_schema', 1),
		('fixed_schema', 1),
		('timestamp_schema', 1),
		('timestamp_schema_micros', 1),
		('fixed_decimal_schema', 1),
		('bytes_decimal_schema', 1),
		('int_date_schema', 1),
		('string_uuid_schema', 1),
		('string_uuid_schema', 1),
		('long_time_micros_schema', 1),
	]
)
def test_serialization_compatibility(schema_name, schema_version):
	path = f'../schemata/messaging/{schema_name}/{schema_version}/'
	data = yaml.unsafe_load(open(path + 'example.yaml'))
	SCHEMA_FAVRO = yaml.load(open(path + 'schema.yaml'), Loader = yaml.Loader)
	output_fastavro = io.BytesIO() 
	fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
	c = import_module(f'cerializer_base.{schema_name}_{schema_version}', package = 'cerializer')
	output_cerializer = io.BytesIO()
	c.serialize(data, output_cerializer)
	assert output_cerializer.getvalue() != io.BytesIO().getvalue()
	assert output_cerializer.getvalue() == output_fastavro.getvalue()
