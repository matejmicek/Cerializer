from constants.data_constants import *
import fastavro
import timeit
import schema_parser
import avro.schema
import yaml
import texttable as tt



NOT_SUPPORTED_JSON = ('fixed', 'timestamp', 'decimal', 'date', 'uuid')


def benchmark(schema_favro, path_cerializer, schema_cerializer, count, schema_name, schema_version):
	setup = f'''
import benchmark
import fastavro
import json
import io
import json
import yaml
import cerializer_base.{schema_name + '_' + str(schema_version)} as c
# fixes a Timeit NameError 'mappingproxy'
from types import MappingProxyType as mappingproxy

schema_favro = {schema_favro}
schema_serializer = {schema_cerializer}
data = yaml.unsafe_load(open('{path_cerializer}' + 'example.yaml'))
parsed_schema = fastavro.parse_schema(schema_favro)
output = io.BytesIO()
	'''


	score_fastavro_serialize = timeit.timeit(
		stmt = 'fastavro.schemaless_writer(output, parsed_schema, data)',
		setup = setup,
		number = count
	)

	score_cerializer_serialize = timeit.timeit(
		stmt = 'c.serialize(data, output)',
		setup = setup,
		number = count
	)

	if not any([i in schema_name for i in NOT_SUPPORTED_JSON]):
		score_json_serialize = timeit.timeit(
			stmt = 'json.dumps(data)',
			setup = setup,
			number = count
		)
	else:
		score_json_serialize = 666*score_cerializer_serialize

	return (score_cerializer_serialize, score_fastavro_serialize, score_json_serialize)


schemata = [
	('array_schema', 1),
	('union_schema', 1),
	('BBGStockInfo', 2),
	('enum_schema', 1),
	('string_schema', 1),
	('map_schema', 1),
	('fixed_schema', 1),
	('timestamp_schema', 1),
	('timestamp_schema_micros', 1),
	('bytes_decimal_schema', 1),
	('fixed_decimal_schema', 1),
	('int_date_schema', 1),
	('string_uuid_schema', 1),
]

results = []

for schema, version in schemata:
	SCHEMA_FILE = f'/Users/matejmicek/PycharmProjects/Cerializer/schemata/messaging/{schema}/{version}/schema.yaml'
	SCHEMA_CERIALIZER = schema_parser.parse_schema_from_file(SCHEMA_FILE)
	SCHEMA_FAVRO = yaml.load(open(SCHEMA_FILE), Loader = yaml.Loader)
	result = benchmark(
		schema_cerializer = SCHEMA_CERIALIZER,
		path_cerializer = f'/Users/matejmicek/PycharmProjects/Cerializer/schemata/messaging/{schema}/{version}/',
		schema_favro = SCHEMA_FAVRO,
		count = 10000,
		schema_name = schema,
		schema_version = version
	)

	results.append((result[1]/result[0], result[2]/result[0]))


names = [f'{schema[0]}:{str(schema[1])}' for schema in schemata]


table = tt.Texttable()
table.header(['schema', 'FastAvro [x faster]', 'Json [x faster]'])
fast_avro_score = [res[0] for res in results]
json_score = [res[1] for res in results]


for row in zip(names, fast_avro_score, json_score):
	table.add_row(row)

print(table.draw())


