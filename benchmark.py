from constants.data_constants import *
import fastavro
import timeit
import schema_parser
import avro.schema
import yaml




def benchmark(schema_favro, path_cerializer, schema_cerializer, count, schema_name, schema_version):
	setup = f'''
import benchmark
import fastavro
import json
import io
import json
import yaml
import cerializer_base.{schema_name + '_' + str(schema_version)} as c

schema_favro = {schema_favro}
schema_serializer = {schema_cerializer}
data = yaml.safe_load(open('{path_cerializer}' + 'example.yaml'))
parsed_schema = fastavro.parse_schema(schema_favro)
output = io.BytesIO()
	'''


	score_fastavro_serialize = timeit.Timer(
		stmt = 'fastavro.schemaless_writer(output, parsed_schema, data)',
		setup = setup
	).timeit(number = count)

	score_cerializer_serialize = timeit.Timer(
		stmt = 'c.serialize(data, output)',
		setup = setup
	).timeit(number = count)

	score_json_serialize = timeit.Timer(
		stmt = 'json.dumps(data)',
		setup = setup
	).timeit(number = count)

	return (score_cerializer_serialize, score_fastavro_serialize, score_json_serialize)


for schema, version in [
	('array_schema', 1),
	('union_schema', 1),
	('BBGStockInfo', 2),
	('enum_schema', 1),
	('string_schema', 1),
]:
	SCHEMA_FILE = f'/home/matejmicek/work/Cerializer/schemata/messaging/{schema}/{version}/schema.yaml'
	SCHEMA_CERIALIZER = schema_parser.parse_schema_from_file(SCHEMA_FILE)
	SCHEMA_FAVRO = yaml.load(open(SCHEMA_FILE), Loader = yaml.Loader)
	result = benchmark(
		schema_cerializer = SCHEMA_CERIALIZER,
		path_cerializer = f'/home/matejmicek/work/Cerializer/schemata/messaging/{schema}/{version}/',
		schema_favro = SCHEMA_FAVRO,
		count = 100000,
		schema_name = schema,
		schema_version = version
	)

	print('-----------------------------------------------------------------------')
	print(f'Benchmark for schema {schema}')
	print('-----------------------------------------------------------------------')
	print(result)
	print(f'Cerializer is {result[1]/result[0]}x faster than Fastavro')
	print(f'Cerializer is {result[2]/result[0]}x faster than Json')
