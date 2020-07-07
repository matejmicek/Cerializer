import timeit

import texttable
import yaml

import constants.constants



NOT_SUPPORTED_JSON = ('fixed', 'timestamp', 'time', 'decimal', 'date', 'uuid')


def benchmark_schema(schema_favro, path_cerializer, count, schema_name, schema_version):
	'''
	Helper function. This should not be used on its own. Use benchmark() instead.
	'''
	setup = f'''
import benchmark
import fastavro
import json
import io
import json
import yaml
import {schema_name + '_' + str(schema_version)} as c
# fixes a Timeit NameError 'mappingproxy'
from types import MappingProxyType as mappingproxy

schema_favro = {schema_favro}
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




def benchmark():
	'''
	Benchmarking function. Compares FastAvro, Cerializer and Json.
	In some cases, Json is not able to serialize given data. In such a case it is given an arbitrary score.
	'''
	schemata = constants.constants.SCHEMATA
	results = []

	for schema, version in schemata:
		SCHEMA_FILE = f'schemata/messaging/{schema}/{version}/schema.yaml'
		SCHEMA_FAVRO = yaml.load(open(SCHEMA_FILE), Loader = yaml.Loader)
		result = benchmark_schema(
			path_cerializer = f'schemata/messaging/{schema}/{version}/',
			schema_favro = SCHEMA_FAVRO,
			count = 10000,
			schema_name = schema,
			schema_version = version
		)

		results.append((result[1]/result[0], result[2]/result[0]))


	names = [f'{schema[0]}:{str(schema[1])}' for schema in schemata]


	table = texttable.Texttable()
	table.header(['schema', 'FastAvro [x faster]', 'Json [x faster]'])
	fast_avro_score = [res[0] for res in results]
	json_score = [res[1] for res in results]


	for row in zip(names, fast_avro_score, json_score):
		table.add_row(row)

	print(table.draw())

benchmark()