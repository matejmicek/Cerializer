import timeit

import texttable
import yaml

import cerializer.cerializer_handler
import constants.constants



NOT_SUPPORTED_JSON = ('fixed', 'timestamp', 'time', 'decimal', 'date', 'uuid')



def benchmark_schema(schema_favro, path_cerializer, count, schema_name, schema_identifier):
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
import cerializer.compiler
# fixes a Timeit NameError 'mappingproxy'
from types import MappingProxyType as mappingproxy
fastavro._schema_common.SCHEMA_DEFS['messaging.PlainInt'] = fastavro.parse_schema(yaml.unsafe_load(open('/Users/matejmicek/PycharmProjects/Cerializer/cerializer/tests/schemata/messaging/plain_int/1/schema.yaml')))
fastavro._schema_common.SCHEMA_DEFS['messaging.Profit:1'] = fastavro.parse_schema(yaml.unsafe_load(open('/Users/matejmicek/PycharmProjects/Cerializer/cerializer/tests/schemata/messaging/map_schema/1/schema.yaml')))


schema_favro = {schema_favro}
data = yaml.unsafe_load(open('{path_cerializer}' + 'example.yaml'))
parsed_schema = fastavro.parse_schema(schema_favro)
output = io.BytesIO()

import cerializer.cerializer_handler as c
import datetime
from decimal import Decimal
from uuid import UUID
import json

buff = io.BytesIO()

x = c.Cerializer(['schemata']).code['{schema_identifier}']['serialize']
	'''

	score_fastavro_serialize = timeit.timeit(
		stmt = 'fastavro.schemaless_writer(output, parsed_schema, data)',
		setup = setup,
		number = count
	)

	score_string_cerializer = timeit.timeit(
		stmt = 'x(data, buff)',
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
		score_json_serialize = 666*score_string_cerializer

	return (
		score_string_cerializer,
		score_fastavro_serialize,
		score_json_serialize
	)




def benchmark():
	'''
	Benchmarking function. Compares FastAvro, Cerializer and Json.
	In some cases, Json is not able to serialize given data. In such a case it is given an arbitrary score.
	'''
	schema_root = '/Users/matejmicek/PycharmProjects/Cerializer/cerializer/tests/schemata'
	schemata = list(constants.constants.iterate_over_schemata(schema_root))
	results = []

	for schema, version in schemata:
		SCHEMA_FILE = f'{schema_root}/messaging/{schema}/{version}/schema.yaml'
		SCHEMA_FAVRO = yaml.load(open(SCHEMA_FILE), Loader = yaml.Loader)
		result = benchmark_schema(
			path_cerializer = f'{schema_root}/messaging/{schema}/{version}/',
			schema_favro = SCHEMA_FAVRO,
			count = 100000,
			schema_name = schema,
			schema_identifier = cerializer.cerializer_handler.get_schema_identifier('messaging', schema, version)
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
