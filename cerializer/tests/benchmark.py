import timeit

import texttable
import yaml
import constants.constants
import datetime



NOT_SUPPORTED_JSON = ('fixed', 'timestamp', 'time', 'decimal', 'date', 'uuid')


def benchmark_schema(schema_favro, path_cerializer, count, schema_name, schema_version, code):
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
import {schema_name + '_' + str(schema_version)} as c
# fixes a Timeit NameError 'mappingproxy'
from types import MappingProxyType as mappingproxy


schema_favro = {schema_favro}
data = yaml.unsafe_load(open('{path_cerializer}' + 'example.yaml'))
parsed_schema = fastavro.parse_schema(schema_favro)
output = io.BytesIO()

import cerializer
import datetime
from decimal import Decimal
from uuid import UUID
import {schema_name + "_" + str(schema_version)} as c
import json

code = """
{code}"""
buff = io.BytesIO()
x = cerializer.compiler.compile(code)['serialize']
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
		score_cerializer_serialize,
		score_string_cerializer,
		score_fastavro_serialize,
		score_json_serialize
	)




def benchmark():
	'''
	Benchmarking function. Compares FastAvro, Cerializer and Json.
	In some cases, Json is not able to serialize given data. In such a case it is given an arbitrary score.
	'''
	schemata = constants.constants.SCHEMATA
	results = []

	for schema, version in schemata:
		with open(f'../cerializer_base/{schema + "_" + str(version)}.pyx', 'r') as f:
			code = f.read()
		SCHEMA_FILE = f'schemata/messaging/{schema}/{version}/schema.yaml'
		SCHEMA_FAVRO = yaml.load(open(SCHEMA_FILE), Loader = yaml.Loader)
		result = benchmark_schema(
			path_cerializer = f'schemata/messaging/{schema}/{version}/',
			schema_favro = SCHEMA_FAVRO,
			count = 100000,
			schema_name = schema,
			schema_version = version,
			code = code
		)

		results.append((result[0]/result[1], result[2]/result[1], result[3]/result[1]))


	names = [f'{schema[0]}:{str(schema[1])}' for schema in schemata]


	table = texttable.Texttable()
	table.header(['schema', 'File Compilation [x faster]', 'FastAvro [x faster]', 'Json [x faster]'])
	file_score = [res[0] for res in results]
	fast_avro_score = [res[1] for res in results]
	json_score = [res[2] for res in results]


	for row in zip(names, file_score, fast_avro_score, json_score):
		table.add_row(row)

	print(table.draw())


benchmark()
