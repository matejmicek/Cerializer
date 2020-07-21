import timeit

import texttable
import yaml

import cerializer.cerializer_handler
import constants.constants



NOT_SUPPORTED_JSON = ('fixed', 'timestamp', 'time', 'decimal', 'date', 'uuid')

SCHEMA_ROOT = '/home/development/root_schemata'
SCHEMA_ROOT = '/home/development/work/Cerializer/cerializer/tests/schemata'


def benchmark_schema_serialize(schema_favro, path_cerializer, count, schema_name, schema_identifier):
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

x = c.Cerializer(['{SCHEMA_ROOT}']).code['{schema_identifier}']['serialize']
	'''

	score_string_cerializer = timeit.timeit(
		stmt = 'x(data, buff)',
		setup = setup,
		number = count
	)
	score_fastavro_serialize = timeit.timeit(
		stmt = 'fastavro.schemaless_writer(output, parsed_schema, data)',
		setup = setup,
		number = count
	)

	try:
		score_json_serialize = timeit.timeit(
			stmt = 'json.dumps(data)',
			setup = setup,
			number = count
		)
	except:
		print(f'Schema = {schema_name} has elements not supported by JSON.')
		score_json_serialize = 666*score_string_cerializer

	return (
		score_string_cerializer,
		score_fastavro_serialize,
		score_json_serialize
	)



def benchmark_schema_deserialize(schema_favro, path_cerializer, count, schema_name, schema_identifier):
	'''
	Helper function. This should not be used on its own. Use benchmark() instead.
	'''
	setup = f'''
import benchmark
import fastavro
import json
import io
import yaml
import cerializer.compiler
# fixes a Timeit NameError 'mappingproxy'
from types import MappingProxyType as mappingproxy


schema_favro = {schema_favro}
data = yaml.unsafe_load(open('{path_cerializer}' + 'example.yaml'))
parsed_schema = fastavro.parse_schema(schema_favro)
serialized_data = io.BytesIO()

fastavro.schemaless_writer(serialized_data, parsed_schema, data)
serialized_data.seek(0)
import cerializer.cerializer_handler as c
import datetime
from decimal import Decimal
from uuid import UUID

x = c.Cerializer(['{SCHEMA_ROOT}']).code['{schema_identifier}']['deserialize']
	'''

	score_string_cerializer = timeit.timeit(
		stmt = 'serialized_data.seek(0)\n'
			   'y = x(serialized_data)',
		setup = setup,
		number = count
	)
	score_fastavro_serialize = timeit.timeit(
		stmt = 'serialized_data.seek(0)\n'
			   'y = fastavro.schemaless_reader(serialized_data, parsed_schema)',
		setup = setup,
		number = count
	)

	return (
		score_string_cerializer,
		score_fastavro_serialize,
		6666
	)



def benchmark():
	'''
	Benchmarking function. Compares FastAvro, Cerializer and Json.
	In some cases, Json is not able to serialize given data. In such a case it is given an arbitrary score.
	'''
	schemata = list(constants.constants.iterate_over_schemata(SCHEMA_ROOT))
	results = []
	for schema, version in schemata:
		SCHEMA_FILE = f'{SCHEMA_ROOT}/messaging/{schema}/{version}/schema.yaml'
		SCHEMA_FAVRO = yaml.load(open(SCHEMA_FILE), Loader = yaml.Loader)
		result = benchmark_schema_deserialize(
			path_cerializer = f'{SCHEMA_ROOT}/messaging/{schema}/{version}/',
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
