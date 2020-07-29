import timeit
from typing import List, Union, Dict, Hashable, Any, Tuple

import texttable
import yaml
import cerializer.cerializer_handler
import cerializer.tests.test_cerializer_compatibility
import cerializer.utils
import logwood


SCHEMA_ROOT = '/home/development/root_schemata'
SCHEMA_ROOT = '/home/development/work/Cerializer/cerializer/tests/schemata'

SCHEMA_ROOTS = [SCHEMA_ROOT]



def benchmark_schema_deserialize(
	schema_roots: List[str],
	schema_favro: Union[Dict[Hashable, Any], list, None],
	path: str,
	count: int,
	schema_name: str,
	schema_version: int,
	namespace: str,
	schema_identifier: str,
) -> Tuple:
	'''
	Helper function. This should not be used on its own. Use benchmark() instead.
	'''
	setup = f'''
import fastavro
import json
import io
import yaml
import datetime
import cerializer.cerializer_handler as c
from decimal import Decimal
from uuid import UUID
import schemachinery.codec.avro_schemata
import schemachinery.codec.avro_codec
import cerializer.tests.test_cerializer_compatibility

schema_favro = {schema_favro}
data = list(yaml.unsafe_load_all(open('{path}' + 'example.yaml')))[0]
parsed_schema = fastavro.parse_schema(schema_favro)
serialized_data = io.BytesIO()

fastavro.schemaless_writer(serialized_data, parsed_schema, data)
serialized_data.seek(0)

prefix = cerializer.tests.test_cerializer_compatibility.prefix({schema_version})
serialized_data_bytes = prefix + serialized_data.getvalue()
try:
	json_data = json.dumps(data)
except: 
	json_data = None

avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(*{schema_roots})
codec = schemachinery.codec.avro_codec.AvroCodec(avro_schemata, '{namespace}', '{schema_name}', {schema_version})

x = c.Cerializer({schema_roots}).code['{schema_identifier}']['deserialize']
	'''

	score_string_cerializer = timeit.timeit(
		stmt = 'serialized_data.seek(0)\n' 'y = x(serialized_data)',
		setup = setup,
		number = count,
	)
	score_fastavro = timeit.timeit(
		stmt = 'serialized_data.seek(0)\n' 'y = fastavro.schemaless_reader(serialized_data, parsed_schema)',
		setup = setup,
		number = count,
	)

	score_codec = timeit.timeit(
		stmt = 'serialized_data.seek(0)\n' 'y = codec.decode(serialized_data_bytes)',
		setup = setup,
		number = count,
	)

	try:
		score_json = timeit.timeit(stmt = 'y = json.loads(json_data)', setup = setup, number = count)
	except TypeError:
		score_json = 666 * score_string_cerializer

	return (score_string_cerializer, score_codec, score_fastavro, score_json)



def benchmark_schema_serialize(
	schema_roots: str,
	schema_favro: Union[Dict[Hashable, Any], list, None],
	path: str,
	count: int,
	schema_name: str,
	schema_identifier: str,
	schema_version,
	namespace
) -> Tuple:
	'''
	Helper function. This should not be used on its own. Use benchmark() instead.
	'''
	setup = f'''
import fastavro
import json
import io
import yaml
import datetime
import cerializer.cerializer_handler as c
from decimal import Decimal
from uuid import UUID
import schemachinery.codec.avro_schemata
import schemachinery.codec.avro_codec


schema_favro = {schema_favro}
data = list(yaml.unsafe_load_all(open('{path}' + 'example.yaml')))[0]
parsed_schema = fastavro.parse_schema(schema_favro)
output = io.BytesIO()
buff = io.BytesIO()


avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(*{schema_roots})
codec = schemachinery.codec.avro_codec.AvroCodec(avro_schemata, '{namespace}', '{schema_name}', {schema_version})

x = c.Cerializer({schema_roots}).code['{schema_identifier}']['serialize']
	'''

	score_string_cerializer = timeit.timeit(stmt = 'x(data, buff)', setup = setup, number = count)
	score_fastavro_serialize = timeit.timeit(
		stmt = 'fastavro.schemaless_writer(output, parsed_schema, data)',
		setup = setup,
		number = count,
	)

	score_codec_serialize = timeit.timeit(
		stmt = 'encoded = codec.encode(data)',
		setup = setup,
		number = count
	)

	try:
		score_json_serialize = timeit.timeit(stmt = 'json.dumps(data)', setup = setup, number = count)
	except TypeError:
		score_json_serialize = 666 * score_string_cerializer

	return (score_string_cerializer, score_codec_serialize, score_fastavro_serialize, score_json_serialize)



def benchmark(schema_roots, count = 100000) -> str:
	'''
	Benchmarking function. Compares FastAvro, Cerializer and Json.
	In some cases, Json is not able to serialize given data. In such a case it is given an arbitrary score.
	'''
	# TODO supposed to be here???
	logwood.basic_config()
	cerializer.tests.test_cerializer_compatibility.init_fastavro()
	schemata = list(cerializer.utils.iterate_over_schemata(schema_roots))
	table_results_serialize: List[Tuple[Any, Any, Any]] = []
	table_results_deserialize: List[Tuple[Any, Any, Any]] = []
	table_results_roundtrip: List[Tuple[Any, Any, Any]] = []
	for schema_root, namespace, schema, version in schemata:
		SCHEMA_FILE = f'{schema_root}/{namespace}/{schema}/{version}/schema.yaml'
		SCHEMA_FAVRO = yaml.load(open(SCHEMA_FILE), Loader = yaml.Loader)
		result_deserialize = benchmark_schema_deserialize(
			schema_roots = schema_roots,
			schema_version = version,
			namespace = namespace,
			path = f'{schema_root}/{namespace}/{schema}/{version}/',
			schema_favro = SCHEMA_FAVRO,
			count = count,
			schema_name = schema,
			schema_identifier = cerializer.utils.get_schema_identifier(namespace, schema, version),
		)

		result_serialize = benchmark_schema_serialize(
			schema_version = version,
			namespace = namespace,
			schema_roots = schema_roots,
			path = f'{schema_root}/{namespace}/{schema}/{version}/',
			schema_favro = SCHEMA_FAVRO,
			count = count,
			schema_name = schema,
			schema_identifier = cerializer.utils.get_schema_identifier(namespace, schema, version),
		)

		table_results_serialize.append(
			(result_serialize[1] / result_serialize[0], result_serialize[2] / result_serialize[0],
			 result_serialize[3] / result_serialize[0])
		)
		table_results_deserialize.append(
			(result_deserialize[1] / result_deserialize[0], result_deserialize[2] / result_deserialize[0],
			 result_deserialize[3] / result_deserialize[0])
		)
		table_results_roundtrip.append(
			(
				(result_deserialize[1] + result_serialize[1]) / (result_deserialize[0] + result_serialize[0]),
				(result_deserialize[2] + result_serialize[2]) / (result_deserialize[0] + result_serialize[0]),
				(result_deserialize[3] + result_serialize[3]) / (result_deserialize[0] + result_serialize[0]),
			)
		)

	names = [f'{schema[2]}:{str(schema[3])}' for schema in schemata]

	tables: List[str] = []

	for heading, results in (
		('serialize', table_results_serialize),
		('deserialize', table_results_deserialize),
		('roundtrip', table_results_roundtrip),
	):
		table = texttable.Texttable()
		table.header([heading + ' benchmark', 'ql Codec [x faster]', 'FastAvro [x faster]', 'Json [x faster]'])
		codec_score = [res[0] for res in results]
		fast_avro_score = [res[1] for res in results]
		json_score = [res[2] for res in results]
		for row in zip(names, codec_score, fast_avro_score, json_score):
			table.add_row(row)
		tables.append(table.draw())
	return '\n\n\n'.join(tables)


print(benchmark(SCHEMA_ROOTS, 100000))
