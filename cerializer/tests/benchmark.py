# Quantlane specific code for benchmarking Cerializer

import timeit
from typing import List, Union, Dict, Hashable, Any, Tuple

import texttable
import yaml
import cerializer.cerializer_handler
import cerializer.tests.test_cerializer_compatibility
import cerializer.utils
import cerializer.quantlane_utils
import logwood


# developer specific path. Serves only as an example.
SCHEMA_ROOT1 = '/home/development/schemata'
SCHEMA_ROOT2 = '/home/development/work/Cerializer/cerializer/tests/schemata'

SCHEMA_ROOTS = [SCHEMA_ROOT2]


def _benchmark_schema_deserialize(
	schema_roots: List[str],
	schema_favro: Union[Dict[Hashable, Any], list, None],
	path: str,
	count: int,
	schema_name: str,
	schema_version: int,
	namespace: str,
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
import cerializer.cerializer_handler
import cerializer
from decimal import Decimal
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
serialized_data_bytes_cerializer = serialized_data.getvalue()
try:
	json_data = json.dumps(data)
except: 
	json_data = None

avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(*{schema_roots})
codec = schemachinery.codec.avro_codec.AvroCodec(avro_schemata, '{namespace}', '{schema_name}', {schema_version})

s =  cerializer.quantlane_utils.schema_roots_to_schemata({schema_roots})
schemata = cerializer.schema_handler.CerializerSchemata(s)
cerializer_codec = cerializer.cerializer_handler.Cerializer(schemata, '{namespace}', '{schema_name}:{schema_version}')
	'''

	score_string_cerializer = timeit.timeit(
		stmt = 'serialized_data.seek(0)\n' 'y = cerializer_codec.deserialize(serialized_data_bytes_cerializer)',
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
		# Json is not able to serialize certain data types: when this happens we give it a score of -1
		score_json = -1 * score_string_cerializer

	return (score_string_cerializer, score_codec, score_fastavro, score_json)


def _benchmark_schema_serialize(
	schema_roots: str,
	schema_favro: Union[Dict[Hashable, Any], list, None],
	path: str,
	count: int,
	schema_name: str,
	schema_version,
	namespace,
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
import cerializer.cerializer_handler
from decimal import Decimal
import schemachinery.codec.avro_schemata
import schemachinery.codec.avro_codec


schema_favro = {schema_favro}
data = list(yaml.unsafe_load_all(open('{path}' + 'example.yaml')))[0]
parsed_schema = fastavro.parse_schema(schema_favro)
output = io.BytesIO()


avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(*{schema_roots})
codec = schemachinery.codec.avro_codec.AvroCodec(avro_schemata, '{namespace}', '{schema_name}', {schema_version})

s =  cerializer.quantlane_utils.schema_roots_to_schemata({schema_roots})
schemata = cerializer.schema_handler.CerializerSchemata(s)
cerializer_codec = cerializer.cerializer_handler.Cerializer(schemata, '{namespace}', '{schema_name}:{schema_version}')
	'''

	score_string_cerializer = timeit.timeit(
		stmt = 'encoded = cerializer_codec.serialize(data)',
		setup = setup,
		number = count,
	)
	score_fastavro_serialize = timeit.timeit(
		stmt = 'fastavro.schemaless_writer(output, parsed_schema, data)',
		setup = setup,
		number = count,
	)

	score_codec_serialize = timeit.timeit(stmt = 'encoded = codec.encode(data)', setup = setup, number = count)

	try:
		score_json_serialize = timeit.timeit(stmt = 'json.dumps(data)', setup = setup, number = count)
	except TypeError:
		# Json is not able to serialize certain data types: when this happens we give it a score of -1
		score_json_serialize = -1 * score_string_cerializer

	return (score_string_cerializer, score_codec_serialize, score_fastavro_serialize, score_json_serialize)


def benchmark(schema_roots, count = 100000) -> str:
	'''
	Benchmarking function. Compares FastAvro, Cerializer and Json.
	In some cases, Json is not able to serialize given data. In such a case it is given an arbitrary score.
	'''
	# this hes to be here because of logs from schema poller
	logwood.basic_config()
	cerializer.tests.test_cerializer_compatibility.init_fastavro(SCHEMA_ROOTS)
	schemata = list(cerializer.quantlane_utils.iterate_over_schemata(schema_roots))
	table_results_serialize: List[Tuple[Any, Any, Any]] = []
	table_results_deserialize: List[Tuple[Any, Any, Any]] = []
	table_results_roundtrip: List[Tuple[Any, Any, Any]] = []
	for schema_root, namespace, schema, version in schemata:
		SCHEMA_FILE = f'{schema_root}/{namespace}/{schema}/{version}/schema.yaml'
		SCHEMA_FAVRO = yaml.load(open(SCHEMA_FILE), Loader = yaml.Loader)
		result_deserialize = _benchmark_schema_deserialize(
			schema_roots = schema_roots,
			schema_version = version,
			namespace = namespace,
			path = f'{schema_root}/{namespace}/{schema}/{version}/',
			schema_favro = SCHEMA_FAVRO,
			count = count,
			schema_name = schema,
		)

		result_serialize = _benchmark_schema_serialize(
			schema_version = version,
			namespace = namespace,
			schema_roots = schema_roots,
			path = f'{schema_root}/{namespace}/{schema}/{version}/',
			schema_favro = SCHEMA_FAVRO,
			count = count,
			schema_name = schema,
		)

		table_results_serialize.append(
			(
				f'{(result_serialize[1] / result_serialize[0]):.3f}\n{result_serialize[1]:.3f}',
				f'{(result_serialize[2] / result_serialize[0]):.3f}\n{result_serialize[2]:.3f}',
				f'{(result_serialize[3] / result_serialize[0]):.3f}\n{result_serialize[3]:.3f}',
			)
		)
		table_results_deserialize.append(
			(
				f'{(result_deserialize[1] / result_deserialize[0]):.3f}\n{result_deserialize[1]:.3f}',
				f'{(result_deserialize[2] / result_deserialize[0]):.3f}\n{result_deserialize[2]:.3f}',
				f'{(result_deserialize[3] / result_deserialize[0]):.3f}\n{result_deserialize[3]:.3f}',
			)
		)
		table_results_roundtrip.append(
			(
				f'{((result_deserialize[1] + result_serialize[1]) / (result_deserialize[0] + result_serialize[0])):.3f}\n{(result_deserialize[1] + result_serialize[1]):.3f}',
				f'{((result_deserialize[2] + result_serialize[2]) / (result_deserialize[0] + result_serialize[0])):.3f}\n{(result_deserialize[2] + result_serialize[2]):.3f}',
				f'{((result_deserialize[3] + result_serialize[3]) / (result_deserialize[0] + result_serialize[0])):.3f}\n{(result_deserialize[3] + result_serialize[3]):.3f}',
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
