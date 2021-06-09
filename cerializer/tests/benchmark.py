import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))



import yaml
import timeit
import cerializer.cerializer
import cerializer.schemata
import cerializer.utils
import tqdm



schemata = None


def get_schemata() -> cerializer.schemata.CerializerSchemata:
	schemata = []
	for schema_identifier, schema_root in tqdm.tqdm(
			list(cerializer.utils.iterate_over_schemata()),
			desc = 'Loading schemata'
	):
		# mypy things yaml has no attribute unsafe_load, which is not true
		schema_tuple = (
			schema_identifier,
			yaml.unsafe_load(open(os.path.join(schema_root, 'schema.yaml'))),  # type: ignore
		)
		schemata.append(schema_tuple)
	return cerializer.schemata.CerializerSchemata(schemata, verbose = True)


def benchmark(number: int = 1000, preheat_number: int = 10) -> str:
	results = []
	report = []
	report_json = []
	for schema_identifier, path in tqdm.tqdm(list(cerializer.utils.iterate_over_schemata()), desc = 'Benchmarking'):
		setup = f'''
import cerializer.tests.dev_utils as t
import cerializer.tests.benchmark as b
import yaml
import cerializer.cerializer
import fastavro
import os
import io
import json


from __main__ import schemata as CERIALIZER_SCHEMATA


t.init_fastavro()
schema_identifier = '{schema_identifier}'
#CERIALIZER_SCHEMATA = b.schemata()
cerializer_codec = cerializer.cerializer.Cerializer(
	cerializer_schemata = CERIALIZER_SCHEMATA,
	namespace = schema_identifier.split('.')[0],
	schema_name = schema_identifier.split('.')[1]
)
data = list(yaml.unsafe_load_all(open(os.path.join('{path}', 'example.yaml'))))[0]  # type: ignore
SCHEMA_FAVRO = fastavro.parse_schema(
	yaml.load(open(os.path.join('{path}', 'schema.yaml')), Loader = yaml.Loader)
)

for i in range({preheat_number}):
	output_cerializer = cerializer_codec.serialize(data)
	deserialized_data = cerializer_codec.deserialize(output_cerializer)
	output_fastavro = io.BytesIO()
	fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
	x = output_fastavro.getvalue()
	output_fastavro.seek(0)
	res = fastavro.schemaless_reader(output_fastavro, SCHEMA_FAVRO)


import io
import json
import yaml
import avro
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import datetime
schema = yaml.load(open(os.path.join('{path}', 'schema.yaml')), Loader = yaml.Loader)
try:
	data_avro = yaml.load(open(os.path.join('{path}', 'example_avro.yaml')), Loader = yaml.Loader)
except:
	data_avro = data
parsed_schema = avro.schema.parse(json.dumps(schema))


		'''

		stmt_avro = '''
output_avro = io.BytesIO()
writer = avro.io.DatumWriter(parsed_schema)
encoder = avro.io.BinaryEncoder(output_avro)
writer.write(data_avro, encoder)
raw_bytes = output_avro.getvalue()
bytes_reader = io.BytesIO(raw_bytes)
decoder = avro.io.BinaryDecoder(bytes_reader)
reader = avro.io.DatumReader(parsed_schema)
data_deserialized = reader.read(decoder)
		'''

		stmt_cerializer = '''
output_cerializer = cerializer_codec.serialize(data)
deserialized_data = cerializer_codec.deserialize(output_cerializer)
		'''

		stmt_fastavro = '''
output_fastavro = io.BytesIO()
fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
x = output_fastavro.getvalue()
output_fastavro.seek(0)
res = fastavro.schemaless_reader(output_fastavro, SCHEMA_FAVRO)
		'''

		stmt_json = '''
json_encoded = json.dumps(data)
json_decoded = json.loads(json_encoded)
		'''
		# we do this since we do first one half and the second half
		half_number = int(number / 2)
		result_cerializer_1 = timeit.timeit(stmt = stmt_cerializer, setup = setup, number = half_number)
		result_fastavro_1 = timeit.timeit(stmt = stmt_fastavro, setup = setup, number = half_number)
		result_avro_1 = timeit.timeit(stmt = stmt_avro, setup = setup, number = half_number)
		result_fastavro_2 = timeit.timeit(stmt = stmt_fastavro, setup = setup, number = half_number)
		result_avro_2 = timeit.timeit(stmt = stmt_avro, setup = setup, number = half_number)
		result_cerializer_2 = timeit.timeit(stmt = stmt_cerializer, setup = setup, number = half_number)

		data = list(yaml.unsafe_load_all(open(os.path.join(path, 'example.yaml'))))[0]
		try:
			import json
			# this will fail if the data has components that are not JSON serializable such as datetime
			serialized = json.dumps(data)
			result_json_1 = 0 if True else timeit.timeit(stmt = stmt_json, setup = setup, number = half_number)
			result_json_2 = 0 if True else timeit.timeit(stmt = stmt_json, setup = setup, number = half_number)
			result_json = result_json_1 + result_json_2
		except:
			result_json = 0

		result_cerializer = result_cerializer_1 + result_cerializer_2
		result_fastavro = result_fastavro_1 + result_fastavro_2
		result_avro = result_avro_1 + result_avro_2
		maximum = max(result_cerializer, result_fastavro, result_avro)
		max_json = max(result_cerializer, result_json)

		if result_json:
			report_json.append(
				f'{schema_identifier.ljust(36, " ")},{result_cerializer/max_json},{result_json/max_json}'
			)

		results.append(
			f'{schema_identifier.ljust(36, " ")},{result_cerializer/maximum},{result_fastavro/maximum},{result_avro/maximum}'
		)
	for r in results:
		report.append(r)
	benchmark_header = '============================== BENCHMARK RESULTS =============================='
	benchmark_header_json = '=========================== BENCHMARK RESULTS JSON ============================'
	return benchmark_header + '\n' + '\n'.join(report) + '\n' + '\n' + benchmark_header_json + '\n' + '\n'.join(report_json)



if __name__ == "__main__":
	schemata = get_schemata()
	# args repeat_number, preheat_number
	if len(sys.argv) < 3:
		report = benchmark()
	else:
		report = benchmark(int(sys.argv[1]), int(sys.argv[2]))
	os.system('clear')
	os.system('export TERM=xterm')
	print(report)
