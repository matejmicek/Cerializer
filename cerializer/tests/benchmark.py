import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import yaml
import timeit
import cerializer.cerializer
import cerializer.schemata
import cerializer.utils



schemata = None


def get_schemata() -> cerializer.schemata.CerializerSchemata:
	schemata = []
	for schema_identifier, schema_root in cerializer.utils.iterate_over_schemata():
		# mypy things yaml has no attribute unsafe_load, which is not true
		schema_tuple = (
			schema_identifier,
			yaml.unsafe_load(open(os.path.join(schema_root, 'schema.yaml'))),  # type: ignore
		)
		schemata.append(schema_tuple)
	print(f'Compiling code for {len(schemata)} Cerializer schemata.')
	return cerializer.schemata.CerializerSchemata(schemata)


def benchmark(number: int = 1000, preheat_number: int = 1000) -> str:
	print(f'Starting Benchmark.')
	print()
	results = []
	total_cerializer = 0.0
	total_fastavro = 0.0
	report = []
	for schema_identifier, path in cerializer.utils.iterate_over_schemata():
		print(f'Benchmarking schema {schema_identifier}.')
		setup = f'''
import cerializer.tests.dev_utils as t
import cerializer.tests.benchmark as b
import yaml
import cerializer.cerializer
import fastavro
import os
import io

from __main__ import schemata as CERIALIZER_SCHEMATA


t.init_fastavro()
schema_identifier = '{schema_identifier}'
#CERIALIZER_SCHEMATA = b.schemata()
cerializer_codec = cerializer.cerializer.Cerializer(
	cerializer_schemata = CERIALIZER_SCHEMATA,
	namespace = schema_identifier.split('.')[0],
	schema_name = schema_identifier.split('.')[1]
)
print('Loading test data.', end = " ")
data = list(yaml.unsafe_load_all(open(os.path.join('{path}', 'example.yaml'))))[0]  # type: ignore
SCHEMA_FAVRO = fastavro.parse_schema(
	yaml.load(open(os.path.join('{path}', 'schema.yaml')), Loader = yaml.Loader)
)

print(f'Preheating by serializing and deserializing {preheat_number} times.')
for i in range({preheat_number}):
	output_cerializer = cerializer_codec.serialize(data)
	deserialized_data = cerializer_codec.deserialize(output_cerializer)
	output_fastavro = io.BytesIO()
	fastavro.schemaless_writer(output_fastavro, SCHEMA_FAVRO, data)
	x = output_fastavro.getvalue()
	output_fastavro.seek(0)
	res = fastavro.schemaless_reader(output_fastavro, SCHEMA_FAVRO)
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
		# we do this since we do first one half and the second half
		half_number = int(number / 2)
		print('Benchmarking Cerializer.', end = " ")
		result_cerializer_1 = timeit.timeit(stmt = stmt_cerializer, setup = setup, number = half_number)
		print('Benchmarking FastAvro.', end = " ")
		result_fastavro_1 = timeit.timeit(stmt = stmt_fastavro, setup = setup, number = half_number)
		print('Benchmarking FastAvro.', end = " ")
		result_fastavro_2 = timeit.timeit(stmt = stmt_fastavro, setup = setup, number = half_number)
		print('Benchmarking Cerializer.', end = " ")
		result_cerializer_2 = timeit.timeit(stmt = stmt_cerializer, setup = setup, number = half_number)
		result_cerializer = result_cerializer_1 + result_cerializer_2
		result_fastavro = result_fastavro_1 + result_fastavro_2
		total_cerializer += result_cerializer
		total_fastavro += result_fastavro
		results.append(
			f'{schema_identifier.ljust(36, " ")}   {(result_fastavro/result_cerializer):.4f} times faster,   {result_fastavro:.4f}s : {result_cerializer:.4f}s'
		)
		print(f'Finished benchmarking {schema_identifier}.')
		print()
	for r in results:
		report.append(r)  # dumb_style_checker:disable = print-statement
	report.append(  # dumb_style_checker:disable = print-statement
		f'AVERAGE: {(total_fastavro/total_cerializer):.4f} times faster'
	)
	benchmark_header = '============================== BENCHMARK RESULTS =============================='
	return benchmark_header + '\n' + '\n'.join(report)



if __name__ == "__main__":
	schemata = get_schemata()
	# args repeat_number, preheat_number
	report = benchmark(int(sys.argv[1]), int(sys.argv[2]))
	os.system('clear')
	print(report)
