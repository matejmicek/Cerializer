import os
import timeit

import yaml

import cerializer.cerializer_handler
import cerializer.schema_handler
import cerializer.tests.test_cerializer


def schemata() -> cerializer.schema_handler.CerializerSchemata:
	schemata = []
	for schema_identifier, schema_root in cerializer.tests.test_cerializer.iterate_over_schemata():
		# mypy things yaml has no attribute unsafe_load, which is not true
		schema_tuple = (
			schema_identifier,
			yaml.unsafe_load(open(os.path.join(schema_root, 'schema.yaml'))),  # type: ignore
		)
		schemata.append(schema_tuple)
	return cerializer.schema_handler.CerializerSchemata(schemata)


def benchmark(number: int) -> None:
	results = []
	total_cerializer = 0.0
	total_fastavro = 0.0
	for schema_identifier, path in cerializer.tests.test_cerializer.iterate_over_schemata():
		setup = f'''
import cerializer.tests.test_cerializer as t
import cerializer.tests.benchmark as b
import yaml
import cerializer.cerializer_handler
import fastavro
import os
import io


t.init_fastavro()
schema_identifier = '{schema_identifier}'
CERIALIZER_SCHEMATA = b.schemata()
cerializer_codec = cerializer.cerializer_handler.Cerializer(
	cerializer_schemata = CERIALIZER_SCHEMATA,
	namespace = schema_identifier.split('.')[0],
	schema_name = schema_identifier.split('.')[1]
)
data = list(yaml.unsafe_load_all(open(os.path.join('{path}', 'example.yaml'))))[0]  # type: ignore
SCHEMA_FAVRO = fastavro.parse_schema(
	yaml.load(open(os.path.join('{path}', 'schema.yaml')), Loader = yaml.Loader)
)
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

		result_cerializer = timeit.timeit(stmt = stmt_cerializer, setup = setup, number = number)
		result_fastavro = timeit.timeit(stmt = stmt_fastavro, setup = setup, number = number)
		total_cerializer += result_cerializer
		total_fastavro += result_fastavro
		results.append(
			f'{schema_identifier.ljust(36, " ")}   {(result_fastavro/result_cerializer):.4f} times faster,   {result_fastavro:.4f}s : {result_cerializer:.4f}s'
		)
	for r in results:
		print(r)  # dumb_style_checker:disable = print-statement
	print(  # dumb_style_checker:disable = print-statement
		f'AVERAGE: {(total_fastavro/total_cerializer):.4f} times faster'
	)

