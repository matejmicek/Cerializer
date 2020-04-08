import projekt
import fastavro
import timeit
import pprint
import tempfile



DATA = {
	'first_name': 'Matej',
	'last_name': 'Micek',
	'age': 22
}



SCHEMA = {
	'type': 'record',
	'name': 'user_data',
	'namespace': 'example.avro',
	'fields': [
		{
			'type': 'string',
			'name': 'first_name'
		},
		{
			'type': 'string',
			'name': 'last_name'
		},
		{
			'type': 'int',
			'name': 'age'
		}
	]
}



def do_full_circle_matej(filename, data):
	schema = {
		'first_name': 'string',
		'last_name': 'string',
		'age': 'int'
	}

	projekt.serialize_to_file(filename, data, schema)
	projekt.deserialize_from_file(filename, data, schema)



def serialize_matej(filename, data):
	schema = {
		'first_name': 'string',
		'last_name': 'string',
		'age': 'int'
	}
	projekt.serialize_to_file(filename, data, schema)



def deserialize_matej(filename, data):
	schema = {
		'first_name': 'string',
		'last_name': 'string',
		'age': 'int'
	}
	projekt.deserialize_from_file(filename, data, schema)



def serialize_fastavro(filename, data, parsed_schema):
	with open(filename, 'wb') as output_file:
		fastavro.writer(output_file, parsed_schema, [data])



def deserialize_fastavro(filename):
	with open(filename, 'rb') as reading_file:
		for _ in fastavro.reader(reading_file):
			pass



def benchmark(schema, data, count):
	file_m = tempfile.TemporaryFile()
	filename_m = file_m.name
	file_f = tempfile.TemporaryFile()
	filename_f = file_f.name
	setup = f'''
import benchmark
import fastavro

filename_m = b'{filename_m}'
filename_f = b'{filename_f}'
schema = {schema}
data = {data}
parsed_schema = fastavro.parse_schema(schema)
	'''

	score_fastavro_serialize = timeit.Timer(
		stmt = 'benchmark.serialize_fastavro(filename_f, data, parsed_schema)',
		setup = setup
	).timeit(number = count)

	score_matej_serialize = timeit.Timer(
		stmt = 'benchmark.serialize_matej(filename_m, data)',
		setup = setup
	).timeit(number = count)

	score_fastavro_deserialize = timeit.Timer(
		stmt = 'benchmark.deserialize_fastavro(filename_f)',
		setup = setup
	).timeit(number = count)

	score_matej_deserialize = timeit.Timer(
		stmt = 'benchmark.deserialize_matej(filename_m, data)',
		setup = setup
	).timeit(number = count)

	return {
		'serialize': {
			'fast_avro ': score_fastavro_serialize,
			'serializer': score_matej_serialize
		},
		'deserialize': {
			'fast_avro ': score_fastavro_deserialize,
			'serializer': score_matej_deserialize
		},
		'total': {
			'fast_avro ': score_fastavro_deserialize + score_fastavro_serialize,
			'serializer': score_matej_deserialize + score_matej_serialize
		}
	}

pprint.pprint(benchmark(SCHEMA, DATA, 10000))