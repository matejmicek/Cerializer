import pyximport; pyximport.install()
import fastavro
import timeit
import schema_parser



RENDERED_FILENAME = 'serializer.pyx'



DATA = {
	'username': 'Matej',
	'age': 22,
	'phone': '7775515677',
	'housenum': '555555',
	'address': {
		'street': 'Masarykova',
		'city': 'Orlova',
		'state_prov': 'MS_kraj',
		'country': 'Czechi',
		'zip': '75551615186153'
	}
}



SCHEMA_SERIALIZER = schema_parser.parse_schema_from_file('schemata/user_schema.avsc')
SCHEMA_FAVRO = fastavro.schema.load_schema('schemata/user_schema.avsc')



def deserialize_fastavro(filename, schema):
	with open(filename, 'rb') as reading_file:
		for _ in fastavro.schemaless_reader(reading_file, schema):
			pass



def benchmark(schema_favro, schema_serializer, data, count):
	setup = f'''
import serializer
import benchmark
import fastavro
import json
import io
import json

schema_favro = {schema_favro}
schema_serializer = {schema_serializer}
data = {data}
parsed_schema = fastavro.parse_schema(schema_favro)
output = io.BytesIO()
	'''

	score_fastavro_serialize = timeit.Timer(
		stmt = 'fastavro.schemaless_writer(output, parsed_schema, data)',
		setup = setup
	).timeit(number = count)

	score_cerializer_serialize = timeit.Timer(
		stmt = 'serializer.serialize(data, output)',
		setup = setup
	).timeit(number = count)

	score_json_serialize = timeit.Timer(
		stmt = 'json.dumps(data)',
		setup = setup
	).timeit(number = count)

	return (score_fastavro_serialize, score_cerializer_serialize, score_json_serialize)



	'''
	score_fastavro_deserialize = timeit.Timer(
		stmt = 'benchmark.deserialize_fastavro(filename_f, parsed_schema)',
		setup = setup
	).timeit(number = count)

	score_matej_deserialize = timeit.Timer(
		stmt = 'serializer.deserialize(filename_m)',
		setup = setup
	).timeit(number = count)

	score_json_serialize = timeit.Timer(
		stmt = 'with open(filename_j, \'w\') as outfile: \n json.dump(data, outfile)',
		setup = setup
	).timeit(number = count)
	
	os.remove(str(filename_f))
	os.remove(str(filename_m))
	os.remove(str(filename_j))
	return {
		'total': {
			'fast_avro ': score_fastavro_deserialize + score_fastavro_serialize,
			'serializer': score_matej_deserialize + score_matej_serialize
		},
		'serialize': {
			'fast_avro ': score_fastavro_serialize,
			'serializer': score_matej_serialize,
			'json': score_json_serialize
		},
		'deserialize': {
			'fast_avro ': score_fastavro_deserialize,
			'serializer': score_matej_deserialize
		}
	}
	'''

result = benchmark(data = DATA, schema_serializer = SCHEMA_SERIALIZER, schema_favro = SCHEMA_FAVRO, count = 10)
print(result)
print((result[1]/result[0] - 1) * 100, '%')
