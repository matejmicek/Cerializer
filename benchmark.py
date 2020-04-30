import pyximport; pyximport.install()
import projekt
import fastavro
import timeit
import os
import template_handler



RENDERED_FILENAME = 'serializer.pyx'
SERIALIZATION_FILENAME = 'temp_m'


DATA = {
	'first_name1': 'Matej',
	'last_name1': 'Micek',
	'age1': 22,
	'other1': {
		'a': 'ahoj',
		'b': 15089464645646565
	},
	'first_name2': 'Matej',
	'last_name2': 'Micek',
	'age2': 22,
	'other2': {
		'a': 'ahoj',
		'b': 15089464645646565
	},
	'first_name3': 'Matej',
	'last_name3': 'Micek',
	'age3': 22,
	'other3': {
		'a': 'ahoj',
		'b': 15089464645646565
	},
}


SCHEMA_SERIALIZER = {
	'first_name1': 'str',
	'last_name1': 'str',
	'age1': 'long',
	'other1': {
		'a': 'str',
		'b': 'long'
	},
	'first_name2': 'str',
	'last_name2': 'str',
	'age2': 'long',
	'other2': {
		'a': 'str',
		'b': 'long'
	},
	'first_name3': 'str',
	'last_name3': 'str',
	'age3': 'long',
	'other3': {
		'a': 'str',
		'b': 'long'
	}
}


SCHEMA_FAVRO = {
	'type': 'record',
	'name': 'user_data',
	'namespace': 'example.avro',
	'fields': [
		{
			'type': 'string',
			'name': 'first_name1'
		},
		{
			'type': 'string',
			'name': 'last_name1'
		},
		{
			'type': 'long',
			'name': 'age1'
		},
		{
			'name': 'other1',
			'type': {
				"name": "Dependency",
				"namespace": "com.namespace.dependencies1",
				"type": "record",
				"fields": [
					{"name": "a", "type": ["string", "long"]},
					{"name": "b", "type": "long"}
				]
			}
		},
		{
			'type': 'string',
			'name': 'first_name2'
		},
		{
			'type': 'string',
			'name': 'last_name2'
		},
		{
			'type': 'long',
			'name': 'age2'
		},
		{
			'name': 'other2',
			'type': {
				"name": "Dependency",
				"namespace": "com.namespace.dependencies2",
				"type": "record",
				"fields": [
					{"name": "a", "type": ["string", "long"]},
					{"name": "b", "type": "long"}
				]
			}
		},
		{
			'type': 'string',
			'name': 'first_name3'
		},
		{
			'type': 'string',
			'name': 'last_name3'
		},
		{
			'type': 'long',
			'name': 'age3'
		},
		{
			'name': 'other3',
			'type': {
				"name": "Dependency",
				"namespace": "com.namespace.dependencies3",
				"type": "record",
				"fields": [
					{"name": "a", "type": ["string", "long"]},
					{"name": "b", "type": "long"}
				]
			}
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


def serialize_fastavro(filename, data, parsed_schema):
	with open(filename, 'wb') as output_file:
		fastavro.writer(output_file, parsed_schema, [data])



def deserialize_fastavro(filename):
	with open(filename, 'rb') as reading_file:
		for _ in fastavro.reader(reading_file):
			pass



def benchmark(schema_favro, schema_serializer, data, count):
	filename_m = SERIALIZATION_FILENAME
	filename_f = 'temp_f'
	filename_j = 'temp_j'
	setup = f'''
import serializer
import benchmark
import fastavro
import json

filename_m = b'{filename_m}'
filename_f = b'{filename_f}'
filename_j = b'{filename_j}'
schema_favro = {schema_favro}
schema_serializer = {schema_serializer}
data = {data}
parsed_schema = fastavro.parse_schema(schema_favro)
	'''

	score_fastavro_serialize = timeit.Timer(
		stmt = 'benchmark.serialize_fastavro(filename_f, data, parsed_schema)',
		setup = setup
	).timeit(number = count)

	score_matej_serialize = timeit.Timer(
		stmt = 'serializer.serialize(data, filename_m)',
		setup = setup
	).timeit(number = count)

	score_fastavro_deserialize = timeit.Timer(
		stmt = 'benchmark.deserialize_fastavro(filename_f)',
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
	return {
		'serialize': {
			'fast_avro ': score_fastavro_serialize,
			'serializer': score_matej_serialize,
			'json': score_json_serialize
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

print(benchmark(data = DATA, schema_serializer = SCHEMA_SERIALIZER, schema_favro = SCHEMA_FAVRO, count = 10000))

#print(f'Serilazer was {result["total"]["fast_avro "]/result["total"]["serializer"]} times faster.')

#template_handler.render_schema(rendered_filename = RENDERED_FILENAME, schema = SCHEMA_SERIALIZER)
