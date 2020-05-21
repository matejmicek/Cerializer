import fastavro
import avro.schema



schema = avro.schema.parse(open('../cerializer/tests/schemata/messaging/user_schema/1/user_schema.avsc', 'rb').read()).to_json()
print(schema)
parsed_schema = fastavro.parse_schema(schema)


records = [
	{u'name': u'matej', u'favorite_number': None, u'favorite_color': 'yellow'},
	{u'name': u'matej', u'favorite_number': None, u'favorite_color': 'yellow'},
	{u'name': u'matej', u'favorite_number': None, u'favorite_color': 'yellow'}
]


with open('user.avro', 'wb') as output_file:
	fastavro.writer(output_file, parsed_schema, records)

with open('user.avro', 'rb') as reading_file:
	for record in fastavro.reader(reading_file):
		print(record)
