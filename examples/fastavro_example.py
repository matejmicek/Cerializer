import fastavro



schema = {
	'type': 'record',
	'name': 'User',
	'namespace': 'example.avro',
	'fields': [
		{
			'type': 'string',
			'name': 'name'
		},
		{
			'type': [
				'int',
				'null'
			],
			'name': 'favorite_number'
		},
		{
			'type': [
				'string',
				'null'
			],
			'name': 'favorite_color'
		}
	]
}

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
