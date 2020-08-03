import avro.schema
import avro.datafile
import avro.io
import pprint


schema = avro.schema.parse(open('../schemata/user_schema.avsc', 'rb').read())

writer = avro.datafile.DataFileWriter(open('result', 'wb'), avro.io.DatumWriter(), schema)
writer.append({'name': 'matej'})

writer.close()

reader = avro.datafile.DataFileReader(open('result', 'rb'), avro.io.DatumReader())
for user in reader:
	print(type(user['username']))
reader.close()
