import avro.schema
import avro.datafile
import avro.io



schema = avro.schema.parse(open('../schemata/user_schema.avsc', 'rb').read())

writer = avro.datafile.DataFileWriter(open('result', 'wb'), avro.io.DatumWriter(), schema)
writer.append({'name': 'Alyssa', 'favorite_number': 256})
writer.append({'name': 'Ben', 'favorite_number': 7, 'favorite_color': 'red'})
writer.close()

reader = avro.datafile.DataFileReader(open('result', 'rb'), avro.io.DatumReader())
for user in reader:
	print(user)
reader.close()
