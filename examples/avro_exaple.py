from constants.data_constants import *
import avro.schema
import avro.datafile
import avro.io
import pprint
import io

schema = avro.schema.parse(open('../cerializer/tests/schemata/messaging/user_schema/1/user_schema.avsc', 'rb').read())

writer = avro.datafile.DataFileWriter(io.BytesIO(), avro.io.DatumWriter(), schema)
writer.append(DATA_USER)

writer.close()

reader = avro.datafile.DataFileReader(open('result', 'rb'), avro.io.DatumReader())
for user in reader:
	print(type(user['username']))
reader.close()
