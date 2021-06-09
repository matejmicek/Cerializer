import sys

from bottle import run, post, request
import cerializer_demo.config
import os
import cerializer.schemata
import cerializer.cerializer
from cerializer_demo.config import schema
import cerializer_demo.config as config



if __name__ == '__main__':
	kafka = None
	if sys.argv[1]:
		kafka = sys.argv[1]
	schemata = cerializer.schemata.CerializerSchemata(
		schemata = [
			('school.student_schema:1', schema)
		],
		schemata_url = kafka,
		verbose = True
	)

	cerializer_instance = cerializer.cerializer.Cerializer(schemata, config.NAMESPACE, config.SCHEMA_NAME)

	@post('/process')
	def my_process():
		req_obj = cerializer_instance.deserialize(request.body.read())
		print(f'received message id = {req_obj["id"]}')
	os.system('clear')
	print('server running')
	run(host='localhost', port = cerializer_demo.config.SERVER_PORT, debug=True)
