import requests
import cerializer_demo.config as config
import random
import cerializer.cerializer
import cerializer.schemata
import time
from cerializer_demo.config import schema as schema
from cerializer_demo.config import data as data
import sys



def send_message(message):
	x = requests.post(url, data = message)
	return x.text



if __name__ == '__main__':
	url = f'{config.LOCALHOST}:{config.SERVER_PORT}/process'
	kafka = None
	if sys.argv[1]:
		kafka = sys.argv[1]

	schemata = cerializer.schemata.CerializerSchemata(
		schemata = [
			(
				f'{config.NAMESPACE}.{config.SCHEMA_NAME}',
				schema
			)
		],
		schemata_url=kafka,
		verbose=True
	)

	cerializer_instance = cerializer.cerializer.Cerializer(schemata, config.NAMESPACE, config.SCHEMA_NAME)
	while True:
		message_id = random.randint(1, 1000)
		print('sleeping for 1 second')
		time.sleep(1)
		print(f'sending message id = {message_id}')
		data['id'] = message_id
		message = cerializer_instance.serialize(data)
		send_message(message)
