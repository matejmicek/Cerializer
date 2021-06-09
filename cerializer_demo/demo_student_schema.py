from cerializer.schemata import CerializerSchemata
from cerializer.cerializer import Cerializer
from cerializer.code_generator import CodeGenerator
import json
import time
import pprint as p
from cerializer_demo.config import schema as schema
from cerializer_demo.config import data as data



def show_demo():
	print('using student schema:')
	print()
	p.pprint(schema)
	print()
	print()
	time.sleep(2)
	print('initializing a schemata instance:')
	schemata_instance = CerializerSchemata(
		[
			('cerializer.student:1', schema),
		],
		verbose = True
	)
	print('schemata instance initialized')
	time.sleep(2)
	print()
	print('initializing a Cerializer instance')
	instance = Cerializer(
		schemata_instance,
		'cerializer',
		'student:1'
	)
	print('Cerializer instance initialized')
	print()
	time.sleep(2)
	print('generated a code for the example schema:')
	code = CodeGenerator(
		schemata_instance,
		'cerializer.student:1'
	).render_code_with_wraparounds(schema)
	print()
	print(code)
	print()
	print()
	time.sleep(2)
	print('loaded example data:')
	print()
	p.pprint(json.dumps(data))
	cerialized = instance.serialize(data)
	print()
	print('serializing data...')
	print('example data serialized to:')
	print(cerialized)
	time.sleep(2)
	deceri = instance.deserialize(cerialized)
	print()
	print('deserializing data ...')
	print('data deserialized to:')
	print()
	p.pprint(deceri)
	print()
	print('The deserialized data is the same as the original:')
	print(deceri == data)


if __name__ == '__main__':
	show_demo()
