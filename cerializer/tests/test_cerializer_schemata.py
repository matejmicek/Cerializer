from cerializer.schemata import CerializerSchemata
from cerializer.cerializer import Cerializer


SCHEMA_IDENTIFIER  = 'cerializer.user'
SCHEMA = {
	'name': 'user_id',
	'namespace': 'cerializer',
	'type': 'int'
}

CYCLE_SCHEMA_IDENTIFIER = 'cerializer.cycle'
CYCLE_SCHEMA = {
	'name': 'cycle',
	'type': 'record',
	'namespace': 'cerializer',
	'fields': [
		{
			'type': 'cycle',
			'name': 'cycle starting node'
		},
		{
			'type': 'int',
			'name': 'pain_int'
		},

	]
}



def test_schemata_add():
	'''
	tests the addition of a new schema after schemata instance init
	'''
	schemata = CerializerSchemata([])
	schemata.add_schema(SCHEMA_IDENTIFIER, SCHEMA)
	assert SCHEMA_IDENTIFIER in schemata
	# we do not yet have code for the schema
	assert SCHEMA_IDENTIFIER not in schemata.get_known_schemata()


def test_schemata_init():
	'''
	tests the addition of a new schema on schemata instance init
	'''
	schemata = CerializerSchemata([(SCHEMA_IDENTIFIER, SCHEMA)])
	assert SCHEMA_IDENTIFIER in schemata
	# we already have code for the schema
	assert SCHEMA_IDENTIFIER in schemata.get_known_schemata()


def test_cycles():
	'''
	tests whether the cycles are properly detected
	'''
	schemata = CerializerSchemata([(CYCLE_SCHEMA_IDENTIFIER, CYCLE_SCHEMA)])
	assert schemata.is_cycle_starting(CYCLE_SCHEMA_IDENTIFIER)
	assert not schemata.is_cycle_starting('cerializer.plain_int')


def test_load_schema():
	'''
	tests whether loading a schema returns it in parsed form
	'''
	schemata = CerializerSchemata([(SCHEMA_IDENTIFIER, SCHEMA)])
	# we store only parsed schemata without reserved properties
	assert schemata.load_schema(SCHEMA_IDENTIFIER) == {'type': 'int'}


def test_load_schema_and_init():
	'''
	tests whether loading a schema returns it in parsed form when on init
	'''
	schemata1 = CerializerSchemata([])
	schemata1.add_schema(SCHEMA_IDENTIFIER, SCHEMA)

	schemata2 = CerializerSchemata([(SCHEMA_IDENTIFIER, SCHEMA)])
	assert schemata1.load_schema(SCHEMA_IDENTIFIER) == schemata2.load_schema(SCHEMA_IDENTIFIER)


def test_add_code():
	'''
	tests whether added code will cause the schemata instance to react properly
	'''
	schemata = CerializerSchemata([])
	SERIALIZED = b''
	DESERIALIZED = 6
	def dummy_code():
		def serialize(data, output):
			output.write(SERIALIZED)
		def deserialize(data):
			return 6
		return locals()
	schemata.add_code(SCHEMA_IDENTIFIER, dummy_code())
	schema_name = SCHEMA_IDENTIFIER.split('.')[1]
	schema_namespace = SCHEMA_IDENTIFIER.split('.')[0]
	cerializer_instance = Cerializer(schemata, schema_namespace, schema_name)
	assert cerializer_instance.serialize({}) == SERIALIZED
	assert cerializer_instance.deserialize(b'') == DESERIALIZED
