import os


BOOLEAN = 'boolean'
INT = 'int'
LONG = 'long'
FLOAT = 'float'
DOUBLE = 'double'
BYTES = 'bytes'
STRING = 'string'

RECORD = 'record'
ENUM = 'enum'
ARRAY = 'array'
MAP = 'map'
FIXED = 'fixed'

BASIC_TYPES = {BOOLEAN, INT, LONG, FLOAT, DOUBLE, BYTES, STRING}

COMPLEX_TYPES = {RECORD, ENUM, ARRAY, MAP, FIXED}

PROJECT_ROOT = '/home/development/work/Cerializer'

SCHEMA_ROOTS = [os.path.join(PROJECT_ROOT, 'cerializer', 'tests', 'schemata')]


class SerializationMode:
	MODE_SERIALIZE = 'serialize'
	MODE_DESERIALIZE = 'deserialize'
