import datetime
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


MCS_PER_SECOND = 1000000
MCS_PER_MINUTE = MCS_PER_SECOND * 60
MCS_PER_HOUR = MCS_PER_MINUTE * 60

MLS_PER_SECOND = 1000
MLS_PER_MINUTE = MLS_PER_SECOND * 60
MLS_PER_HOUR = MLS_PER_MINUTE * 60

DAYS_SHIFT = datetime.date(1970, 1, 1).toordinal()



def iterate_over_schemata(schema_root):
	for namespace in [f for f in os.listdir(schema_root) if not f.startswith('.')]:
		for schema_name in [f for f in os.listdir(os.path.join(schema_root, namespace)) if not f.startswith('.')]:
			for version in [
				f for f in os.listdir(os.path.join(schema_root, namespace, schema_name)) if not f.startswith('.')
			]:
				yield schema_name, int(version)


MODE_SERIALIZE = 'serialize'
MODE_DESERIALIZE = 'deserialize'