import datetime



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

WRITE_PREFIX = ''
PREPARE_PREFIX = 'cerializer.'
WRITE_LOCATION = ''

SCHEMATA = [
	('array_schema', 1),
	('union_schema', 1),
	('string_schema', 1),
	('enum_schema', 1),
	('map_schema', 1),
	('fixed_schema', 1),
	('timestamp_schema', 1),
	('timestamp_schema_micros', 1),
	('fixed_decimal_schema', 1),
	('bytes_decimal_schema', 1),
	('int_date_schema', 1),
	('string_uuid_schema', 1),
	('string_uuid_schema', 1),
	('long_time_micros_schema', 1),
	('huge_schema', 1)
]