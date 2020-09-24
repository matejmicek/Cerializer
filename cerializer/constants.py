import enum
import importlib.util
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

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')


SCHEMA_ROOTS = [os.path.join(PROJECT_ROOT, 'cerializer', 'tests', 'schemata')]


class SerializationMode(enum.Enum):
	MODE_SERIALIZE = 'serialize'
	MODE_DESERIALIZE = 'deserialize'


QUANTLANE = False

package_name = 'qutils'
spec = importlib.util.find_spec(package_name)
if spec:
	# there is no way to know whether the user used the extra 'quantlane'
	# other than searching for the packages
	QUANTLANE = True
