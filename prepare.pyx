#cython: language_level=3
import datetime
import decimal
import os
import uuid
import cerializer.constants

from cpython.int cimport PyInt_AS_LONG
from cpython.tuple cimport PyTuple_GET_ITEM
from libc.time cimport mktime, tm
from pytz import utc


if cerializer.constants.QUANTLANE:
	import qutils.time.nanotime



# Mostly taken from fastavro version 0.22.6
# Original file: https://github.com/fastavro/fastavro/blob/master/fastavro/_logical_writers.pyx


'''
This module deals with preparation of logical types.
It is only used from within Cerializer.
'''



ctypedef long long long64

cdef is_windows = os.name == 'nt'
cdef has_timestamp_fn = hasattr(datetime.datetime, 'timestamp')

cdef long64 MCS_PER_SECOND = 1000000
cdef long64 MCS_PER_MINUTE = 60000000
cdef long64 MCS_PER_HOUR = 3600000000

cdef long64 MLS_PER_SECOND = 1000
cdef long64 MLS_PER_MINUTE = 60000
cdef long64 MLS_PER_HOUR = 3600000


epoch = datetime.datetime(1970, 1, 1, tzinfo=utc)
epoch_naive = datetime.datetime(1970, 1, 1)
DAYS_SHIFT = datetime.date(1970, 1, 1).toordinal()



cdef inline mk_bits(bits):
	return bytes([bits & 0xff])


cdef inline prepare_timestamp_millis(object data):
	cdef object tt
	cdef tm time_tuple
	if isinstance(data, datetime.datetime):
		if not has_timestamp_fn:
			if data.tzinfo is not None:
				return <long64>(<double>(
					<object>(data - epoch).total_seconds()) * MLS_PER_SECOND
				)
			tt = data.timetuple()
			time_tuple.tm_sec = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 5)))
			time_tuple.tm_min = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 4)))
			time_tuple.tm_hour = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 3)))
			time_tuple.tm_mday = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 2)))
			time_tuple.tm_mon = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 1))) - 1
			time_tuple.tm_year = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 0))) - 1900
			time_tuple.tm_isdst = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 8)))
			return mktime(& time_tuple) * MLS_PER_SECOND + <long64>(
				int(data.microsecond) / 1000)
		else:
			# On Windows, timestamps before the epoch will raise an error.
			# See https://bugs.python.org/issue36439
			if is_windows:
				if data.tzinfo is not None:
					return <long64>(<double>(
						<object>(data - epoch).total_seconds()) * MLS_PER_SECOND
					)
				else:
					return <long64>(<double>(
						<object>(data - epoch_naive).total_seconds()) * MLS_PER_SECOND
					)
			else:
				x =  <long64>(<double>(data.timestamp()) * MLS_PER_SECOND)
				return x
	else:
		return data


cdef inline prepare_timestamp_micros(object data):
	cdef object tt
	cdef tm time_tuple
	if isinstance(data, datetime.datetime):
		if not has_timestamp_fn:
			if data.tzinfo is not None:
				return <long64>(<double>(
					<object>(data - epoch).total_seconds()) * MCS_PER_SECOND
				)
			tt = data.timetuple()
			time_tuple.tm_sec = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 5)))
			time_tuple.tm_min = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 4)))
			time_tuple.tm_hour = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 3)))
			time_tuple.tm_mday = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 2)))
			time_tuple.tm_mon = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 1))) - 1
			time_tuple.tm_year = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 0))) - 1900
			time_tuple.tm_isdst = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 8)))

			return mktime(& time_tuple) * MCS_PER_SECOND + \
				<long64>(data.microsecond)
		else:
			# On Windows, timestamps before the epoch will raise an error.
			# See https://bugs.python.org/issue36439
			if is_windows:
				if data.tzinfo is not None:
					return <long64>(<double>(
						<object>(data - epoch).total_seconds()) * MCS_PER_SECOND
					)
				else:
					return <long64>(<double>(
						<object>(data - epoch_naive).total_seconds()) * MCS_PER_SECOND
					)
			else:
				return <long64>(<double>(data.timestamp()) * MCS_PER_SECOND)
	else:
		return data


cdef inline prepare_date(object data):
	if isinstance(data, datetime.date):
		return data.toordinal() - DAYS_SHIFT
	elif isinstance(data, str):
		return datetime.datetime.strptime(data, "%Y-%m-%d").toordinal() - DAYS_SHIFT
	else:
		return data


cdef inline prepare_bytes_decimal(object data, schema):
	"""Convert decimal.Decimal to bytes"""
	if not isinstance(data, decimal.Decimal):
		return data
	scale = schema.get('scale', 0)

	sign, digits, exp = data.as_tuple()

	delta = exp + scale

	if delta < 0:
		raise ValueError(
			'Scale provided in schema does not match the decimal')

	unscaled_datum = 0
	for digit in digits:
		unscaled_datum = (unscaled_datum * 10) + digit

	unscaled_datum = 10 ** delta * unscaled_datum

	bytes_req = (unscaled_datum.bit_length() + 8) // 8

	if sign:
		unscaled_datum = -unscaled_datum

	return unscaled_datum.to_bytes(bytes_req, byteorder='big', signed=True)



cdef inline prepare_fixed_decimal(object data, schema):
	cdef bytearray tmp
	if not isinstance(data, decimal.Decimal):
		return data
	scale = schema.get('scale', 0)
	size = schema['size']

	# based on https://github.com/apache/avro/pull/82/

	sign, digits, exp = data.as_tuple()

	if -exp > scale:
		raise ValueError(
			'Scale provided in schema does not match the decimal')
	delta = exp + scale
	if delta > 0:
		digits = digits + (0,) * delta

	unscaled_datum = 0
	for digit in digits:
		unscaled_datum = (unscaled_datum * 10) + digit

	bits_req = unscaled_datum.bit_length() + 1

	size_in_bits = size * 8
	offset_bits = size_in_bits - bits_req

	mask = 2 ** size_in_bits - 1
	bit = 1
	for i in range(bits_req):
		mask ^= bit
		bit <<= 1

	if bits_req < 8:
		bytes_req = 1
	else:
		bytes_req = bits_req // 8
		if bits_req % 8 != 0:
			bytes_req += 1

	tmp = bytearray()

	if sign:
		unscaled_datum = (1 << bits_req) - unscaled_datum
		unscaled_datum = mask | unscaled_datum
		for index in range(size - 1, -1, -1):
			bits_to_write = unscaled_datum >> (8 * index)
			tmp += mk_bits(bits_to_write & 0xff)
	else:
		for i in range(offset_bits // 8):
			tmp += mk_bits(0)
		for index in range(bytes_req - 1, -1, -1):
			bits_to_write = unscaled_datum >> (8 * index)
			tmp += mk_bits(bits_to_write & 0xff)

	return tmp


cdef inline prepare_uuid(object data):
	if isinstance(data, uuid.UUID):
		return str(data)
	else:
		return data



cdef inline prepare_time_millis(object data):
	if isinstance(data, datetime.time):
		return int(
			data.hour * MLS_PER_HOUR + data.minute * MLS_PER_MINUTE
			+ data.second * MLS_PER_SECOND + int(data.microsecond / 1000))
	else:
		return data



cdef inline prepare_time_micros(object data):
	if isinstance(data, datetime.time):
		return int(data.hour * MCS_PER_HOUR + data.minute * MCS_PER_MINUTE
					+ data.second * MCS_PER_SECOND + data.microsecond)
	else:
		return data



cdef prepare_nano_time(data):
	return data.nanoseconds



cdef prepare_nano_time_delta(data):
	return data.nanoseconds



cdef inline read_timestamp_millis(data):
	return parse_timestamp(data, float(1000))

cdef inline read_timestamp_micros(data):
	return parse_timestamp(data, float(1000000))

cdef inline read_date(data):
	return datetime.date.fromordinal(data + datetime.date(1970, 1, 1).toordinal())

cdef inline read_uuid(data):
	return uuid.UUID(data)

cdef inline read_decimal(data, schema):
	scale = schema.get('scale', 0)
	precision = schema['precision']
	unscaled_datum = int.from_bytes(data, byteorder = 'big', signed = True)
	decimal_context = decimal.Context()
	decimal_context.prec = precision
	return decimal_context.create_decimal(unscaled_datum). \
		scaleb(-scale, decimal_context)

cdef inline read_time_millis(data):
	h = int(data / 60 * 60 * 1000)
	m = int(data / 60 * 1000) % 60
	s = int(data / 1000) % 60
	mls = int(data % 1000) * 1000
	return datetime.time(h, m, s, mls)

cdef inline read_time_micros(data):
	h = int(data / (60 * 60 * 1000000))
	m = int(data / (60 * 1000000)) % 60
	s = int(data / 1000000) % 60
	mcs = data % 1000000
	return datetime.time(h, m, s, mcs)

cdef inline parse_timestamp(data, resolution):
	return datetime.datetime(1970, 1, 1, tzinfo = utc) + datetime.timedelta(seconds = data / resolution)

cdef read_nano_time(data):
	return qutils.time.nanotime.NanoTime(data)

cdef read_nano_time_delta(data):
	return qutils.time.NanoTimeDelta(data)
