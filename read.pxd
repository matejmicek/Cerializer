#cython: language_level=3
ctypedef int int32
ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64
cdef union double_ulong64:
	double d
	ulong64 n



cpdef inline read_null(fo):
	"""null is written as zero bytes."""
	return None


cpdef inline read_boolean(fo):
	"""A boolean is written as a single byte whose value is either 0 (false) or
	1 (true).
	"""
	cdef unsigned char ch_temp
	cdef bytes bytes_temp = fo.read(1)
	# technically 0x01 == true and 0x00 == false, but many languages will
	# cast anything other than 0 to True and only 0 to False
	ch_temp = bytes_temp[0]
	return ch_temp != 0


cpdef inline long64 read_long(fo) except? -1:
	return read_int(fo)


cpdef inline long64 read_int(fo) except? -1:
	"""int and long values are written using variable-length, zig-zag
	coding."""
	cdef ulong64 b
	cdef ulong64 n
	cdef int32 shift

	cdef bytes c = fo.read(1)

	# We do EOF checking only here, since most reader start here
	if not c:
		raise StopIteration

	b = <unsigned char>(c[0])
	n = b & 0x7F
	shift = 7

	while (b & 0x80) != 0:
		c = fo.read(1)
		b = <unsigned char>(c[0])
		n |= (b & 0x7F) << shift
		shift += 7

	return (n >> 1) ^ -(n & 1)


cdef union float_uint32:
	float f
	uint32 n


cpdef inline read_float(fo):
	"""A float is written as 4 bytes.

	The float is converted into a 32-bit integer using a method equivalent to
	Java's floatToIntBits and then encoded in little-endian format.
	"""
	cdef bytes data
	cdef unsigned char ch_data[4]
	cdef float_uint32 fi
	data = fo.read(4)
	ch_data[:4] = data
	fi.n = (ch_data[0]
			| (ch_data[1] << 8)
			| (ch_data[2] << 16)
			| (ch_data[3] << 24))
	return fi.f


cdef union double_ulong64:
	double d
	ulong64 n


cpdef inline read_double(fo):
	"""A double is written as 8 bytes.

	The double is converted into a 64-bit integer using a method equivalent to
	Java's doubleToLongBits and then encoded in little-endian format.
	"""
	cdef bytes data
	cdef unsigned char ch_data[8]
	cdef double_ulong64 dl
	data = fo.read(8)
	ch_data[:8] = data
	dl.n = (ch_data[0]
			| (<ulong64>(ch_data[1]) << 8)
			| (<ulong64>(ch_data[2]) << 16)
			| (<ulong64>(ch_data[3]) << 24)
			| (<ulong64>(ch_data[4]) << 32)
			| (<ulong64>(ch_data[5]) << 40)
			| (<ulong64>(ch_data[6]) << 48)
			| (<ulong64>(ch_data[7]) << 56))
	return dl.d



cpdef inline read_bytes(fo):
	"""Bytes are encoded as a long followed by that many bytes of data."""
	cdef long64 size = read_int(fo)
	return fo.read(<long>size)


cpdef inline unicode read_string(fo):
	"""A string is encoded as a long followed by that many bytes of UTF-8
	encoded character data.
	"""
	return read_bytes(fo).decode('utf-8')


cpdef inline read_fixed(fo, writer_schema):
	"""Fixed instances are encoded using the number of bytes declared in the
	schema."""
	return fo.read(writer_schema['size'])
