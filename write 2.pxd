#cython: language_level=3
'''
This module deals with writing basic types. 
Complex types are decomposed first in schema_parser.

This code is heavily inspired by FastAvro https://github.com/fastavro/fastavro
'''


ctypedef int int32
ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64
cdef union double_ulong64:
    double d
    ulong64 n



cdef inline write_null(object fo):
    """null is written as zero bytes"""
    pass


cdef inline write_boolean(bytearray fo, bint datum):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    cdef unsigned char ch_temp[1]
    ch_temp[0] = 1 if datum else 0
    fo += ch_temp[:1]


cdef inline write_int(bytearray fo, datum):
    """int and long values are written using variable-length, zig-zag coding.
    """
    cdef ulong64 n
    cdef unsigned char ch_temp[1]
    n = (datum << 1) ^ (datum >> 63)
    while (n & ~0x7F) != 0:
        ch_temp[0] = (n & 0x7f) | 0x80
        fo += ch_temp[:1]
        n >>= 7
    ch_temp[0] = n
    fo += ch_temp[:1]


cdef inline write_long(bytearray fo, datum):
    write_int(fo, datum)


cdef union float_uint32:
    float f
    uint32 n


cdef inline write_float(bytearray fo, float datum):
    """A float is written as 4 bytes.  The float is converted into a 32-bit
    integer using a method equivalent to Java's floatToIntBits and then encoded
    in little-endian format."""
    cdef float_uint32 fi
    cdef unsigned char ch_temp[4]

    fi.f = datum
    ch_temp[0] = fi.n & 0xff
    ch_temp[1] = (fi.n >> 8) & 0xff
    ch_temp[2] = (fi.n >> 16) & 0xff
    ch_temp[3] = (fi.n >> 24) & 0xff

    fo += ch_temp[:4]



cdef inline write_double(bytearray fo, double datum, schema=None):
    """A double is written as 8 bytes.  The double is converted into a 64-bit
    integer using a method equivalent to Java's doubleToLongBits and then
    encoded in little-endian format.  """
    cdef double_ulong64 fi
    cdef unsigned char ch_temp[8]

    fi.d = datum
    ch_temp[0] = fi.n & 0xff
    ch_temp[1] = (fi.n >> 8) & 0xff
    ch_temp[2] = (fi.n >> 16) & 0xff
    ch_temp[3] = (fi.n >> 24) & 0xff
    ch_temp[4] = (fi.n >> 32) & 0xff
    ch_temp[5] = (fi.n >> 40) & 0xff
    ch_temp[6] = (fi.n >> 48) & 0xff
    ch_temp[7] = (fi.n >> 56) & 0xff

    fo += ch_temp[:8]


cdef inline write_bytes(bytearray fo, bytes datum, schema=None):
    """Bytes are encoded as a long followed by that many bytes of data."""
    write_long(fo, len(datum))
    fo += datum


cdef inline write_string(bytearray fo, datum, schema=None):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data."""
    write_bytes(fo, bytes(datum, 'UTF-8'))



cdef inline write_fixed(bytearray fo, object datum, schema=None):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    fo += datum


cdef inline write_enum(bytearray fo, datum, schema):
    """An enum is encoded by a int, representing the zero-based position of
    the symbol in the schema."""
    index = schema['symbols'].index(datum)
    write_int(fo, index)
