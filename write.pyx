from typing import Dict
from libc.stdio cimport fopen, fclose, FILE, EOF, fseek, SEEK_END, SEEK_SET, fwrite, fscanf, rewind, sscanf
from libc.stdio cimport ftell, fgetc, fgets, getc, gets, feof, fread, getline, fprintf
from libc.stdlib cimport malloc
from libc.string cimport strlen, memcpy, strcpy, strtok, strchr, strncpy
import os



ctypedef int int32
ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64
cdef union double_ulong64:
    double d
    ulong64 n



cpdef inline write_null(object fo, datum):
    """null is written as zero bytes"""
    pass


cpdef inline write_boolean(bytearray fo, bint datum):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    cdef unsigned char ch_temp[1]
    ch_temp[0] = 1 if datum else 0
    fo += ch_temp[:1]


cpdef inline write_int(bytearray fo, datum):
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


cpdef inline write_long(bytearray fo, datum):
    write_int(fo, datum)


cdef union float_uint32:
    float f
    uint32 n


cpdef inline write_float(bytearray fo, float datum):
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



cpdef inline write_double(bytearray fo, double datum, schema=None):
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


cpdef inline write_bytes(bytearray fo, bytes datum, schema=None):
    """Bytes are encoded as a long followed by that many bytes of data."""
    write_long(fo, len(datum))
    fo += datum


cpdef inline write_string(bytearray fo, datum, schema=None):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data."""
    write_bytes(fo, bytes(datum, 'UTF-8'))



cpdef inline write_fixed(bytearray fo, object datum, schema=None):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    fo += datum


cpdef inline write_enum(bytearray fo, datum, schema):
    """An enum is encoded by a int, representing the zero-based position of
    the symbol in the schema."""
    index = schema['symbols'].index(datum)
    write_int(fo, index)


'''
cdef write_array(bytearray fo, list datum, _type):
    """Arrays are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many array
    items.  A block with count zero indicates the end of the array.  Each item
    is encoded per the array's item schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.  """

    if len(datum) > 0:
        write_long(fo, len(datum))
        for item in datum:
            write_data(fo, item, _type)
    write_long(fo, 0)


cdef write_map(bytearray fo, object datum, dict schema):
    """Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block. The actual
    count in this case is the absolute value of the count written."""
    cdef dict d_datum
    try:
        d_datum = <dict?>(datum)
    except TypeError:
        # Slower, general-purpose code where datum is something besides a dict,
        # e.g. a collections.OrderedDict or collections.defaultdict.
        if len(datum) > 0:
            write_long(fo, len(datum))
            vtype = schema['values']
            for key, val in iteritems(datum):
                write_utf8(fo, key)
                write_data(fo, val, vtype)
        write_long(fo, 0)
    else:
        # Faster, special-purpose code where datum is a Python dict.
        if len(d_datum) > 0:
            write_long(fo, len(d_datum))
            vtype = schema['values']
            for key, val in iteritems(d_datum):
                write_utf8(fo, key)
                write_data(fo, val, vtype)
        write_long(fo, 0)


cdef write_union(bytearray fo, datum, schema):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value. The value
    is then encoded per the indicated schema within the union."""

    cdef int32 best_match_index
    cdef int32 most_fields
    cdef int32 index
    cdef int32 fields
    cdef str schema_name
    if isinstance(datum, tuple):
        (name, datum) = datum
        for index, candidate in enumerate(schema):
            if extract_record_type(candidate) == 'record':
                schema_name = candidate["name"]
            else:
                schema_name = candidate
            if name == schema_name:
                break
        else:
            msg = 'provided union type name %s not found in schema %s' \
                % (name, schema)
            raise ValueError(msg)
    else:
        pytype = type(datum)
        best_match_index = -1
        most_fields = -1
        for index, candidate in enumerate(schema):
            if validate(datum, candidate, raise_errors=False):
                if extract_record_type(candidate) == 'record':
                    candidate_fields = set(
                        f["name"] for f in candidate["fields"]
                    )
                    datum_fields = set(datum)
                    fields = len(candidate_fields.intersection(datum_fields))
                    if fields > most_fields:
                        best_match_index = index
                        most_fields = fields
                else:
                    best_match_index = index
                    break
        if best_match_index < 0:
            msg = '%r (type %s) do not match %s' % (datum, pytype, schema)
            raise ValueError(msg)
        index = best_match_index

    # write data
    write_long(fo, index)
    write_data(fo, datum, schema[index])'''