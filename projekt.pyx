from typing import Dict


from libc.stdio cimport fopen, fclose, FILE, EOF, fseek, SEEK_END, SEEK_SET, fwrite, fscanf, rewind, sscanf
from libc.stdio cimport ftell, fgetc, fgets, getc, gets, feof, fread, getline, fprintf
from libc.stdlib cimport malloc
from libc.string cimport strlen, memcpy, strcpy, strtok, strchr, strncpy
import os




ctypedef long long long64
ctypedef unsigned long long ulong64



cpdef void serialize_two_chars(char filename[], char a, char b):
    # serialization
    cdef char printable_bytes[2]
    cdef FILE *serialized
    serialized = fopen(filename, 'wb')
    printable_bytes[0] = a
    printable_bytes[1] = b
    fwrite(printable_bytes, 1, 2, serialized)
    fclose(serialized)



cpdef tuple[int] deserialize_two_chars(char filename[]):
    # deserialization
    cdef FILE *serialized
    serialized = fopen(filename, 'rb')
    cdef char chars[2]
    fread(chars, 1, 2, serialized)
    return chars[0], chars[1]


cpdef void write_string(buffer, s):
    cdef char* byte_string
    encoded_string = s.encode('utf-8')
    byte_string = <bytes>encoded_string
    length = strlen(byte_string)
    write_int(buffer, length)
    buffer += byte_string


cpdef str read_string(buffer):
    cdef long64 string_length = read_long(buffer)
    a = read_buffer(buffer, string_length)
    return a.decode('utf-8')


cpdef bytes read_buffer(buffer, n):
    cdef char *to_return = <char *>malloc(n)
    x = buffer[:n]
    to_return = x
    temp_buffer = buffer[n:]
    buffer.clear()
    buffer.extend(temp_buffer) # TODO NEEDS TO BE FIXED
    return to_return



cdef long64 read_long(bytearray buffer):
    """int and long values are written using variable-length, zig-zag
    coding."""
    cdef ulong64 b
    cdef ulong64 n
    cdef int shift
    cdef bytes c = read_buffer(buffer, 1)

    b = <unsigned char>(c[0])
    n = b & 0x7F
    shift = 7

    while (b & 0x80) != 0:
        c = read_buffer(buffer, 1)
        b = <unsigned char>(c[0])
        n |= (b & 0x7F) << shift
        shift += 7

    return (n >> 1) ^ -(n & 1)


cdef inline write_int(bytearray buffer, datum):
    cdef ulong64 n
    cdef unsigned char ch_temp[1]
    n = (datum << 1) ^ (datum >> 63)
    while (n & ~0x7F) != 0:
        ch_temp[0] = (n & 0x7f) | 0x80
        buffer += ch_temp[:1]
        n >>= 7
    ch_temp[0] = n
    buffer += ch_temp[:1]



schema = {
    'name': 'string',
    'age': 'int'
}



def serialize_to_file(char *filename, data, schema):
    cdef FILE *serialized
    serialized = fopen(filename, 'wb')
    cdef long64 length
    cdef bytearray buffer = bytearray()

    for k, _type in schema.items():
        if _type == 'string':
            write_string(buffer, data[k])
        elif _type == 'int':
            write_int(buffer, data[k])

    writing_length = len(buffer)
    cdef char *pr = buffer
    fwrite(pr, writing_length, 1, serialized)
    fclose(serialized)


def deserialize_from_file(char *filename, data, schema):
    cdef long file_length
    serialized = fopen(filename, 'rb')
    fseek(serialized, 0, SEEK_END)
    file_length = ftell(serialized)
    rewind(serialized)

    cdef char *read_bytes = <char *> malloc(file_length)
    fread(read_bytes, 1, file_length, serialized)
    fclose(serialized)

    cdef read_buffer = bytearray()
    read_buffer.extend(read_bytes) # TODO NEEDS TO BE FIXED

    output = dict()
    for k, _type in schema.items():
        if _type == 'string':
            output[k] = read_string(read_buffer)
        elif _type == 'int':
            output[k] = read_long(read_buffer)
