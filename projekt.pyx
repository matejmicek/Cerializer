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


cpdef serialize_string(data):
    return data


cdef long64 read_long(fo,
                      writer_schema=None,
                      reader_schema=None) except? -1:
    """int and long values are written using variable-length, zig-zag
    coding."""
    cdef ulong64 b
    cdef ulong64 n
    cdef int shift
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


cdef inline write_int(bytearray fo, datum, schema=None):
    cdef ulong64 n
    cdef unsigned char ch_temp[1]
    n = (datum << 1) ^ (datum >> 63)
    while (n & ~0x7F) != 0:
        ch_temp[0] = (n & 0x7f) | 0x80
        fo += ch_temp[:1]
        n >>= 7
    ch_temp[0] = n

    print('ch temp ', ch_temp[:1])

    fo += ch_temp[:1]

    print('fo', fo)



def do_full_circle(char *filename, data):
    cdef FILE *serialized
    serialized = fopen(filename, 'wb')
    cdef char* serialized_string
    s1 = data['s'].encode('utf-8')
    serialized_string = <bytes>s1
    print(serialized_string)
    cdef long64 length
    length = strlen(serialized_string)

    print(length)

    cdef bytearray buffer
    buffer = bytearray()

    write_int(buffer, length)
    print('writing buffer: ', buffer)

    writing_length = len(buffer)

    cdef char *pr
    pr = buffer
    print('pr ', pr, writing_length)
    fwrite(pr, writing_length, 1, serialized)
    fclose(serialized)



    cdef long filelen
    serialized = fopen(filename, 'rb')
    fseek(serialized, 0, SEEK_END)
    filelen = ftell(serialized)
    print('file length ', filelen)
    rewind(serialized)

    cdef char *read_bytes = <char *> malloc(filelen * sizeof(char))
    print('reading')
    fread(read_bytes, 1, filelen, serialized)
    fclose(serialized)
    print('read bytes ', read_bytes)

    cdef long64 file_long = read_long(read_bytes)
    print('long ', file_long)

    '''




    
    fclose(serialized)

    cdef long filelen
    serialized = fopen(filename, "rb")
    fseek(serialized, 0, SEEK_END)
    filelen = ftell(serialized)
    print('file length ', filelen)
    rewind(serialized)

    cdef char buffer[100]
    cdef char *deserialized
    fscanf(serialized, '%s', buffer)
    print('buffer', buffer)
    print(len(buffer))
    fclose(serialized)
    print(deserialized)'''

