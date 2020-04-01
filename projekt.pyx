from libc.stdio cimport fopen, fclose, FILE, EOF, fseek, SEEK_END, SEEK_SET, fwrite
from libc.stdio cimport ftell, fgetc, fgets, getc, gets, feof, fread, getline
from libc.string cimport strlen, memcpy, strcpy, strtok, strchr, strncpy
import os




cpdef serialize_two_chars(char filename[], int a, char b):
    # serialization
    cdef char printable_bytes[2]
    cdef FILE *serialized
    serialized = fopen(filename, 'wb')
    printable_bytes[0] = a
    printable_bytes[1] = b
    fwrite(printable_bytes, 1, 2, serialized)
    fclose(serialized)



cpdef int[] deserialize_two_chars(char filename[]):
    # deserialization
    cdef FILE *serialized
    serialized = fopen(filename, 'rb')
    cdef char chars[2]
    fread(chars, 1, 2, serialized)
    return chars



cpdef serialize_short_int(char filename[], short int a):
    cdef FILE *serialized
    serialized = fopen(filename, 'wb')
    cdef char printable_bytes[2]
    printable_bytes[0] = (a >> 8) & 0xFF
    printable_bytes[1] = a & 0xFF
    fwrite(printable_bytes, 1, 2, serialized)
    fclose(serialized)


cpdef int deserialize_short_int(char filename[]):
    cdef FILE *serialized
    serialized = fopen(filename, 'rb')
    cdef int deserialized_int
    cdef char file_bytes[2]
    fread(file_bytes, 1, 2, serialized)
    fclose(serialized)
    deserialized_int = (file_bytes[0] << 8) ^ file_bytes[1]
    return deserialized_int
