from libc.stdio cimport fopen, fclose, FILE, EOF, fseek, SEEK_END, SEEK_SET, fwrite
from libc.stdio cimport ftell, fgetc, fgets, getc, gets, feof, fread, getline
from libc.string cimport strlen, memcpy, strcpy, strtok, strchr, strncpy
import os



cpdef serialize_two_chars(int a, char b):
    # serialization
    cdef char n[2]
    cdef FILE *serialized
    serialized = fopen('ahoj', 'wb')
    n[0] = a
    n[1] = b
    fwrite(n, 1, 2, serialized)
    fclose(serialized)



cpdef deserialize_two_chars():
    # deserialization
    cdef FILE *fp
    fp = fopen('ahoj', 'rb')
    cdef char p[2]
    fread(p, 1, 2, fp)
    print(p[0], p[1])
