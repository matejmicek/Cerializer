'''
from typing import Dict
from libc.stdio cimport fopen, fclose, FILE, fseek, SEEK_END, fwrite, fscanf, rewind, sscanf
from libc.stdio cimport ftell, fgets, getc, gets, fread, fprintf
from libc.stdlib cimport malloc
import os



ctypedef long long long64
ctypedef unsigned long long ulong64


def serialize_to_file(char *filename, data, schema):
    cdef FILE *serialized
    serialized = fopen(filename, 'wb')
    cdef long64 length
    cdef bytearray buffer = bytearray()

    for k, _type in schema.items():
        if _type == 'str':
            write_string(buffer, data[k])
        elif _type == 'long':
            write_long(buffer, data[k])

    writing_length = len(buffer)
    cdef char *pr = buffer
    fwrite(pr, writing_length, 1, serialized)
    fclose(serialized)


def deserialize_from_file(char *filename, schema):
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

    data = dict()
    for k, _type in schema.items():
        if _type == 'str':
            data[k] = read_string(read_buffer)
        elif _type == 'long':
            data[k] = read_long(read_buffer)
    return data
'''
