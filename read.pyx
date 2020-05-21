from typing import Dict


from libc.stdio cimport fopen, fclose, FILE, EOF, fseek, SEEK_END, SEEK_SET, fwrite, fscanf, rewind, sscanf
from libc.stdio cimport ftell, fgetc, fgets, getc, gets, feof, fread, getline, fprintf
from libc.stdlib cimport malloc
from libc.string cimport strlen, memcpy, strcpy, strtok, strchr, strncpy
import os



cpdef bytes read_buffer(buffer, n):
    cdef char *to_return = <char *>malloc(n)
    x = buffer[:n]
    to_return = x
    temp_buffer = buffer[n:]
    buffer.clear()
    buffer.extend(temp_buffer) # TODO NEEDS TO BE FIXED
    return to_return
