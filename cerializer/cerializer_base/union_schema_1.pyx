cimport cerializer.write as write
import cerializer.prepare as prepare



ctypedef long long long64
ctypedef unsigned long long ulong64

cpdef serialize(data, output):
    cdef bytearray buffer = bytearray()
    
    if 'cumulative_profit' not in data:
        write.write_long(buffer, 0)
        write.write_null(buffer)

    elif type(data['cumulative_profit']) is int:
        write.write_long(buffer, 1)
        write.write_int(buffer, data['cumulative_profit'])

    output.write(buffer)