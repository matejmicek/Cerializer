cimport cerializer.write as write
import cerializer.prepare as prepare



ctypedef long long long64
ctypedef unsigned long long ulong64

cpdef serialize(data, output):
    cdef bytearray buffer = bytearray()
    write.write_int(buffer, data['timeseries_identifier'])
    write.write_double(buffer, data['time'])
    write.write_string(buffer, data['security'])
    write.write_int(buffer, data['index'])

    if 'state' not in data:
        write.write_long(buffer, 0)
        write.write_null(buffer)

    elif type(data['state']) is str:
        write.write_long(buffer, 1)
        write.write_string(buffer, data['state'])

    output.write(buffer)