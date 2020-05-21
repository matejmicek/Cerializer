cimport cerializer.write as write
import cerializer.prepare as prepare



ctypedef long long long64
ctypedef unsigned long long ulong64

cpdef serialize(data, output):
    cdef bytearray buffer = bytearray()
    write.write_string(buffer, data['imbalance'])
    write.write_string(buffer, data['error'])
    write.write_string(buffer, data['price'])
    output.write(buffer)