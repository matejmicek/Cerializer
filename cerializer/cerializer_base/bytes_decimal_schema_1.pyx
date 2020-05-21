cimport cerializer.write as write
import cerializer.prepare as prepare



ctypedef long long long64
ctypedef unsigned long long ulong64

cpdef serialize(data, output):
    cdef bytearray buffer = bytearray()
    write.write_bytes(buffer, prepare.prepare_bytes_decimal(data['date'], {'scale': 2, 'size': 0}))
    output.write(buffer)