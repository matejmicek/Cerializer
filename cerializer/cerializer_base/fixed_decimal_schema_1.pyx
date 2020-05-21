cimport cerializer.write as write
import cerializer.prepare as prepare



ctypedef long long long64
ctypedef unsigned long long ulong64

cpdef serialize(data, output):
    cdef bytearray buffer = bytearray()
    write.write_fixed(buffer, prepare.prepare_fixed_decimal(data['date'], {'scale': 2, 'size': 16}))
    output.write(buffer)