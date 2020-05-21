cimport cerializer.write as write
import cerializer.prepare as prepare



ctypedef long long long64
ctypedef unsigned long long ulong64

cpdef serialize(data, output):
    cdef bytearray buffer = bytearray()
    write.write_int(buffer, ('FAIR_PRICE_CHANGE', 'POSITION_CHANGE', 'SNAPSHOT').index(data['event_type']))
    output.write(buffer)