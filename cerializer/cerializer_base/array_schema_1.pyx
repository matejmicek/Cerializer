cimport cerializer.write as write
import cerializer.prepare as prepare



ctypedef long long long64
ctypedef unsigned long long ulong64

cpdef serialize(data, output):
    cdef bytearray buffer = bytearray()
    write.write_string(buffer, data['order_id'])
    if len(data['round_trip_trade_ids']) > 0:
        write.write_long(buffer, len(data['round_trip_trade_ids']))
        for item in data['round_trip_trade_ids']:
            write.write_string(buffer, item)
    write.write_long(buffer, 0)
    output.write(buffer)