cimport cerializer.write as write
import cerializer.prepare as prepare



ctypedef long long long64
ctypedef unsigned long long ulong64

cpdef serialize(data, output):
    cdef bytearray buffer = bytearray()
    cdef dict d_datum
    datum = data['meta']
    try:
        d_datum = <dict?>(datum)
    except TypeError:
        if len(datum) > 0:
            write.write_long(buffer, len(datum))
            for key, val in datum.items():
                write.write_string(buffer, key)
                
                if key not in data['meta']:
                    write.write_long(buffer, 0)
                    write.write_null(buffer)
                
                elif type(data['meta'][key]) is bool:
                    write.write_long(buffer, 1)
                    write.write_boolean(buffer, val)
                
                elif type(data['meta'][key]) is int:
                    write.write_long(buffer, 2)
                    write.write_int(buffer, val)
                
                elif type(data['meta'][key]) is int:
                    write.write_long(buffer, 3)
                    write.write_long(buffer, val)
                
                elif type(data['meta'][key]) is float:
                    write.write_long(buffer, 4)
                    write.write_float(buffer, val)
                
                elif type(data['meta'][key]) is float:
                    write.write_long(buffer, 5)
                    write.write_double(buffer, val)
                
                elif type(data['meta'][key]) is bytes:
                    write.write_long(buffer, 6)
                    write.write_bytes(buffer, val)
                
                elif type(data['meta'][key]) is str:
                    write.write_long(buffer, 7)
                    write.write_string(buffer, val)
                
        write.write_long(buffer, 0)
    else:
        # Faster, special-purpose code where datum is a Python dict.
        if len(d_datum) > 0:
            write.write_long(buffer, len(d_datum))
            for key, val in d_datum.items():
                write.write_string(buffer, key)
                
                if key not in data['meta']:
                    write.write_long(buffer, 0)
                    write.write_null(buffer)
                
                elif type(data['meta'][key]) is bool:
                    write.write_long(buffer, 1)
                    write.write_boolean(buffer, val)
                
                elif type(data['meta'][key]) is int:
                    write.write_long(buffer, 2)
                    write.write_int(buffer, val)
                
                elif type(data['meta'][key]) is int:
                    write.write_long(buffer, 3)
                    write.write_long(buffer, val)
                
                elif type(data['meta'][key]) is float:
                    write.write_long(buffer, 4)
                    write.write_float(buffer, val)
                
                elif type(data['meta'][key]) is float:
                    write.write_long(buffer, 5)
                    write.write_double(buffer, val)
                
                elif type(data['meta'][key]) is bytes:
                    write.write_long(buffer, 6)
                    write.write_bytes(buffer, val)
                
                elif type(data['meta'][key]) is str:
                    write.write_long(buffer, 7)
                    write.write_string(buffer, val)
                
        write.write_long(buffer, 0)
    output.write(buffer)