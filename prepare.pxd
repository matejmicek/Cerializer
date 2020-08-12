#cython: language_level=3

cdef mk_bits(bits)

cdef mk_bits(bits)

cdef prepare_timestamp_millis(object data)

cdef prepare_timestamp_micros(object data)

cdef prepare_date(object data)

cdef prepare_bytes_decimal(object data, schema)

cdef prepare_fixed_decimal(object data, schema)

cdef prepare_uuid(object data)

cdef prepare_time_millis(object data)

cdef prepare_time_micros(object data)

cdef prepare_nano_time(data)

cdef prepare_nano_time_delta(data)

cdef read_timestamp_millis(data)

cdef read_timestamp_micros(data)

cdef read_date(data)

cdef read_uuid(data)

cdef read_decimal(data, schema)

cdef read_time_millis(data)

cdef read_time_micros(data)

cdef parse_timestamp(data, resolution)

cdef read_nano_time(data)

cdef read_nano_time_delta(data)
