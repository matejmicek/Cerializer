# WIP: Cerializer
Cerializer is an Avro de/serialization library that aims at providing an even faster alternative to FastAvro and Avro standard library.

This speed increase does not come without a cost. Cerializer will work only with predefined set of schemata for which it will generate tailor made Cython code. This way, the overhead caused by the universality of other serialization libraries will be avoided.

Special credit needs to be given to [FastAvro](https://github.com/fastavro/fastavro) library, by which is this project heavily inspired.

**Example of a schema and the corresponding code**

SCHEMA
```
{
    'name': 'array_schema', 
    'doc': 'Array schema', 
    'namespace': 'cerializer', 
    'type': 'record', 
    'fields': [
        {
            'name': 'order_id', 
            'doc': 'Id of order', 
            'type': 'string'
        }, 
        {
            'name': 'trades', 
            'type': {
                'type': 'array', 
                'items': ['string', 'int']
            }
        }
    ]
}
```

CORRESPONDING CODE
```
def serialize(data, output):
    cdef bytearray buffer = bytearray()
    cdef dict datum
    cdef str type_0
    write.write_string(buffer, data['order_id'])
    if len(data['trades']) > 0:
        write.write_long(buffer, len(data['trades']))
        for val_0 in data['trades']:
            if type(val_0) is tuple:
                type_0, val_1 = val_0
                
                if type_0 == 'string':
                    write.write_long(buffer, 0)
                    write.write_string(buffer, val_1)
                
                elif type_0 == 'int':
                    write.write_long(buffer, 1)
                    write.write_int(buffer, val_1)
                
            else:
                if type(val_0) is str:
                    write.write_long(buffer, 0)
                    write.write_string(buffer, val_0)
                elif type(val_0) is int:
                    write.write_long(buffer, 1)
                    write.write_int(buffer, val_0)
    write.write_long(buffer, 0)
    output.write(buffer)



def deserialize(fo):
    cdef long long i_0
    cdef long long i_1
    cdef long i_2
    data = {}
    data['order_id'] = read.read_string(fo)
    data['trades'] = []

    i_1 = read.read_long(fo)
    while i_1 != 0:
        if i_1 < 0:
            i_1 = -i_1
            read.read_long(fo)
        for i_0 in range(i_1):
            i_2 = read.read_int(fo)
            if i_2 == 0:
                val_2 = read.read_string(fo)
            if i_2 == 1:
                val_2 = read.read_int(fo)
            data['trades'].append(val_2)
        i_1 = read.read_long(fo)
    return data
```

This Cython code is later compiled into C and imported.


**Usage Example:**
1. Create an instance of CerializerSchemata
For initializing CerializerSchemata it is necessary to supply a list of tuples in form of (schema_identifier, schema) 
where schema_identifier is a string and schema is a dict representing the Avro schema and/or an url to Kafka Schema Repository.
schema tuple = (namespace.schema_name, schema).
It is also highly recommended to reuse this schemata instance since on init, it compiles all the schemata which is usually computationally expensive.
eg.
    ```python
    import cerializer.schemata
    
    
    
    KAFKA_URL = '...'
    
    def get_schemata_from_local_repo():
        # iterates through all your schemata and return a list of 
        # (schema_identifier, schema) tuples
        raise NotImplemented
    
    cerializer_schemata = cerializer.schemata.CerializerSchemata(
        get_schemata_from_local_repo(), 
        KAFKA_URL
    )
    ```

1. Create an instance of Cerializer for each of your schemata by calling `cerializer.Cerializer` .
eg. `cerializer_instance = cerializer.Cerializer(cerializer_schemata, schema_namespace, schema_name)`
This will create an instance of Cerializer that can serialize and deserialize data in the particular schema format.

3. Use the instance accordingly.
  eg. 
    ```python
    data_record = {
        'order_id': 'aaaa', 
        'trades': [123, 456, 765]
    }
    
     cerializer_instance = cerializer.cerializer.Cerializer(cerializer_schemata, 'school', 'student')
     serialized_data = cerializer_instance.serialize(data_record)
     print(serialized_data)
    
    deserialized_data = cerializer_instance.deserialize(serialized_data)
    print(deserialized_data)
    ```

    Output
    ```python
    b'\x08aaaa\x06\x02\xf6\x01\x02\x90\x07\x02\xfa\x0b\x00'
    
   {
        'order_id': 'aaaa', 
        'trades': [123, 456, 765]
    }
    ```

**Benchmark** 
```
cerializer.huge_schema:1               3.6394 times faster,   1.4231s : 0.3910s
cerializer.timestamp_schema_micros:1   1.7470 times faster,   0.2759s : 0.1579s
cerializer.default_schema:1            1.4456 times faster,   0.2392s : 0.1655s
cerializer.fixed_schema:1              1.6654 times faster,   0.0980s : 0.0589s
cerializer.map_schema:2                1.6411 times faster,   0.2103s : 0.1281s
cerializer.enum_schema:1               1.5498 times faster,   0.1079s : 0.0696s
cerializer.array_schema:2              1.3455 times faster,   4.9207s : 3.6572s
cerializer.array_schema:3              1.3385 times faster,   2.3153s : 1.7297s
cerializer.array_schema:4              4.2781 times faster,   12.3307s : 2.8823s
cerializer.timestamp_schema:1          1.6614 times faster,   0.2454s : 0.1477s
cerializer.default_schema:3            1.5040 times faster,   0.1191s : 0.0792s
cerializer.tree_schema:1               4.1126 times faster,   0.6619s : 0.1609s
cerializer.default_schema:2            1.5767 times faster,   0.1261s : 0.0800s
cerializer.string_schema:1             1.5083 times faster,   0.2119s : 0.1405s
cerializer.plain_int:1                 1.8953 times faster,   0.1286s : 0.0678s
cerializer.union_schema:1              1.9114 times faster,   0.1755s : 0.0918s
cerializer.fixed_decimal_schema:1      1.2929 times faster,   2.2791s : 1.7627s
cerializer.int_date_schema:1           2.3736 times faster,   0.1702s : 0.0717s
cerializer.reference_schema:1          2.1570 times faster,   0.9120s : 0.4228s
cerializer.bytes_decimal_schema:1      1.6603 times faster,   0.3401s : 0.2049s
cerializer.string_uuid_schema:1        1.5366 times faster,   0.3236s : 0.2106s
cerializer.map_schema:1                4.1051 times faster,   0.5873s : 0.1431s
cerializer.long_time_micros_schema:1   1.9305 times faster,   0.2250s : 0.1166s
cerializer.array_schema:1              1.4784 times faster,   0.1987s : 0.1344s
AVERAGE: 2.1894 times faster
```

Measured against Fastavro using the benchmark in Cerializer/tests.

Device: MacBook Pro 13-inch, 2020, 1,4 GHz Quad-Core Intel Core i5, 16 GB 2133 MHz LPDDR3