# WIP: Cerializer
Cerializer is an Avro de/serialization library that aims at providing a even faster alternative to FastAvro and Avro standard library.

This speed increase does not come without a cost. Cerializer will work only with predefined set of schemata for which it will generate tailor made Cython code. This way, the overhead caused by the universality of other serialization libraries will be avoided.

Special credit needs to be given to [FastAvro](https://github.com/fastavro/fastavro) library, by which is this project heavily inspired.

**Example of a schema and the corresponding code**

SCHEMA
```
{
    'name': 'array_schema', 
    'doc': 'Array schema', 
    'namespace': 'messaging', 
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
    cdef str type_51

    write.write_string(buffer, data['order_id'])

    if len(data['trades']) > 0:
        write.write_long(buffer, len(data['trades']))
        for val_59 in data['trades']:
            if type(val_59) is tuple:
                type_51, val_60 = val_59
                
                if type_51 == 'string':
                    write.write_long(buffer, 0)
                    write.write_string(buffer, val_60)
                
                elif type_51 == 'int':
                    write.write_long(buffer, 1)
                    write.write_int(buffer, val_60)
                

            else:

                if type(val_59) is str:
                    write.write_long(buffer, 0)
                    write.write_string(buffer, val_59)

                elif type(val_59) is int:
                    write.write_long(buffer, 1)
                    write.write_int(buffer, val_59)

    write.write_long(buffer, 0)
    output.write(buffer)



def deserialize(fo):
    cdef long long i_62
    cdef long long i_63
    cdef long i_64
    data = {}
    data['order_id'] = read.read_string(fo)
    data['trades'] = []

    i_63 = read.read_long(fo)
    while i_63 != 0:
        if i_63 < 0:
            i_63 = -i_63
            read.read_long(fo)
        for i_62 in range(i_63):
            i_64 = read.read_int(fo)

            if i_64 == 0:
                val_61 = read.read_string(fo)


            if i_64 == 1:
                val_61 = read.read_int(fo)


            data['trades'].append(val_61)
        i_63 = read.read_long(fo)
    return data
```


**Usage Example:**
1. Put all your schemata into a schemata root folder following this pattern.
```
[schemata_root_folder]/[namespace]/[schema_name]/[schema_version]/schema.yaml
```
It is essential that you follow this pattern and safe your schemata in `schema.yaml` files.

2. Create an instance of Cerializer by calling `cerializer_handler.Cerializer` with a list of all your schemata root directories.
eg. `cerializer_handler.Cerializer(['cerializer/tests/schemata'])`
This will create an instance of Cerializer that can serialize and deserialize data in all schema formats.

3. Use the instance accordingly.
  eg. 
  ```python
 import cerializer.cerializer_handler
 import io
 output = io.BytesIO()
 data_record = {
    'name': 'Matej',
    'age': 22
 }
 cerializer_instance = cerializer.cerializer_handler.Cerializer(['schemata'])
 cerializer_instance.serialize('namespace', 'schema_name', 'schema_version', data_record, output)
 print(output.getvalue())
```
