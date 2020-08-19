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


**Usage Example:**
1. Create an instance of CerializerSchemata
For initializing CerializerSchemata it is necessary to supply a list of tuples in form of (schema_identifier, schema)
schema touple = (namespace.schema_name, {schema})
 eg.
 ```python
import cerializer.schema_handler
import os
import yaml

def list_schemata():
    # iterates through all your schemata and yields schema_identifier and path to schema folder
    raise NotImplemented

def schemata() -> cerializer.schema_handler.CerializerSchemata:
    schemata = []
	for schema_identifier, schema_root in list_schemata():
		schema_tuple = schema_identifier, yaml.unsafe_load( # type: ignore
			open(os.path.join(schema_root, 'schema.yaml'))
		)
		schemata.append(schema_tuple)
	return cerializer.schema_handler.CerializerSchemata(schemata)
```

2. Create an instance of Cerializer for each of your schemata by calling `cerializer_handler.Cerializer` .
eg. `cerializer_instance = cerializer_handler.Cerializer(cerializer_schemata, schema_namespace, schema_name)`
This will create an instance of Cerializer that can serialize and deserialize data in the particular schema format.

3. Use the instance accordingly.
  eg. 
  ```python
 data_record = {
    'name': 'Matej',
    'age': 22
 }
 cerializer_instance = cerializer.cerializer_handler.Cerializer(cerializer_schemata, 'school', 'student')
 serialized_data = cerializer_instance.serialize(data_record)
 print(serialized_data.getvalue())
```
