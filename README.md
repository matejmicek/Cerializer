# Cerializer
Cerializer is an Avro de/serialization library that aims at providing an even faster alternative to FastAvro and Avro standard library.

This speed increase does not come without a cost. Cerializer will work only with predefined set of schemata for which it will generate tailor made Cython code. This way, the overhead caused by the universality of other serialization libraries will be avoided.

Special credit needs to be given to [FastAvro](https://github.com/fastavro/fastavro) library, by which is this project heavily inspired.

**Demonstration shell scripts**

To see Cerializer in action, we can utilize three ready-made shell scripts.

Prerequisites:
- Linux or MacOS (or Windows with Windows Subsystem for Linux)
- pip installed on the machine
- python3 installed on the machine
- the ROOT directory of the project opened in terminal

**To run a basic demonstration:**

```bash
./demo.sh
```

**Server-Client demonstration**
This demonstration can be performed either with or without a running Confluent Platform. To set up a Confluent Platform, please follow only Step 1 of this quickstart: https://docs.confluent.io/platform/current/quickstart/ce-quickstart.html#step-1-download-and-start-cp. 

If you decide to run the demonstration without Kafka, you will see a client sending messages to a server. These messages will have a special id which is first displayed by the client and then by the server when the messages are received.

If you decide to use Kafka, the procedure will be the same except for the client and the server updating themselves with the schema repository (Kafka). This behaviour is indicated by the script printing "updating with schema repo." There are no schemata in the repository yet, but if you decide to add any by adding a Kafka topic (https://docs.confluent.io/platform/current/quickstart/ce-quickstart.html#step-2-create-ak-topics). Then, to add a schema in the Control Center in the newly created topic, click on schema -> Set a schema -> Optionally create a custom schema -> Create.

The new schema should be now recognised on the next update by the server and the client. Success is indicated by printing:

```
updating with schema repo
finished compiling new_schema:1
```

**To launch a server that will wait for clients messages and use Cerializer to deserialize them (client needs to be started later):**

No Kafka:
```bash
./demo_server.sh
```

With Kafka running (if port was left to default, replace otherwise):
```bash
./demo_server.sh 'http://localhost:8081'
```


**To launch the client:**

No Kafka:
```bash
./demo_client.sh
```

With Kafka running (if port was left to default, replace otherwise):
```bash
./demo_client.sh 'http://localhost:8081'
```

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
This benchmark was executed using the `benchmark.py` script in `cerializer/tests`. Note that the times are normalized into the interval [0, 1].
```
schema, time_cerializer, time_fastavro, time_avro
cerializer.array_int_str            ,0.03717227738867462,0.09904868034296802,1.0
cerializer.enum                     ,0.048077945812758136,0.07441811969170659,1.0
cerializer.str                      ,0.09283684814105748,0.12196546031964381,1.0
cerializer.boolean                  ,0.09427573269822606,0.19705318200832128,1.0
cerializer.time_micros              ,0.09606025976542311,0.1788007017680777,1.0
cerializer.union                    ,0.060184068066858026,0.1813994477900716,1.0
cerializer.reference                ,0.06198405651031086,0.11474401653960228,1.0
cerializer.fixed                    ,0.06252474812476431,0.10319272255475644,1.0
cerializer.int                      ,0.12912923935835008,0.21864098615776942,1.0
cerializer.fixed_decimal            ,0.4118826653983815,0.39155348359636166,1.0
cerializer.array_str                ,0.11198333128456968,0.12303638986216604,1.0
cerializer.decimal                  ,0.22093593577251328,0.5555941832921116,1.0
cerializer.nested                   ,0.04412209825367442,0.1253421778625141,1.0
cerializer.map_str                  ,0.10425944117425574,0.17391576854275986,1.0
cerializer.map_int_null             ,0.05494826231827742,0.10157567561100056,1.0
cerializer.double                   ,0.05018755906953313,0.07678813441659749,1.0
cerializer.bytes                    ,0.06720745759338456,0.10033397964242413,1.0
cerializer.long                     ,0.05210266110360744,0.07486254385198769,1.0
cerializer.date_int                 ,0.09069971820674391,0.18936284577559476,1.0
cerializer.array_bool               ,0.04280888532797152,0.07022321886620599,1.0
cerializer.array_int                ,0.04993196162094609,0.06142152243472576,1.0
```

Measured against Fastavro using the benchmark in Cerializer/tests.

Device: MacBook Pro 13-inch, 2020, 1,4 GHz Quad-Core Intel Core i5, 16 GB 2133 MHz LPDDR3