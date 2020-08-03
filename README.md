# WIP: Cerializer
Cerializer is an Avro de/serialization library that aims at providing a even faster alternative to FastAvro and Avro standard library.

This speed increase does not come without a cost. Cerializer will work only with predefined set of schemata for which it will generate tailor made code. This way, the overhead caused by the universality of other serialization libraries will be avoided.

Special credit needs to be given to [FastAvro](https://github.com/fastavro/fastavro) library, by which is this project heavily inspired.

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
 import cerializer_handler
 import io
 output = io.BytesIO()
 data_record = {
    'name': 'Matej',
    'age': 22
 }
 cerializer = cerializer_handler.Cerializer(['schemata'])
 cerializer.serialize('namespace', 'schema_name', 'schema_version', data_record, output)
 print(output.getvalue())
```
