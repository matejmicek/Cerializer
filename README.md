# WIP: Cerializer
Cerializer aims at providing a even faster alternative to FastAvro and Avro standard library.

This speed increase does not come without a cost. Cerializer will work only with predefined set of schemata for which it will generate tailor made code. This way, the overhead caused by the universality of other serialization libraries will be avoided.

Special credit needs to be given to [FastAvro](https://github.com/fastavro/fastavro) library, by which is this project heavily inspired.

**Usage Example:**
1. Put all your schemata into a schemata root folder following this pattern.<br>
`[schemata_root_folder]/[namespace]/[schema_name]/[schema_version]/schema.yaml`<br>
It is essential that you follow this pattern and safe your schemata in `schema.yaml` files.

2. Run `cerializer_handler.update_cerializer` with a list of all your schemata root directories.<br>
eg. `cerializer_handler.update_cerializer(['cerializer/tests/schemata'])`<br>
This will generate an importable module for each schema that contains two methods: `serialize` and `deserialize`.<br>
This module can be then imported as:<br>
 `import [schema_name]_[schema_version]`<br>
 and then used accordingly<br>
3. Use the module accordingly.<br>
  eg.<pre><code>
 import student_schema_1
 import io<br>
 output = io.BytesIO()
 data_recod = {
    'name': 'Matej',
    'age': 22
 }
 student_schema_1.serializer(data_record, output)
 print(output.getvalue())
 </code></pre>
