import cerializer.quantlane_utils
import cerializer.quantlane_cerializer
import os
import yaml
import schemachinery.codec.avro_schemata
import schemachinery.codec.avro_codec
import logging
import cerializer.tests.test_cerializer_compatibility
import distutils.dir_util
import tempfile



SCHEMA_ROOT1 = '/home/development/schemata'
SCHEMA_ROOT2 = '/home/development/work/Cerializer/cerializer/tests/schemata'
SCHEMA_ROOT_COMMON = '/home/development/work/schemachinery/schemachinery/schemata'

THE_ONE = '/home/development/work/the-one/the_one/schemata'
SCHEMACHINERY = '/home/development/work/schemachinery/schemachinery/schemata'

HELPER = [SCHEMACHINERY]

SCHEMA_ROOTS = [SCHEMA_ROOT1]

def test_file_compilation():
	# patch for not working avro codec
	temp_schema_roots = []
	for schema_root in SCHEMA_ROOTS:
		temp_schemata = tempfile.gettempdir()
		distutils.dir_util.copy_tree(schema_root, temp_schemata)
		temp_schema_roots.append(temp_schemata)
	cerializer.tests.test_cerializer_compatibility.init_fastavro(temp_schema_roots + HELPER)
	cerializer.quantlane_utils.add_compiled_cerializer_code(temp_schema_roots, helper_schema_roots = HELPER)
	for schema_root, namespace, name, version in cerializer.quantlane_utils.iterate_over_schemata(temp_schema_roots):
		try:
			cerializer_instance = cerializer.quantlane_cerializer.CerializerQuantlaneCodec(temp_schema_roots, namespace, name, version)
			path = os.path.join(schema_root, namespace, name, str(version))
			data_all = yaml.unsafe_load_all(open(os.path.join(path, 'example.yaml')))
			avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(*temp_schema_roots)
			codec = schemachinery.codec.avro_codec.AvroCodec(avro_schemata, namespace, name, version)
			for data in data_all:
				encoded = codec.encode(data)
				encoded_cerializer = cerializer_instance.encode(data)
				assert encoded == encoded_cerializer
				decoded = codec.decode(encoded)
				decoded_cerializer = cerializer_instance.decode(encoded)
				assert decoded == decoded_cerializer
		except FileNotFoundError:
			logging.warning(
				'Missing schema or Example file for schema == %s and version == %s',
				name,
				version,
			)
			assert False
