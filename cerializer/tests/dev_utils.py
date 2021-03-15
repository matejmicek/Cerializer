import fastavro
import cerializer.utils
import yaml
import os



def init_fastavro() -> None:
	for identifier, schema_root in cerializer.utils.iterate_over_schemata():
		fastavro._schema_common.SCHEMA_DEFS[identifier] = cerializer.utils.parse_schema(
			# mypy things yaml has no attribute unsafe_load, which is not true
			yaml.unsafe_load(os.path.join(schema_root, 'schema.yaml')) # type: ignore
		)