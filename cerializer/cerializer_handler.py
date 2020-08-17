from typing import Any, Dict

import io

import cerializer.compiler
import cerializer.schema_handler
import cerializer.utils


class Cerializer:
	'''
	The main entry point of Cerializer project.
	TODO Change when rendering on startup
	A Cerializer instance renders and then compiles code for schemata.
	It then provides two methods - serialize and deserialize.
	These methods are then called by the user to de/serialize data.
	'''

	def __init__(
		self,
		cerializer_schemata: cerializer.schema_handler.CerializerSchemata,
		namespace: str,
		schema_name: str,
	) -> None:
		'''
		One can initialize Cerializer with a list of schema_identifier, schema or with a list of schema roots.
		For schema roots usage checkout README.
		'''
		self.code_generator = cerializer.schema_handler.CodeGenerator(
			cerializer_schemata,
			cerializer.utils.get_schema_identifier(namespace, schema_name),
		)
		self.schema_identifier = cerializer.utils.get_schema_identifier(namespace, schema_name)
		self.cerializer_schemata = cerializer_schemata
		compiled_code = self._get_compiled_code(
			self.cerializer_schemata.load_schema(self.schema_identifier, self.schema_identifier)
		)
		self.serialization_function = compiled_code['serialize']
		self.deserialization_function = compiled_code['deserialize']

	def deserialize(self, data: bytes) -> Any:
		data_io = io.BytesIO(data)
		return self.deserialization_function(data_io)

	def serialize(self, data: Any) -> bytes:  # the result is stored in the output variable
		output = io.BytesIO()
		self.serialization_function(data, output)
		return output.getvalue()

	def _get_compiled_code(self, schema: Dict[str, Any]) -> Any:
		code = self.code_generator.render_code_with_wraparounds(schema)
		return cerializer.compiler.compile_code(code)
