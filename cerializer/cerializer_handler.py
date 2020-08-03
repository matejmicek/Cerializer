from typing import Any, Dict, List, BinaryIO, Tuple

import io

import jinja2

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

	def __init__(self, schemata: List[Tuple[str, Dict[str, Any]]]) -> None:
		'''
		One can initialize Cerializer with a list of schema_identifier, schema or with a list of schema roots.
		For schema roots usage checkout README.
		'''
		self.code: Dict[str, Any] = {}
		self.env = jinja2.Environment(loader = jinja2.FileSystemLoader(searchpath = '../templates'))
		self.env.globals['env'] = self.env
		self.code_generator = cerializer.schema_handler.CodeGenerator(self.env, schemata, 'buffer')
		self.update_code()

	def deserialize(self, namespace: str, schema_name: str, data: bytes) -> Any:
		data_io = io.BytesIO(data)
		deserialization_function = self.code[
			cerializer.utils.get_schema_identifier(namespace, schema_name)
		]['deserialize']
		return deserialization_function(data_io)

	def serialize(
		self,
		namespace: str,
		schema_name: str,
		data: Any,
		output: BinaryIO,
	) -> None: # the result is stored in the output variable
		serialization_function = self.code[
			cerializer.utils.get_schema_identifier(namespace, schema_name)
		]['serialize']
		serialization_function(data, output)

	def update_code(self, schemata: List[Tuple[str, Dict[str, Any]]]) -> None:
		'''
		Generates code for all schemata in all schema roots and then compiles it.
		'''
		for schema_identifier, schema in schemata:
			self.code_generator.acknowledge_new_schemata(schemata)
			self.code[schema_identifier] = self._get_compiled_code(schema)

	def _get_compiled_code(self, schema: Dict[str, Any]) -> Any:
		code = self.code_generator.render_code(schema)
		return cerializer.compiler.compile_code(code)

