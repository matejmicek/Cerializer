from typing import Any, Dict, List

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

	def __init__(self, schema_roots: List[str]) -> None:
		self.schema_roots = schema_roots
		self.code: Dict[str, Any] = {}
		self.env = jinja2.Environment(loader = jinja2.FileSystemLoader(searchpath = '../templates'))
		self.env.globals['env'] = self.env
		self.code_generator = cerializer.schema_handler.CodeGenerator(self.env, self.schema_roots, 'buffer')
		self.update_code()

	def deserialize(self, namespace: str, schema_name: str, schema_version: int, data: Any) -> Any:
		deserialization_function = self.code[
			cerializer.utils.get_schema_identifier(namespace, schema_name, schema_version)
		]['deserialize']
		return deserialization_function(data)

	def serialize(
		self,
		namespace: str,
		schema_name: str,
		schema_version: int,
		data: Any,
		output: io.BytesIO,
	) -> None:
		serializatio_function = self.code[
			cerializer.utils.get_schema_identifier(namespace, schema_name, schema_version)
		]['serialize']
		serializatio_function(data, output)

	def update_code(self) -> None:
		'''
        Generates code for all schemata in all schema roots and then compiles it.
        '''
		for schema_path, schema_identifier in cerializer.utils.iterate_over_schema_roots(self.schema_roots):
			schema = cerializer.utils.parse_schema_from_file(schema_path)
			self.code[schema_identifier] = self._get_compiled_code(schema)

	def _get_compiled_code(self, schema: Dict[str, Any]) -> Any:
		self.code_generator.cdefs: List[str] = []
		self.code_generator.necessary_defs = set()
		code = self.code_generator.render_code(schema)
		return cerializer.compiler.compile_code(code)
