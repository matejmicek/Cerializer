import io
from typing import Any

import cerializer.code_generator
import cerializer.compiler
import cerializer.schemata
import cerializer.utils


class Cerializer:
	'''
	Basic driver Class for the Cerializer project.
	Each instance of Cerializer corresponds to one schema.
	On init, Cerializer fetches compiled code from CerializerSchemata and uses it to serialize and
	deserialize data.
	'''

	def __init__(
		self,
		cerializer_schemata: cerializer.schemata.CerializerSchemata,
		namespace: str,
		schema_name: str,
	) -> None:
		'''
		Generates a Cerializer instance.
		:param cerializer_schemata: Cerializer schema database
		:param namespace: schema namespace
		:param schema_name: schema name
		'''
		self.schema_identifier = cerializer.utils.get_schema_identifier(namespace, schema_name)
		# self.code_generator = cerializer.code_generator.CodeGenerator(cerializer_schemata, self.schema_identifier,)
		self._cerializer_schemata = cerializer_schemata
		compiled_code = self._cerializer_schemata.get_compiled_code(self.schema_identifier)
		self._serialization_function = compiled_code['serialize']
		self._deserialization_function = compiled_code['deserialize']

	def deserialize(self, data: bytes) -> Any:
		'''
		Deserialize bytes into Python structures.
		:param data: byte like data to read
		:return: deserialized data in Python format.
		'''
		data_io = io.BytesIO(data)
		return self._deserialization_function(data_io)

	def serialize(self, data: Any) -> bytes:
		'''
		Generates a series of bytes representing data.
		:param data: Python objects to be serialized.
		:return: serialized bytes
		'''
		output = io.BytesIO()
		self._serialization_function(data, output)
		return output.getvalue()
