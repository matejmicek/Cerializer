import jinja2


import cerializer.compiler
import cerializer.schema_handler
import cerializer.utils


class Cerializer:
	def __init__(self, schema_roots):
		self.schema_roots = schema_roots
		self.code = {}
		self.env = jinja2.Environment(loader = jinja2.FileSystemLoader(searchpath = '../templates'))
		self.env.globals['env'] = self.env
		self.code_generator = cerializer.schema_handler.CodeGenerator(self.env, self.schema_roots, 'buffer')
		self.update_code()

	def deserialize(self, namespace, schema_name, schema_version, data):
		deserializatio_function = self.code[
			cerializer.utils.get_schema_identifier(namespace, schema_name, schema_version)
		]['deserialize']
		return deserializatio_function(data)

	def serialize(self, namespace, schema_name, schema_version, data, output):
		serializatio_function = self.code[
			cerializer.utils.get_schema_identifier(namespace, schema_name, schema_version)
		]['serialize']
		return serializatio_function(data, output)

	def update_code(self):
		'''
        Generates code for all schemata in all schema roots and then compiles it.
        '''
		for schema_path, schema_identifier in cerializer.utils.iterate_over_schema_roots(self.schema_roots):
			schema = cerializer.utils.parse_schema_from_file(schema_path.decode())
			self.code[schema_identifier] = self.get_compiled_code(schema)

	def get_compiled_code(self, schema):
		self.code_generator.cdefs = []
		self.code_generator.necessary_defs = set()
		code = self.code_generator.render_code(schema)
		return cerializer.compiler.compile_code(code)
