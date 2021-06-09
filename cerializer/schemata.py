# pylint: disable=protected-access
import copy
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import cerializer.utils
import cerializer.code_generator
import cerializer.compiler
import cerializer.cerializer_daemon
import tqdm



class CerializerSchemata:
	'''
	Storage class for schemata and compiled code.
	An instance of CerializerSchemata is ideally shared between multiple Cerializer instances.
	This is because each schema repository runs a daemon in the background and compiles and holds
	all the available schemata. This way, it would be counterproductive to have one schema repo for each
	Cerializer.
	You can init CerializerSchemata either with list of schemata or a schema url pointing to Kafka schema repo.
	'''

	def __init__(self, schemata: List[Tuple[str, Any]] = None, schemata_url: str = None, verbose = False) -> None:
		'''
		Produces an instance of CerializerSchemata.
		On init, it compiles all the schemata either from the list of schemata or the schema ulr.
		This instance is meant to be reused between multiple Cerializers.
		You can either supply a list of schemata or Kafka repo url or both.
		:param schemata: list of tuples in form of (schema_identifier, schema)
		:param schemata_url: url to Kafka schema repo.
		'''
		self._schema_database = cerializer.utils.get_subschemata(schemata) if schemata else {}
		self._schema_code_database = {}
		self._schemata_url = schemata_url
		self._cycle_starting_nodes: Set[str] = set()
		self._init_cycles()
		self._verbose = verbose
		# fetching and compiling all the schemata from Kafka
		if self._schemata_url:
			self._cerializer_daemon = cerializer.cerializer_daemon.CerializerDaemon(self, self._schemata_url)
			# downloads and compiles schemata in a snapshot way
			self._cerializer_daemon.update_with_schema_repo()
			# checks periodically for new schemata
			self._cerializer_daemon.start()
		# compiling all the code for schemata from schema list
		for schema_identifier, schema in tqdm.tqdm(
				self._schema_database.items(),
				desc = 'Compiling schemata',
				disable = not self._verbose
		):
			code_generator = cerializer.code_generator.CodeGenerator(self, schema_identifier)
			code = code_generator.render_code_with_wraparounds(schema)
			compiled_code = cerializer.compiler.compile_code(code)
			self.add_code(schema_identifier, compiled_code)

	# custom contains definition
	def __contains__(self, item: str) -> bool:
		'''
		Returns whether we have the given schema in the database.
		:param item: schema identifier
		:return: presence of schema in database
		'''
		return item in self._schema_database

	def get_known_schemata(self) -> Set[str]:
		'''
		Callback used mainly by the daemon. Returns all the schema identifiers we have code for.
		:return: schema identifiers
		'''
		return set(self._schema_code_database.keys())

	def add_code(self, schema_identifier, schema_code) -> None:
		'''
		Callback to add code for a schema identifier. Used from within daemon.
		:param schema_identifier: schema identifier
		:param schema_code: compiled code
		:return: None
		'''
		self._schema_code_database[schema_identifier] = schema_code

	def add_schema(self, schema_identifier, schema) -> None:
		'''
		Callback to add schema, not schema code.
		:param schema_identifier: schema identifier to add
		:param schema: schema to add
		:return: None
		'''
		new_subschemata = cerializer.utils.get_subschemata([(schema_identifier, schema)])
		self._schema_database = {**self._schema_database, **new_subschemata}
		self._init_cycles()

	def get_compiled_code(self, schema_identifier):
		'''
		Returns the compiled code for the given schema identifier.
		:param schema_identifier: schema identifier to get the code for.
		:return: compiled code
		'''
		return self._schema_code_database[schema_identifier]

	def load_schema(
		self,
		schema_identifier: str,
		context_schema_identifier: Optional[str] = None,
	) -> Union[str, List, Dict[str, Any]]:
		'''
		Loads the schema corresponding to the schema identifier.
		We first check whether the schema we are looking for is not defined in the same big schema
		this would mean the schema is redefined and that the local version has to be used
		:param schema_identifier: schema identifier to look for
		:param context_schema_identifier: in which schema are we, for redefinition purposes.
		:return: Schema
		'''
		# we first check whether the schema we are looking for is not defined in the same big schema
		# this would mean the schema is redefined and that the local version has to be used
		if context_schema_identifier:
			context_schema = self._schema_database[context_schema_identifier]
			# mypy needs this type annotation
			local_schema_database: Dict[str, Union[str, List[Any], Dict[str, Any]]] = cerializer.utils.get_subschemata(
				[(context_schema_identifier, context_schema)]
			)
			if schema_identifier in local_schema_database:
				return local_schema_database[schema_identifier]
		if schema_identifier in self._schema_database:
			return self._schema_database[schema_identifier]
		else:
			raise RuntimeError(f'Schema with identifier = {schema_identifier} not found in schema database.')

	def is_cycle_starting(self, schema_identifier: str) -> bool:
		'''
		Checks whether a schema identifier starts a cycle.
		:param schema_identifier: schema identifier
		:return: bool, cycle starting
		'''
		return schema_identifier in self._cycle_starting_nodes

	def _init_cycles(self) -> None:
		'''
		Look for all the cycles within the schema database.
		:return: None
		'''
		for _, schema in self._schema_database.items():
			visited: Set[str] = set()
			self._cycle_detection(schema, visited, self._cycle_starting_nodes)

	def _cycle_detection(
		self,
		parsed_schema: Union[str, List, Dict[str, Any]],
		visited: Set[str],
		cycle_starting_nodes: Set[str],
	) -> None:
		'''
		Detects cycles in schemata.
		This can happen when for example a schema is defined using itself eg. a tree schema.
		This method add all cycle starting nodes in all schemata_database to cycle_starting_nodes set.
		:param parsed_schema: schema to check
		:param visited: visited nodes
		:param cycle_starting_nodes: cycle starting nodes
		:return: None
		'''
		if isinstance(parsed_schema, str) and parsed_schema in visited:
			cycle_starting_nodes.add(parsed_schema)
		elif isinstance(parsed_schema, dict):
			name = parsed_schema.get('name')
			type_ = parsed_schema['type']
			if type(type_) is str and type_ in visited:
				cycle_starting_nodes.add(type_)
			elif name:
				visited.add(name)
				new_visited = copy.deepcopy(visited)
				if 'fields' in parsed_schema:
					for field in parsed_schema['fields']:
						self._cycle_detection(field, new_visited, cycle_starting_nodes)
				if type(type_) is dict:
					self._cycle_detection(type_, new_visited, cycle_starting_nodes)
				if type(type_) is list:
					for element in type_:
						self._cycle_detection(element, new_visited, cycle_starting_nodes)
				elif type(type_) is str and type_ in self:
					self._cycle_detection(self.load_schema(type_), new_visited, cycle_starting_nodes)
