import json
import threading

import requests

import cerializer.code_generator
import cerializer.compiler


class CerializerDaemon(threading.Thread):
	'''
	Deamon that takes care of fetching new schemata from Kafka and compiling them.
	It runs on a second thread to Cerializer Schemata.
	'''

	started_threads = []

	def __init__(self, cerializer_schemata, schemata_url, refresh_time = 10) -> None:
		'''
		Initialises a daemon to run in the background and fetch schemata and compile them.
		:param cerializer_schemata: An instance of schemata that this daemon should be responsible for.
		:param schemata_url: Kafka repo url
		:param refresh_time: Period of refreshes and fetching.
		'''
		super().__init__(daemon = True)
		self._refresh_time = refresh_time
		self._schema_url = schemata_url
		self._cerializer_schemata = cerializer_schemata
		self._stop_requested = threading.Event()

	def start(self) -> None:
		'''
		Starts the daemon.
		:return: None
		'''
		self.started_threads.append(self)
		return super().start()

	def stop(self) -> None:
		'''
		Stops the daemon.
		:return: None
		'''
		self._stop_requested.set()

	def update_with_schema_repo(self):
		'''
		Fetches all the schemata from Kafka and renders missing code if necessary.
		:return: None
		'''
		known_schemata = self._cerializer_schemata.get_known_schemata()
		subjects = requests.get(f'{self._schema_url}/subjects').json()
		waiting_to_be_rendered = []
		for subject in subjects:
			versions = requests.get(f'{self._schema_url}/subjects/{subject}/versions').json()
			for version in versions:
				record = requests.get(f'{self._schema_url}/subjects/{subject}/versions/{version}').json()
				schema = json.loads(record['schema'])
				# getting rid of the -value added by Kafka
				schema_name = ''.join(record['subject'].split('-')[:-1])
				schema_version = record['version']
				schema_identifier = f'{schema_name}:{schema_version}'
				if schema_identifier in known_schemata:
					# we dont need to add this schema to schemata since we know it already
					continue
				# we first add all the new schemata into the repo since in case there was a cross reference
				# between two new schemata, code generation would fail if one of them was not in the database.
				self._cerializer_schemata.add_schema(schema_identifier, schema)
				waiting_to_be_rendered.append((schema_identifier, schema))
		for schema_identifier, schema in waiting_to_be_rendered:
			# compiles the code and adds it to Schema repo via a callback
			code_generator = cerializer.code_generator.CodeGenerator(self._cerializer_schemata, schema_identifier)
			code = code_generator.render_code_with_wraparounds(schema)
			compiled_code = cerializer.compiler.compile_code(code)
			self._cerializer_schemata.add_code(schema_identifier, compiled_code)

	def run(self) -> None:
		'''
		The poller checks every refresh_time seconds whether there are any new schemata.
		:return: None
		'''
		while not self._stop_requested.is_set():
			self.update_with_schema_repo()
			self._stop_requested.wait(self._refresh_time)
