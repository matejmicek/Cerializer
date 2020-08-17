from typing import Dict, Any, List, Tuple

import schemachinery.codec.base
import schemachinery.codec.avro_codec
import schemachinery.codec.avro_schemata
import cerializer.quantlane_utils
import constants.constants
import io
import struct


class MissingCerializerCode(Exception):
	pass


class CerializerQuantlaneCodec(schemachinery.codec.base.BaseCodec):
	'''
	A version of Cerializer codec built fur Quantlane purposes.
	Works the same way as AvroCodec.
	'''

	def __init__(
		self,
		schema_roots: List[str],
		namespace: str,
		schema_name: str,
		expected_version: int,
	) -> None:
		super().__init__()
		self.schema_roots = schema_roots
		self.namespace = namespace
		self.schema_name = schema_name
		self.expected_version = expected_version
		# will raise MissingCerializerCode if the module is not found
		module = cerializer.quantlane_utils.get_module(
			self.schema_roots,
			self.namespace,
			self.schema_name,
			self.expected_version,
		)
		self.serialization_function = module['serialize']
		self.deserialization_function = module['deserialize']
		# storing alternative decoding functions
		self.alternative_decode: Dict[int, Any] = {}
		self.avro_codec = schemachinery.codec.avro_codec.AvroCodec(
			avro_schemata = schemachinery.codec.avro_schemata.AvroSchemata(*self.schema_roots),
			namespace = namespace,
			schema_name = schema_name,
			expected_version = expected_version,
		)

	def __eq__(self, other: Any) -> bool:
		if isinstance(other, type(self)):
			return (
				self.namespace == other.namespace
				and self.schema_name == other.schema_name
				and self.expected_version == other.expected_version
			)
		return False

	def encode(self, payload: schemachinery.codec.base.PayloadType) -> bytes:
		output = io.BytesIO()
		self.serialization_function(payload, output)
		return cerializer.quantlane_utils.MAGIC_BYTE + struct.pack('>I', self.expected_version) + output.getvalue()

	def decode(self, encoded: bytes) -> schemachinery.codec.base.PayloadType:
		# we assume correct version of schema and correct magic byte
		# removing magic byte and version prefix
		data_io = io.BytesIO(encoded[5:])
		return self.deserialization_function(data_io)

	def decode_no_migration(self, encoded: bytes) -> Tuple[int, schemachinery.codec.base.PayloadType]:
		if encoded[0] != ord(cerializer.quantlane_utils.MAGIC_BYTE):
			raise ValueError('Invalid magic byte in message = {!r}'.format(encoded))
		# Extract schema version and load the schema.
		try:
			(version,) = struct.unpack('>I', encoded[1:5])
		except struct.error as e:
			raise ValueError('Invalid schema version in message = {!r}'.format(encoded)) from e
		if version in self.alternative_decode:
			return version, self.alternative_decode[version]
		try:
			decode = cerializer.quantlane_utils.get_module(
				self.schema_roots,
				self.namespace,
				self.schema_name,
				version,
			)
		except MissingCerializerCode:
			return self.avro_codec.decode_no_migration(encoded)
		self.alternative_decode[version] = decode
		return version, decode(encoded)

	def migrate(
		self,
		payload: schemachinery.codec.base.PayloadType,
		from_version: int,
		to_version: int,
	) -> schemachinery.codec.base.PayloadType:
		return self.avro_codec.migrate(payload, from_version, to_version)
