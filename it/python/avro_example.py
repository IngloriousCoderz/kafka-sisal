import avro.schema
from avro.io import DatumWriter, DatumReader
from io import BytesIO

schema_str = """
{
  "type": "record",
  "name": "User",
  "fields": [
    { "name": "name", "type": "string" },
    { "name": "id", "type": "int" }
  ]
}
"""
schema = avro.schema.parse(schema_str)

user = {
    "name": "Matteo Antony",
    "id": 123
}

writer = DatumWriter(schema)
bytes_writer = BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)
writer.write(user, encoder)
avro_bytes = bytes_writer.getvalue()
print("Avro serialized (bytes): " + str(len(avro_bytes)) + " bytes")

reader = DatumReader(schema)
bytes_reader = BytesIO(avro_bytes)
decoder = avro.io.BinaryDecoder(bytes_reader)
deserialized_user = reader.read(decoder)
print("Deserialized dictionary:")
print(deserialized_user)
print("Name: " + deserialized_user["name"])
