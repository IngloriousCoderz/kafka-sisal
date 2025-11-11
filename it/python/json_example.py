import json

user = {
    "name": "Matteo Antony",
    "id": 123,
}

json_string = json.dumps(user)
print("JSON Serialized (string): " + json_string)
print("Length in bytes: " + str(len(json_string.encode("utf-8"))))

deserialized_user = json.loads(json_string)
print("Deserialized Dictionary:")
print(deserialized_user)
print("Name: " + deserialized_user["name"])
