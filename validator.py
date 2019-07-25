import os

import jsonschema
from bson import json_util


class Validator:
    def __init__(self):
        with open(os.path.join(os.path.dirname(__file__), 'schemas', 'import_schema.json')) as f:
            self.import_schema = json_util.loads(f.read())

    def validate_import(self, import_data: dict):
        jsonschema.validate(import_data, self.import_schema)
