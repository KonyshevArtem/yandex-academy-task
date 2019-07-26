import os
from datetime import datetime

import jsonschema
from bson import json_util
from jsonschema import ValidationError


class Validator:
    def __init__(self):
        with open(os.path.join(os.path.dirname(__file__), 'schemas', 'import_schema.json')) as f:
            self.import_schema = json_util.loads(f.read())

    def validate_import(self, import_data: dict):
        jsonschema.validate(import_data, self.import_schema)

        citizen_ids = {citizen['citizen_id'] for citizen in import_data['citizens']}
        if len(citizen_ids) != len(import_data['citizens']):
            raise ValidationError('Citizens ids are not unique')

        for citizen in import_data['citizens']:
            relatives = set(citizen['relatives'])
            if citizen['citizen_id'] in relatives:
                raise ValidationError('Citizen can not be relative to himself')
            if not citizen_ids.issuperset(relatives):
                raise ValidationError('Citizen relative does not exists')

            try:
                citizen['birth_date'] = datetime.strptime(citizen['birth_date'], '%d.%m.%Y')
            except ValueError as e:
                raise ValidationError('Citizen\'s birth_date format is incorrect: ' + str(e))
