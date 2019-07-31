import os
from datetime import datetime

import jsonschema
from bson import json_util
from jsonschema import ValidationError


class DataValidator(object):
    def __init__(self):
        self.import_schema = self.__load_schema('import_schema.json')
        self.citizen_patch_schema = self.__load_schema('citizen_patch_schema.json')

    @staticmethod
    def __load_schema(schema_name: str):
        with open(os.path.join(os.path.dirname(__file__), 'schemas', schema_name)) as f:
            return json_util.loads(f.read())

    @staticmethod
    def __parse_date(date: str) -> datetime:
        try:
            return datetime.strptime(date, '%d.%m.%Y')
        except ValueError as e:
            raise ValidationError('birth_date format is incorrect: ' + str(e))

    def validate_import(self, import_data: dict):
        jsonschema.validate(import_data, self.import_schema)

        citizen_ids = {citizen['citizen_id'] for citizen in import_data['citizens']}
        if len(citizen_ids) != len(import_data['citizens']):
            raise ValidationError('Citizens ids are not unique')

        citizen_relatives = {citizen['citizen_id']: set(citizen['relatives']) for citizen in import_data['citizens']}
        for citizen in import_data['citizens']:
            citizen_id = citizen['citizen_id']
            relatives = citizen_relatives[citizen_id]

            if len(relatives) != len(citizen['relatives']):
                raise ValidationError('Relatives ids should be unique')
            if citizen_id in relatives:
                raise ValidationError('Citizen can not be relative to himself')
            for relative_id in relatives:
                if relative_id not in citizen_ids:
                    raise ValidationError('Citizen relative does not exists')
                if citizen_id not in citizen_relatives[relative_id]:
                    raise ValidationError('Citizen relatives are not duplex')

            citizen['birth_date'] = self.__parse_date(citizen['birth_date'])

    def validate_citizen_patch(self, citizen_id: int, patch_data: dict):
        jsonschema.validate(patch_data, self.citizen_patch_schema)
        if 'birth_date' in patch_data:
            patch_data['birth_date'] = self.__parse_date(patch_data['birth_date'])
        if 'relatives' in patch_data:
            relatives = set(patch_data['relatives'])
            if len(relatives) != len(patch_data['relatives']):
                raise ValidationError('Relatives ids should be unique')
            if citizen_id in relatives:
                raise ValidationError('Citizen can not be relative to himself')
