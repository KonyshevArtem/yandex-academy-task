import os

import jsonschema
from bson import json_util
from jsonschema import ValidationError


class DataValidator(object):
    def __init__(self):
        self.import_schema = _load_schema('import_schema.json')
        self.citizen_patch_schema = _load_schema('citizen_patch_schema.json')

    def validate_import(self, import_data: dict):
        """
        Проводит валидацию данных поставки.

        Проверяется:
        1. JSON схема
        2. Уникальность идентификаторов жителей
        3. Уникальность идентификаторов родственников каждого жителя
        4. Родственность жителя к самому себе
        5. Существование родственника с указанным индексом
        6. Наличие обратной родственной связи

        :param dict import_data: Данные поставки
        :raises: :class:`ValidationError`: Нарушение любого из указанных пунктов
        """
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

    def validate_citizen_patch(self, citizen_id: int, patch_data: dict):
        """
        Проводит валидацию данных модификации жителя.

        Проверяется:
        1. JSON схема
        2. Уникальность идентификаторов родственников каждого жителя
        3. Родственность жителя к самому себе

        :param citizen_id: Уникальный идентификатор модифицируемого жителя
        :param patch_data: Данные модификации
        :raises: :class:`ValidationError`: Нарушение любого из указанных пунктов
        """
        jsonschema.validate(patch_data, self.citizen_patch_schema)
        if 'relatives' in patch_data:
            relatives = set(patch_data['relatives'])
            if len(relatives) != len(patch_data['relatives']):
                raise ValidationError('Relatives ids should be unique')
            if citizen_id in relatives:
                raise ValidationError('Citizen can not be relative to himself')


def _load_schema(schema_name: str) -> dict:
    """
    Загружает JSON схему из файла с указанным именем.

    :param str schema_name: Имя файла, содержащего JSON схема
    :return: Загруженная JSON схема
    :rtype: dict
    """
    with open(os.path.join(os.path.dirname(__file__), 'schemas', schema_name)) as f:
        return json_util.loads(f.read())
