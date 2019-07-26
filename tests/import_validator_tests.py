import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock

from bson import json_util
from jsonschema import ValidationError
from parameterized import parameterized

from data_validator import DataValidator


class ImportValidatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_validator = DataValidator()

    def test_correct_import_should_be_valid(self):
        with open(os.path.join(os.path.dirname(__file__), 'import.json')) as f:
            import_data = json_util.loads(f.read())
        self.data_validator.validate_import(import_data)

    def assert_exception(self, import_data: dict, expected_exception_message: str):
        with self.assertRaises(ValidationError) as context:
            self.data_validator.validate_import(import_data)
        self.assertIn(expected_exception_message, str(context.exception))

    @parameterized.expand([
        ({}, 'citizens'),
        ({'citizens': [{'town': '', 'street': '', 'building': '', 'appartement': 0, 'name': '', 'birth_date': '',
                        'gender': '', 'relatives': []}]}, 'citizen_id'),
        ({'citizens': [{'citizen_id': 0, 'street': '', 'building': '', 'appartement': 0, 'name': '', 'birth_date': '',
                        'gender': '', 'relatives': []}]}, 'town'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'building': '', 'appartement': 0, 'name': '', 'birth_date': '',
                        'gender': '', 'relatives': []}]}, 'street'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'appartement': 0, 'name': '', 'birth_date': '',
                        'gender': '', 'relatives': []}]}, 'building'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': '', 'name': '', 'birth_date': '',
                        'gender': '', 'relatives': []}]}, 'appartement'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': '', 'appartement': 0, 'birth_date': '',
                        'gender': '', 'relatives': []}]}, 'name'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': '', 'appartement': 0, 'name': '',
                        'gender': '', 'relatives': []}]}, 'birth_date'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': '', 'appartement': 0, 'name': '',
                        'birth_date': '', 'relatives': []}]}, 'gender'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': '', 'appartement': 0, 'name': '',
                        'birth_date': '', 'gender': ''}]}, 'relatives'),
    ])
    def test_import_should_be_incorrect_when_missing_field(self, import_data: dict, field_name: str):
        self.assert_exception(import_data, f'\'{field_name}\' is a required property')

    @parameterized.expand([
        ({'citizens': None}, 'array'),
        ({'citizens': [{'citizen_id': None, 'town': '', 'street': '', 'building': '', 'appartement': 0, 'name': '',
                        'birth_date': '', 'gender': '', 'relatives': []}]}, 'integer'),
        ({'citizens': [{'citizen_id': 0, 'town': None, 'street': '', 'building': '', 'appartement': 0, 'name': '',
                        'birth_date': '', 'gender': '', 'relatives': []}]}, 'string'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': None, 'building': '', 'appartement': 0, 'name': '',
                        'birth_date': '', 'gender': '', 'relatives': []}]}, 'string'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': None, 'appartement': '', 'name': '',
                        'birth_date': '', 'gender': '', 'relatives': []}]}, 'string'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': '', 'appartement': None, 'name': '',
                        'birth_date': '', 'gender': '', 'relatives': []}]}, 'integer'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': '', 'appartement': 0, 'name': None,
                        'birth_date': '', 'gender': '', 'relatives': []}]}, 'string'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': '', 'appartement': 0, 'name': '',
                        'birth_date': None, 'gender': '', 'relatives': []}]}, 'string'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': '', 'appartement': 0, 'name': '',
                        'birth_date': '', 'gender': None, 'relatives': []}]}, 'string'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': '', 'appartement': 0, 'name': '',
                        'birth_date': '', 'gender': '', 'relatives': None}]}, 'array'),
        ({'citizens': ['']}, 'object'),
        ({'citizens': [{'citizen_id': 0, 'town': '', 'street': '', 'building': '', 'appartement': 0, 'name': '',
                        'birth_date': '', 'gender': '', 'relatives': ['']}]}, 'integer'),
    ])
    def test_import_should_be_incorrect_when_wrong_type_of_field(self, import_data: dict, data_type: str):
        self.assert_exception(import_data, f'is not of type \'{data_type}\'')

    @unittest.mock.patch('jsonschema.validate')
    def test_import_should_be_incorrect_when_citizen_ids_not_unique(self, _):
        import_data = {'citizens': [{'citizen_id': 1}, {'citizen_id': 1}]}
        self.assert_exception(import_data, 'Citizens ids are not unique')

    @unittest.mock.patch('jsonschema.validate')
    def test_import_should_be_incorrect_when_citizen_is_relative_to_himself(self, _):
        import_data = {'citizens': [{'citizen_id': 1, 'relatives': [1]}]}
        self.assert_exception(import_data, 'Citizen can not be relative to himself')

    @unittest.mock.patch('jsonschema.validate')
    def test_import_should_be_incorrect_when_citizen_relative_not_exists(self, _):
        import_data = {'citizens': [{'citizen_id': 1, 'relatives': [2]}]}
        self.assert_exception(import_data, 'Citizen relative does not exists')

    @unittest.mock.patch('jsonschema.validate')
    def test_correct_birth_date_should_be_parsed(self, _):
        import_data = {'citizens': [{'citizen_id': 1, 'birth_date': '01.02.2019', 'relatives': []}]}
        self.data_validator.validate_import(import_data)
        birth_date: datetime = import_data['citizens'][0]['birth_date']
        self.assertIsInstance(birth_date, datetime)
        self.assertEqual(1, birth_date.day)
        self.assertEqual(2, birth_date.month)
        self.assertEqual(2019, birth_date.year)

    @unittest.mock.patch('jsonschema.validate')
    def test_import_should_be_incorrect_when_birth_date_in_wrong_format(self, _):
        import_data = {'citizens': [{'citizen_id': 1, 'birth_date': 'aaaa', 'relatives': []}]}
        self.assert_exception(import_data, 'Citizen\'s birth_date format is incorrect')

    def test_import_should_be_correct_when_no_citizens(self):
        import_date = {'citizens': []}
        self.data_validator.validate_import(import_date)


if __name__ == '__main__':
    unittest.main()
