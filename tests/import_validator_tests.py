import os
import unittest

from bson import json_util
from jsonschema import ValidationError
from parameterized import parameterized

from validator import Validator


class ValidatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.validator = Validator()

    def test_correct_scheme_should_be_valid(self):
        with open(os.path.join(os.path.dirname(__file__), 'import.json')) as f:
            import_data = json_util.loads(f.read())
        self.validator.validate_import(import_data)

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
        with self.assertRaises(ValidationError) as context:
            self.validator.validate_import(import_data)
        self.assertIn(f'\'{field_name}\' is a required property', str(context.exception))

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
        with self.assertRaises(ValidationError) as context:
            self.validator.validate_import(import_data)
        self.assertIn(f'is not of type \'{data_type}\'', str(context.exception))


if __name__ == '__main__':
    unittest.main()
