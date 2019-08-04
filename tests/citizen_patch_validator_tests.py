import unittest

from jsonschema import ValidationError
from parameterized import parameterized

from application.data_validator import DataValidator
from tests import test_utils


class CitizenPatchValidatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_validator = DataValidator()

    def assert_exception(self, citizen_id: int, patch_data: dict, expected_exception_message: str):
        with self.assertRaises(ValidationError) as context:
            self.data_validator.validate_citizen_patch(citizen_id, patch_data)
        self.assertIn(expected_exception_message, str(context.exception))

    def test_correct_patch_should_be_valid(self):
        patch_data = test_utils.read_data('citizen_patch.json')
        self.data_validator.validate_citizen_patch(0, patch_data)

    def test_patch_should_be_invalid_when_additional_fields(self):
        patch_data = {'name': 'test', 'test': 1}
        self.assert_exception(0, patch_data, 'Additional properties are not allowed')

    def test_patch_should_be_invalid_when_no_fields(self):
        patch_data = {}
        self.assert_exception(0, patch_data, 'does not have enough properties')

    @parameterized.expand([
        [{'name': None}, 'string'],
        [{'town': None}, 'string'],
        [{'street': None}, 'string'],
        [{'building': None}, 'string'],
        [{'apartment': None}, 'integer'],
        [{'birth_date': None}, 'string'],
        [{'gender': None}, 'string'],
        [{'relatives': None}, 'array'],
        [{'relatives': ['']}, 'integer']
    ])
    def test_patch_should_be_incorrect_when_wrong_type_of_field(self, patch_data: dict, data_type: str):
        self.assert_exception(0, patch_data, f'is not of type \'{data_type}\'')

    @unittest.mock.patch('jsonschema.validate')
    def test_patch_should_be_incorrect_when_relatives_contains_citizen_id(self, _):
        patch_data = {'relatives': [0]}
        self.assert_exception(0, patch_data, 'Citizen can not be relative to himself')

    @unittest.mock.patch('jsonschema.validate')
    def test_patch_should_be_incorrect_when_relatives_not_unique(self, _):
        patch_data = {'relatives': [1, 1]}
        self.assert_exception(0, patch_data, 'Relatives ids should be unique')


if __name__ == '__main__':
    unittest.main()
