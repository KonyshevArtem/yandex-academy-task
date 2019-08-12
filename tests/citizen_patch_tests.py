import unittest
from datetime import datetime
from unittest.mock import MagicMock

from bson import json_util
from jsonschema import ValidationError
from parameterized import parameterized

import tests.test_utils as test_utils


class CitizenPatchTests(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.app, cls.db, cls.validator = test_utils.set_up_service()
        import_data = test_utils.read_data('import.json')
        import_data['import_id'] = 0
        for citizen in import_data['citizens']:
            citizen['birth_date'] = datetime.strptime(citizen['birth_date'], '%d.%m.%Y')
        cls.db['imports'].insert_one(import_data)

    def test_update_db_when_patch_received(self):
        headers = [('Content-Type', 'application/json')]
        patch_data = {'name': 'test'}

        http_response = self.app.patch('/imports/0/citizens/1', data=json_util.dumps(patch_data), headers=headers)

        response_data = http_response.get_json()
        self.assertEqual(http_response.status_code, 201)
        self.assertEqual(patch_data['name'], response_data['data']['name'])

    def test_should_return_bad_request_when_no_content_type(self):
        patch_data = {'name': 'test'}

        http_response = self.app.patch('/imports/0/citizens/1', data=json_util.dumps(patch_data))

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Content-Type must be application/json', response_data)
        self.assertEqual(400, http_response.status_code)

    @parameterized.expand([
        ['/imports/1/citizens/1'],
        ['/imports/0/citizens/5']
    ])
    def test_should_return_bad_request_when_no_import_or_citizen_found(self, url: str):
        headers = [('Content-Type', 'application/json')]
        patch_data = {'name': 'test'}

        http_response = self.app.patch(url, data=json_util.dumps(patch_data), headers=headers)

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Import or citizen with specified id not found', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_should_return_bad_request_when_patch_not_valid(self):
        headers = [('Content-Type', 'application/json')]
        patch_data = {'name': 'test'}
        mock_validation = MagicMock(side_effect=ValidationError('message'))

        with unittest.mock.patch.object(self.validator, 'validate_citizen_patch', mock_validation):
            http_response = self.app.patch('/imports/0/citizens/1', data=json_util.dumps(patch_data), headers=headers)

            response_data = http_response.get_data(as_text=True)
            self.assertIn('Input data is not valid', response_data)
            self.assertEqual(400, http_response.status_code)

    def test_should_return_bad_request_when_incorrect_json(self):
        headers = [('Content-Type', 'application/json')]

        http_response = self.app.patch('/imports/0/citizens/1', data='{', headers=headers)

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Error when parsing JSON', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_should_return_bad_request_when_birth_date_wrong_format(self):
        headers = [('Content-Type', 'application/json')]
        patch_data = {'birth_date': 'aaa'}

        http_response = self.app.patch('/imports/0/citizens/1', data=json_util.dumps(patch_data), headers=headers)

        response_data = http_response.get_data(as_text=True)
        self.assertIn('does not match format', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_should_return_correct_birth_date(self):
        headers = [('Content-Type', 'application/json')]
        patch_data = {'birth_date': '01.01.2012'}

        http_response = self.app.patch('/imports/0/citizens/1', data=json_util.dumps(patch_data), headers=headers)

        response_data = http_response.get_json()
        self.assertEqual(http_response.status_code, 201)
        self.assertEqual(patch_data['birth_date'], response_data['data']['birth_date'])

    def test_should_not_delete_birthdays_when_no_relatives_and_no_birth_date(self):
        headers = [('Content-Type', 'application/json')]
        patch_data = {'name': 'aaa'}
        self.db['birthdays'].insert_one({'import_id': 0})

        http_response = self.app.patch('/imports/0/citizens/1', data=json_util.dumps(patch_data), headers=headers)

        self.assertEqual(http_response.status_code, 201)
        self.assertEqual(1, self.db['birthdays'].count_documents({'import_id': 0}))

    def test_should_delete_birthdays_when_birth_date_in_patch(self):
        headers = [('Content-Type', 'application/json')]
        patch_data = {'birth_date': '01.01.2019'}
        self.db['birthdays'].insert_one({'import_id': 0})

        http_response = self.app.patch('/imports/0/citizens/1', data=json_util.dumps(patch_data), headers=headers)

        self.assertEqual(http_response.status_code, 201)
        self.assertEqual(0, self.db['birthdays'].count_documents({'import_id': 0}))

    def test_should_not_raise_when_no_birthday_data(self):
        headers = [('Content-Type', 'application/json')]
        patch_data = {'birth_date': '01.01.2019'}

        http_response = self.app.patch('/imports/0/citizens/1', data=json_util.dumps(patch_data), headers=headers)

        self.assertEqual(http_response.status_code, 201)
        self.assertEqual(0, self.db['birthdays'].count_documents({'import_id': 0}))


if __name__ == '__main__':
    unittest.main()
