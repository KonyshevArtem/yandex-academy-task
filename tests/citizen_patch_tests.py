import logging
import unittest
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
        cls.db['imports'].insert_one(import_data)
        logging.disable(logging.CRITICAL)

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


if __name__ == '__main__':
    unittest.main()