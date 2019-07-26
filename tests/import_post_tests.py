import os
import unittest
from unittest.mock import MagicMock

import mockupdb
from bson import json_util
from jsonschema import ValidationError
from pymongo import MongoClient

from index import make_app
from validator import Validator


class ImportPostTests(unittest.TestCase):
    @staticmethod
    def create_mock_validator() -> Validator:
        validator = Validator()
        validator.validate_import = MagicMock()
        return validator

    @classmethod
    def setUpClass(cls):
        cls.server = mockupdb.MockupDB(auto_ismaster=True)
        cls.server.run()
        cls.db = MongoClient(cls.server.uri)['db']
        cls.validator = cls.create_mock_validator()
        cls.app = make_app(cls.db, cls.validator).test_client()

    @staticmethod
    def read_import_data():
        document_id = '5a8f1e368f7936badfbb0cfa'
        with open(os.path.join(os.path.dirname(__file__), 'import.json')) as f:
            import_data = json_util.loads(f.read())
        import_data['_id'] = document_id
        import_data['import_id'] = 0
        return import_data

    def test_successful_import_post_should_return_import_id(self):
        headers = [('Content-Type', 'application/json')]
        import_data = self.read_import_data()
        document_id = str(import_data['_id'])
        import_id = import_data['import_id']

        future = mockupdb.go(self.app.post, '/imports', data=json_util.dumps(import_data), headers=headers)
        if self.server.got(mockupdb.OpMsg({'count': 'imports'}, namespace='db')):
            self.server.ok(n=0)
        if self.server.got(mockupdb.OpMsg({'insert': 'imports', 'documents': [import_data]}, namespace='db')):
            self.server.ok(cursor={'inserted_id': document_id})

        http_response = future()

        response_data = http_response.get_json()
        self.assertEqual(import_id, response_data['data']['import_id'])
        self.assertEqual(http_response.status_code, 201)

    def test_when_no_content_type_should_return_bad_request(self):
        http_response = self.app.post('/imports', data=json_util.dumps({'test': 1}))

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Content-Type must be application/json', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_database_error_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]
        import_data = self.read_import_data()

        future = mockupdb.go(self.app.post, '/imports', data=json_util.dumps(import_data), headers=headers)
        if self.server.got(mockupdb.OpMsg({'count': 'imports'}, namespace='db')):
            self.server.ok(n=0)
        if self.server.got(mockupdb.OpMsg({'insert': 'imports', 'documents': [import_data]}, namespace='db')):
            self.server.command_err(11000, 'message')

        http_response = future()
        http_data = http_response.get_data(as_text=True)
        self.assertIn('Database error: ', http_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_incorrect_json_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]

        http_response = self.app.post('/imports', data='{', headers=headers)

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Error when parsing JSON: ', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_invalid_import_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]
        mock_validation = MagicMock(side_effect=ValidationError('message'))
        with unittest.mock.patch.object(self.validator, 'validate_import', mock_validation):
            http_response = self.app.post('/imports', data=json_util.dumps({'test': 1}), headers=headers)

            response_data = http_response.get_data(as_text=True)
            self.assertIn('Import data is not valid', response_data)
            self.assertEqual(400, http_response.status_code)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()


if __name__ == '__main__':
    unittest.main()
