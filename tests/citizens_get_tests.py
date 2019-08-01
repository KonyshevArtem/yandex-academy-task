import unittest
from datetime import datetime

from tests import test_utils


class CitizensGetTests(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.app, cls.db, cls.validator = test_utils.set_up_service()
        import_data = test_utils.read_data('import.json')
        for citizen in import_data['citizens']:
            citizen['birth_date'] = datetime.strptime(citizen['birth_date'], '%d.%m.%Y')
        import_data['import_id'] = 0
        cls.db['imports'].insert_one(import_data)

    def test_should_return_citizens_when_id_correct(self):
        http_response = self.app.get('/imports/0/citizens')
        response_data = http_response.get_json()
        self.assertEqual(201, http_response.status_code)
        expected_data = self.db['imports'].find_one({'import_id': 0}, {'_id': 0, 'import_id': 0})
        self.assertEqual(expected_data['citizens'], response_data['data'])

    def test_should_return_bad_request_when_id_incorrect(self):
        http_response = self.app.get('/imports/1/citizens')
        response_data = http_response.get_data(as_text=True)
        self.assertEqual(400, http_response.status_code)
        self.assertIn('Import with specified id not found', response_data)


if __name__ == '__main__':
    unittest.main()
