import unittest
from datetime import datetime
from unittest import mock

from tests import test_utils


class BirthdaysGetTests(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.app, cls.db, cls.validator = test_utils.set_up_service()
        import_data = test_utils.read_data('import.json')
        import_data['import_id'] = 0
        for citizen in import_data['citizens']:
            citizen['birth_date'] = datetime.strptime(citizen['birth_date'], '%d.%m.%Y')
        cls.db['imports'].insert_one(import_data)

    def test_should_return_birthday_data(self):
        http_response = self.app.get('/imports/0/citizens/birthdays')
        birthday_data = http_response.get_json()
        expected_result = {str(i): [] for i in range(1, 13)}
        expected_result['2'] = [{'citizen_id': 3, 'presents': 1}, {'citizen_id': 1, 'presents': 1}]
        self.assertEqual(201, http_response.status_code)
        self.assertEqual(expected_result, birthday_data['data'])

    def test_should_return_cached_birthday_data_when_present(self):
        expected_result = {str(i): [] for i in range(1, 13)}
        expected_result['2'] = [{'citizen_id': 3, 'presents': 1}, {'citizen_id': 1, 'presents': 1}]
        self.db['birthdays'].insert_one({'import_id': 0, 'data': expected_result})
        http_response = self.app.get('/imports/0/citizens/birthdays')
        birthday_data = http_response.get_json()
        self.assertEqual(201, http_response.status_code)
        self.assertEqual(expected_result, birthday_data['data'])

    def test_should_cache_birthday_data(self):
        with mock.patch('application.handlers.get_birthdays_handler._cache_birthdays_data') as cache_birthdays_mock:
            http_response = self.app.get('/imports/0/citizens/birthdays')
            self.assertEqual(201, http_response.status_code)
            cache_birthdays_mock.assert_called()

    def test_should_not_cache_when_birthday_data_in_cache(self):
        with mock.patch('application.handlers.get_birthdays_handler._cache_birthdays_data') as cache_birthdays_mock:
            self.db['birthdays'].insert_one({'import_id': 0})
            http_response = self.app.get('/imports/0/citizens/birthdays')
            self.assertEqual(201, http_response.status_code)
            cache_birthdays_mock.assert_not_called()
