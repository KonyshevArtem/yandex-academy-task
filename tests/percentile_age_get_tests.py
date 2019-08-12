import unittest
from datetime import datetime
from unittest import mock

from tests import test_utils


class PercentileAgeGetTests(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.app, cls.db, cls.validator = test_utils.set_up_service()
        import_data = test_utils.read_data('import.json')
        import_data['import_id'] = 0
        for citizen in import_data['citizens']:
            citizen['birth_date'] = datetime.strptime(citizen['birth_date'], '%d.%m.%Y')
        cls.db['imports'].insert_one(import_data)

    def test_should_return_percentile_age_data(self):
        http_response = self.app.get('/imports/0/towns/stat/percentile/age')
        percentile_age_data = http_response.get_json()
        expected_result = [{'town': 'Москва', 'p50': 19, 'p75': 19, 'p99': 19},
                           {'town': 'Челябинск', 'p50': 35, 'p75': 35, 'p99': 35},
                           {'town': 'Шумиловский', 'p50': 21, 'p75': 21, 'p99': 21}]
        self.assertEqual(201, http_response.status_code)
        self.assertEqual(expected_result, percentile_age_data['data'])

    def test_should_return_cached_percentile_age_data_when_present(self):
        expected_result = []
        self.db['percentile_age'].insert_one({'import_id': 0, 'data': expected_result})
        http_response = self.app.get('/imports/0/towns/stat/percentile/age')
        birthday_data = http_response.get_json()
        self.assertEqual(201, http_response.status_code)
        self.assertEqual(expected_result, birthday_data['data'])

    def test_should_cache_percentile_age_data(self):
        module = 'application.handlers.get_percentile_age_handler._cache_percentile_age_data'
        with mock.patch(module) as cache_percentile_age_mock:
            http_response = self.app.get('/imports/0/towns/stat/percentile/age')
            self.assertEqual(201, http_response.status_code)
            cache_percentile_age_mock.assert_called()

    def test_should_not_cache_when_birthday_data_in_cache(self):
        module = 'application.handlers.get_percentile_age_handler._cache_percentile_age_data'
        with mock.patch(module) as cache_percentile_age_mock:
            self.db['percentile_age'].insert_one({'import_id': 0})
            http_response = self.app.get('/imports/0/towns/stat/percentile/age')
            self.assertEqual(201, http_response.status_code)
            cache_percentile_age_mock.assert_not_called()

    def test_should_return_bad_request_when_id_incorrect(self):
        self.db['imports'].delete_one({'import_id': 0})
        http_response = self.app.get('/imports/0/towns/stat/percentile/age')
        response_data = http_response.get_data(as_text=True)
        self.assertEqual(400, http_response.status_code)
        self.assertIn('Import with specified id not found', response_data)
