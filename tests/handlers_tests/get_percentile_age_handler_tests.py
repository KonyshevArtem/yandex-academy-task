import unittest
from datetime import datetime

from application.handlers import get_percentile_age_handler
from tests import test_utils


class GetPercentileAgeHandlerTests(unittest.TestCase):

    def test_calculate_age_should_do_nothing_when_citizens_empty(self):
        get_percentile_age_handler._calculate_age([])
        self.assertTrue(True)

    def test_calculate_age_should_append_age_when_one_citizen(self):
        citizens = [{'birth_date': datetime(2000, 12, 1)}]
        get_percentile_age_handler._calculate_age(citizens)
        age = citizens[0]['age']
        self.assertTrue(age == datetime.now().year - 2000 or age == datetime.now().year - 2001)

    def test_calculate_age_should_calculate_when_birth_date_in_leap_year(self):
        citizens = [{'birth_date': datetime(2004, 2, 29)}]
        get_percentile_age_handler._calculate_age(citizens)
        for citizen in citizens:
            age = citizen['age']
            self.assertTrue(age == datetime.now().year - 2004 or age == datetime.now().year - 2004)

    def test_group_by_should_return_empty_when_citizens_empty(self):
        grouped = get_percentile_age_handler._group_by_town([])
        self.assertEqual({}, grouped)

    def test_group_by_should_group_when_one_town(self):
        grouped = get_percentile_age_handler._group_by_town([{'town': 'A', 'age': 20}, {'town': 'A', 'age': 19}])
        self.assertEqual({'A': [20, 19]}, grouped)

    def test_group_by_should_group_when_multiple_towns(self):
        grouped = get_percentile_age_handler._group_by_town(
            [{'town': 'A', 'age': 20}, {'town': 'A', 'age': 19}, {'town': 'B', 'age': 19}])
        self.assertEqual({'A': [20, 19], 'B': [19]}, grouped)

    def test_calculate_percentile_should_do_nothing_when_empty_grouped(self):
        get_percentile_age_handler._calculate_percentile({})
        self.assertTrue(True)

    def test_calculate_percentile_should_return_integers(self):
        grouped = {'A': [19]}
        get_percentile_age_handler._calculate_percentile(grouped)
        for p in grouped['A']:
            self.assertIsInstance(p, int)

    def test_calculate_percentile_when_one_citizen(self):
        grouped = {'A': [19]}
        get_percentile_age_handler._calculate_percentile(grouped)
        self.assertEqual({'A': [19, 19, 19]}, grouped)

    def test_calculate_percentile_when_multiple_citizens(self):
        grouped = {'A': [19, 25, 40, 50, 51, 53, 55]}
        get_percentile_age_handler._calculate_percentile(grouped)
        self.assertEqual({'A': [50, 52, 54]}, grouped)

    def test_calculate_percentile_when_multiple_towns(self):
        grouped = {'A': [19, 25, 40, 50, 51, 53, 55], 'B': [19]}
        get_percentile_age_handler._calculate_percentile(grouped)
        self.assertEqual({'A': [50, 52, 54], 'B': [19, 19, 19]}, grouped)

    def test_get_representation_should_be_empty_when_percentile_empty(self):
        representation = get_percentile_age_handler._get_percentiles_representation({})
        self.assertEqual({'data': []}, representation)

    def test_get_representation_when_one_city(self):
        percentiles = {'A': [50, 52, 54]}
        representation = get_percentile_age_handler._get_percentiles_representation(percentiles)
        self.assertEqual({'data': [{'town': 'A', 'p50': 50, 'p75': 52, 'p99': 54}]}, representation)

    def test_get_representation_when_multiple_city(self):
        percentiles = {'A': [50, 52, 54], 'B': [19, 19, 19]}
        representation = get_percentile_age_handler._get_percentiles_representation(percentiles)
        self.assertEqual({'data': [{'town': 'A', 'p50': 50, 'p75': 52, 'p99': 54},
                                   {'town': 'B', 'p50': 19, 'p75': 19, 'p99': 19}]}, representation)

    def test_get_cached_birthdays_should_return_data_if_found(self):
        db = test_utils.get_fake_db()
        db['percentile_age'].insert_one({'import_id': 0, 'data': {'1': []}})
        percentile_age_data = get_percentile_age_handler._get_cached_percentile_age(0, db)
        self.assertEqual({'data': {'1': []}}, percentile_age_data)

    def test_get_cached_birthdays_should_return_none_if_not_found(self):
        db = test_utils.get_fake_db()
        percentile_age_data = get_percentile_age_handler._get_cached_percentile_age(0, db)
        self.assertEqual(None, percentile_age_data)

    def test_cache_percentile_age_should_write_to_db(self):
        db = test_utils.get_fake_db()
        percentile_age_data = {'data': [{'town': 'A', 'p50': 50, 'p75': 52, 'p99': 54},
                                        {'town': 'B', 'p50': 19, 'p75': 19, 'p99': 19}]}
        get_percentile_age_handler._cache_percentile_age_data(0, percentile_age_data, db)
        cached_percentile_age_data = db['percentile_age'].find_one({'import_id': 0}, {'_id': 0, 'import_id': 0})
        self.assertEqual(percentile_age_data, cached_percentile_age_data)
