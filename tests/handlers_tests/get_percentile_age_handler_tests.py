import unittest
from datetime import datetime

from application.handlers import get_percentile_age_handler


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
            self.assertIsInstance(p, float)

    def test_calculate_percentile_when_one_citizen(self):
        grouped = {'A': [19]}
        get_percentile_age_handler._calculate_percentile(grouped)
        self.assertEqual({'A': [19, 19, 19]}, grouped)

    def test_calculate_percentile_when_multiple_citizens(self):
        grouped = {'A': [19, 25, 40, 50, 51, 53, 55]}
        get_percentile_age_handler._calculate_percentile(grouped)
        self.assertEqual({'A': [50.0, 52.0, 54.88]}, grouped)

    def test_calculate_percentile_when_multiple_towns(self):
        grouped = {'A': [19, 25, 40, 50, 51, 53, 55], 'B': [19]}
        get_percentile_age_handler._calculate_percentile(grouped)
        self.assertEqual({'A': [50.0, 52.0, 54.88], 'B': [19.0, 19.0, 19.0]}, grouped)

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
