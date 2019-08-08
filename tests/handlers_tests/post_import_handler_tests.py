import unittest
from datetime import datetime
from unittest.mock import MagicMock

from parameterized import parameterized
from pymongo.errors import PyMongoError

import application.handlers.post_import_handler as post_import_handler
from tests import test_utils


class PostImportHandlerTests(unittest.TestCase):
    def test_parse_birth_date_should_not_throw_error_when_no_citizens(self):
        import_data = {'citizens': []}
        post_import_handler._parse_birth_date(import_data)
        self.assertTrue(True)

    def test_parse_birth_date_should_parse_string_to_datetime_when_one_citizen(self):
        import_data = {'citizens': [{'birth_date': '01.02.2019'}]}
        expected_result = datetime(2019, 2, 1)
        post_import_handler._parse_birth_date(import_data)
        birth_date = import_data['citizens'][0]['birth_date']
        self.assertIsInstance(birth_date, datetime)
        self.assertEqual(birth_date.year, expected_result.year)
        self.assertEqual(birth_date.month, expected_result.month)
        self.assertEqual(birth_date.day, expected_result.day)

    def test_parse_birth_date_should_parse_string_to_datetime_when_multiple_citizens(self):
        import_data = {'citizens': [{'birth_date': '01.02.2019'}, {'birth_date': '02.01.2017'}]}
        expected_result = [datetime(2019, 2, 1), datetime(2017, 1, 2)]
        post_import_handler._parse_birth_date(import_data)
        for i in range(len(expected_result)):
            birth_date = import_data['citizens'][i]['birth_date']
            self.assertIsInstance(birth_date, datetime)
            self.assertEqual(birth_date.year, expected_result[i].year)
            self.assertEqual(birth_date.month, expected_result[i].month)
            self.assertEqual(birth_date.day, expected_result[i].day)

    @parameterized.expand([
        ('aaa',),
        ('35.02.1998',),
        ('-10.02.1008',),
        ('12.14.2019',)
    ])
    def test_parse_birth_should_throw_exception_when_datetime_in_wrong_format(self, birth_date: str):
        import_date = {'citizens': [{'birth_date': birth_date}]}
        with self.assertRaises(ValueError):
            post_import_handler._parse_birth_date(import_date)

    def test_add_import_id_should_add_zero_when_db_empty(self):
        db = test_utils.get_fake_db()
        import_data = {}
        post_import_handler._add_import_id(import_data, db)
        self.assertIn('import_id', import_data)
        self.assertEqual(0, import_data['import_id'])

    def test_add_import_id_should_add_count_of_imports(self):
        db = test_utils.get_fake_db()
        db['imports'].insert_one({})
        import_data = {}
        post_import_handler._add_import_id(import_data, db)
        self.assertIn('import_id', import_data)
        self.assertEqual(1, import_data['import_id'])

    def test_write_to_db_should_insert_import_data_in_db(self):
        db = test_utils.get_fake_db()
        import_data = {'import_id': 0}
        data, status = post_import_handler._write_to_db(import_data, db)
        self.assertEqual(201, status)
        self.assertEqual(import_data['import_id'], data['data']['import_id'])
        inserted_import_data = db['imports'].find_one({'import_id': import_data['import_id']})
        self.assertEqual(import_data, inserted_import_data)

    def test_write_to_db_should_raise_error_when_write_not_acknowledged(self):
        class FakeInsertOneResult:
            def __init__(self):
                self.acknowledged = False

        db = test_utils.get_fake_db()
        db['imports'].insert_one = MagicMock(return_value=FakeInsertOneResult())
        with self.assertRaises(PyMongoError):
            post_import_handler._write_to_db({}, db)
