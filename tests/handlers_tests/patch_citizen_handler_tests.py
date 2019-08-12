import unittest
from datetime import datetime

from mongolock import MongoLock
from parameterized import parameterized
from pymongo.errors import PyMongoError

import application.handlers.patch_citizen.patch_citizen_handler as patch_citizen_handler
from tests import test_utils


class PatchCitizenHandlerTests(unittest.TestCase):
    def test_parse_birth_date_should_do_nothing_when_no_birth_date(self):
        patch_data = {}
        patch_citizen_handler._parse_birth_date(patch_data)
        self.assertEqual(0, len(patch_data))

    def test_parse_birth_date_should_parse_string_to_datetime(self):
        patch_data = {'birth_date': '31.12.2019'}
        patch_citizen_handler._parse_birth_date(patch_data)
        expected_result = datetime(2019, 12, 31)
        self.assertIsInstance(patch_data['birth_date'], datetime)
        self.assertEqual(expected_result.day, patch_data['birth_date'].day)
        self.assertEqual(expected_result.month, patch_data['birth_date'].month)
        self.assertEqual(expected_result.year, patch_data['birth_date'].year)

    @parameterized.expand([
        ('aaa',),
        ('35.02.1998',),
        ('-10.02.1008',),
        ('12.14.2019',)
    ])
    def test_parse_birth_should_throw_exception_when_datetime_in_wrong_format(self, birth_date: str):
        patch_data = {'birth_date': birth_date}
        with self.assertRaises(ValueError):
            patch_citizen_handler._parse_birth_date(patch_data)

    def test_write_citizen_update_should_update_one_field(self):
        db = test_utils.get_fake_db()
        db['imports'].insert_one({'import_id': 0, 'citizens': [{'citizen_id': 0, 'name': 'test'}]})
        db_response = patch_citizen_handler._write_citizen_update(0, 0, {'name': 'aaa'}, db, None)
        self.assertEqual('aaa', db_response['citizens'][0]['name'])
        self.assertEqual('aaa', db['imports'].find_one({'import_id': 0})['citizens'][0]['name'])

    def test_write_citizen_update_should_update_all_fields(self):
        db = test_utils.get_fake_db()
        db['imports'].insert_one({'import_id': 0, 'citizens': [{'citizen_id': 0, 'name': 'test', 'city': 'test'}]})
        db_response = patch_citizen_handler._write_citizen_update(0, 0, {'name': 'aaa', 'city': 'bbb'}, db, None)
        self.assertEqual('aaa', db_response['citizens'][0]['name'])
        self.assertEqual('bbb', db_response['citizens'][0]['city'])
        self.assertEqual('aaa', db['imports'].find_one({'import_id': 0})['citizens'][0]['name'])
        self.assertEqual('bbb', db['imports'].find_one({'import_id': 0})['citizens'][0]['city'])

    def test_write_citizen_update_should_raise_when_import_not_found(self):
        db = test_utils.get_fake_db()
        with self.assertRaises(PyMongoError):
            patch_citizen_handler._write_citizen_update(0, 0, {}, db, None)

    def test_write_citizen_update_should_raise_when_citizen_not_found(self):
        db = test_utils.get_fake_db()
        db['imports'].insert_one({'import_id': 0, 'citizens': []})
        with self.assertRaises(PyMongoError):
            patch_citizen_handler._write_citizen_update(0, 0, {}, db, None)

    def test_get_citizen_data_should_extract_citizen_data_from_db_request(self):
        citizen = {'citizen_id': 1, 'birth_date': datetime(2019, 12, 31)}
        db_request = {'citizens': [citizen]}
        citizen_data = patch_citizen_handler._get_citizen_data(db_request)
        self.assertEqual(citizen, citizen_data)

    def test_get_citizen_data_should_stringify_birth_date(self):
        citizen = {'citizen_id': 1, 'birth_date': datetime(2019, 12, 31)}
        db_request = {'citizens': [citizen]}
        citizen_data = patch_citizen_handler._get_citizen_data(db_request)
        self.assertEqual('31.12.2019', citizen_data['birth_date'])

    def test_delete_birthdays_should_do_nothing_when_no_relatives_and_no_birth_date_in_patch(self):
        db = test_utils.get_fake_db()
        db['birthdays'].insert_one({'import_id': 0})
        patch_citizen_handler._delete_birthdays_data(0, {}, None, db, None)
        count = db['birthdays'].count_documents({'import_id': 0})
        self.assertEqual(1, count)

    def test_delete_birthdays_should_delete_when_relatives_in_patch(self):
        db = test_utils.get_fake_db()
        lock = MongoLock(client=db.client, db='db')
        db['birthdays'].insert_one({'import_id': 0})
        patch_citizen_handler._delete_birthdays_data(0, {'relatives': []}, lock, db, None)
        count = db['birthdays'].count_documents({'import_id': 0})
        self.assertEqual(0, count)

    def test_delete_birthdays_should_delete_when_birth_date_in_patch(self):
        db = test_utils.get_fake_db()
        lock = MongoLock(client=db.client, db='db')
        db['birthdays'].insert_one({'import_id': 0})
        patch_citizen_handler._delete_birthdays_data(0, {'birth_date': datetime(2019, 1, 1)}, lock, db, None)
        count = db['birthdays'].count_documents({'import_id': 0})
        self.assertEqual(0, count)

    def test_delete_birthdays_should_do_nothing_when_no_birthdays(self):
        db = test_utils.get_fake_db()
        lock = MongoLock(client=db.client, db='db')
        patch_citizen_handler._delete_birthdays_data(0, {'birth_date': datetime(2019, 1, 1)}, lock, db, None)
        count = db['birthdays'].count_documents({'import_id': 0})
        self.assertEqual(0, count)

    def test_delete_percentile_age_should_do_nothing_when_no_town_and_no_birth_date_in_patch(self):
        db = test_utils.get_fake_db()
        db['percentile_age'].insert_one({'import_id': 0})
        patch_citizen_handler._delete_percentile_age_data(0, {}, None, db, None)
        count = db['percentile_age'].count_documents({'import_id': 0})
        self.assertEqual(1, count)

    def test_delete_percentile_age_should_delete_when_town_in_patch(self):
        db = test_utils.get_fake_db()
        lock = MongoLock(client=db.client, db='db')
        db['percentile_age'].insert_one({'import_id': 0})
        patch_citizen_handler._delete_percentile_age_data(0, {'town': 'A'}, lock, db, None)
        count = db['percentile_age'].count_documents({'import_id': 0})
        self.assertEqual(0, count)

    def test_delete_percentile_age_should_delete_when_birth_date_in_patch(self):
        db = test_utils.get_fake_db()
        lock = MongoLock(client=db.client, db='db')
        db['percentile_age'].insert_one({'import_id': 0})
        patch_citizen_handler._delete_percentile_age_data(0, {'birth_date': datetime(2019, 1, 1)}, lock, db, None)
        count = db['percentile_age'].count_documents({'import_id': 0})
        self.assertEqual(0, count)

    def test_delete_percentile_age_should_do_nothing_when_no_percentile_age(self):
        db = test_utils.get_fake_db()
        lock = MongoLock(client=db.client, db='db')
        patch_citizen_handler._delete_percentile_age_data(0, {'birth_date': datetime(2019, 1, 1)}, lock, db, None)
        count = db['percentile_age'].count_documents({'import_id': 0})
        self.assertEqual(0, count)
