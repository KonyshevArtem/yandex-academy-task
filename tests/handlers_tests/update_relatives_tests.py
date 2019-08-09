import unittest
from unittest import mock

from parameterized import parameterized
from pymongo.errors import PyMongoError

import application.handlers.patch_citizen.update_relatives as update_relatives
from tests import test_utils


class UpdateRelativesTests(unittest.TestCase):
    def test_make_update_request_should_raise_exception_when_wrong_operation(self):
        with self.assertRaises(ValueError):
            update_relatives._make_update_relatives_request('test', 0, 0, [])

    def test_make_update_request_should_create_correct_request(self):
        request = update_relatives._make_update_relatives_request('$push', 0, 0, [1])
        self.assertEqual({'import_id': 0}, request._filter)
        self.assertEqual({'$push': {'citizens.$[element].relatives': 0}}, request._doc)
        self.assertEqual([{'element.citizen_id': {'$in': [1]}}], request._array_filters)

    def test_get_relatives_should_return_set_of_relatives(self):
        db = test_utils.get_fake_db()
        db['imports'].insert_one({'import_id': 0, 'citizens': [{'citizen_id': 0, 'relatives': [1, 2, 3]}]})
        relatives = update_relatives._get_relatives(0, 0, db, None)
        self.assertEqual({1, 2, 3}, relatives)

    def test_get_relatives_should_make_set_of_relatives(self):
        db = test_utils.get_fake_db()
        db['imports'].insert_one({'import_id': 0, 'citizens': [{'citizen_id': 0, 'relatives': [1, 1, 2, 2, 3, 3]}]})
        relatives = update_relatives._get_relatives(0, 0, db, None)
        self.assertEqual({1, 2, 3}, relatives)

    def test_get_relatives_should_raise_exception_when_import_not_found(self):
        db = test_utils.get_fake_db()
        with self.assertRaises(PyMongoError):
            update_relatives._get_relatives(0, 0, db, None)

    def test_get_relatives_should_raise_exception_when_citizen_not_found(self):
        db = test_utils.get_fake_db()
        db['imports'].insert_one({'import_id': 0, 'citizens': []})
        with self.assertRaises(PyMongoError):
            update_relatives._get_relatives(0, 0, db, None)

    @parameterized.expand([
        [{1, 2, 3}, {'relatives': [2, 3]}, set(), {1}],
        [{1, 2, 3}, {'relatives': [1, 2, 3, 4]}, {4}, set()],
        [{1, 2, 3}, {'relatives': [2, 3, 4]}, {4}, {1}],
        [{1, 2, 3}, {'relatives': [2, 3, 4, 4]}, {4}, {1}],
        [{1, 2, 3}, {'relatives': []}, set(), {1, 2, 3}],
        [set(), {'relatives': [1, 2, 3]}, {1, 2, 3}, set()],
    ])
    def test_get_relatives_difference(self, old_relatives: set, patch_data: dict, expected_to_push: set,
                                      expected_to_pull: set):
        to_push, to_pull = update_relatives._get_relatives_difference(old_relatives, patch_data)
        self.assertEqual(expected_to_pull, to_pull)
        self.assertEqual(expected_to_push, to_push)

    @parameterized.expand([
        [set(), set(), 0],
        [{1}, set(), 1],
        [set(), {1}, 1],
        [{1}, {2}, 2],
        [{1, 2, 3}, {4, 5, 6}, 2]
    ])
    def test_make_requests_should_return_list_of_length(self, to_push: set, to_pull: set,
                                                        expected_db_requests_length: int):
        with mock.patch('application.handlers.patch_citizen.update_relatives._make_update_relatives_request',
                        return_value=1):
            db_requests = update_relatives._make_db_requests(to_push, to_pull, 0, 0)
            self.assertEqual(expected_db_requests_length, len(db_requests))

    def test_check_citizens_exists_should_do_nothing_when_citizens_empty(self):
        db = test_utils.get_fake_db()
        update_relatives._check_all_citizens_exist(set(), 0, db, None)
        self.assertTrue(True)

    def test_check_citizens_exists_should_not_raise_when_citizen_exists(self):
        db = test_utils.get_fake_db()
        db['imports'].insert_one({'import_id': 0, 'citizens': [{'citizen_id': 0}]})
        update_relatives._check_all_citizens_exist({0}, 0, db, None)
        self.assertTrue(True)

    def test_check_citizens_exists_should_raise_when_citizen_dont_exists(self):
        db = test_utils.get_fake_db()
        with self.assertRaises(PyMongoError):
            update_relatives._check_all_citizens_exist({0}, 0, db, None)

    def test_check_citizens_exists_should_raise_when_at_least_one_citizen_dont_exists(self):
        db = test_utils.get_fake_db()
        db['imports'].insert_one({'import_id': 0, 'citizens': [{'citizen_id': 0}]})
        with self.assertRaises(PyMongoError):
            update_relatives._check_all_citizens_exist({0, 1}, 0, db, None)

    def test_write_relatives_update_should_do_nothing_if_requests_empty(self):
        db = test_utils.get_fake_db()
        update_relatives._write_relatives_update([], db, None)
        db_response = db['imports'].find({})
        self.assertEqual(0, db_response.count())
