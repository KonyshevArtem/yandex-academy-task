import json
import unittest
from unittest import mock
from unittest.mock import MagicMock

from flask import Response
from mongolock import MongoLock

from application.decorators import response_cacher
from tests import test_utils


class ResponseCacherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db = test_utils.get_fake_db()

    def test_cache_data_should_write_to_db(self):
        response_cacher._cache_data(0, 'cache', {'test': 'aaa'}, self.db)
        cached_data = self.db['cache'].find_one({'import_id': 0}, {'import_id': 0, '_id': 0})
        self.assertIsNotNone(cached_data)
        self.assertEqual({'test': 'aaa'}, cached_data)

    def test_cache_data_should_write_all_dict_fields(self):
        response_cacher._cache_data(0, 'cache', {'test': 'aaa', 'test1': {'test2': 2}}, self.db)
        cached_data = self.db['cache'].find_one({'import_id': 0}, {'import_id': 0, '_id': 0})
        self.assertIsNotNone(cached_data)
        self.assertEqual({'test': 'aaa', 'test1': {'test2': 2}}, cached_data)

    def test_get_cached_data_should_read_from_db(self):
        self.db['cache'].insert_one({'import_id': 0, 'test': 'aaa'})
        cached_data = response_cacher._get_cached_data(0, 'cache', self.db)
        self.assertEqual({'test': 'aaa'}, cached_data)

    def test_get_cached_data_should_return_None_when_no_cache(self):
        cached_data = response_cacher._get_cached_data(0, 'cache', self.db)
        self.assertIsNone(cached_data)

    def test_decorator_should_return_cached_data_when_present(self):
        lock = MongoLock(client=self.db.client, db=self.db.name)

        @response_cacher.cache_response('cache', self.db, lock)
        def f(import_id: int):
            pass

        self.db['cache'].insert_one({'import_id': 0, 'test': 'aaa'})
        response: Response = f(import_id=0)
        self.assertEqual(201, response.status_code)
        self.assertEqual({'test': 'aaa'}, response.json)

    def test_decorator_should_not_call_func_when_cached_data_present(self):
        self.db['cache'].insert_one({'import_id': 0, 'test': 'aaa'})
        f = MagicMock()
        lock = MongoLock(client=self.db.client, db=self.db.name)
        decorator = response_cacher.cache_response('cache', self.db, lock)
        wrap = decorator(f)
        wrap(import_id=0)
        f.assert_not_called()

    def test_decorator_should_return_function_response_not_cache_not_present(self):
        lock = MongoLock(client=self.db.client, db=self.db.name)

        @response_cacher.cache_response('cache', self.db, lock)
        def f(import_id: int):
            return Response(json.dumps({'import_id': import_id, 'test': 'aaa'}, ensure_ascii=False), 201,
                            mimetype='application/json; charset=utf-8')

        response = f(import_id=0)
        self.assertEqual(201, response.status_code)
        self.assertEqual({'import_id': 0, 'test': 'aaa'}, response.json)

    def test_decorator_should_cache_data_when_not_cache_not_present(self):
        lock = MongoLock(client=self.db.client, db=self.db.name)

        @response_cacher.cache_response('cache', self.db, lock)
        def f(import_id: int):
            return Response(json.dumps({'import_id': import_id, 'test': 'aaa'}, ensure_ascii=False), 201,
                            mimetype='application/json; charset=utf-8')

        with mock.patch('application.decorators.response_cacher._cache_data') as cache_mock:
            f(import_id=0)
            cache_mock.assert_called()

    def test_decorator_should_not_cache_when_exception_in_func(self):
        lock = MongoLock(client=self.db.client, db=self.db.name)

        @response_cacher.cache_response('cache', self.db, lock)
        def f(import_id: int):
            raise ValueError()

        with mock.patch('application.decorators.response_cacher._cache_data') as cache_mock:
            try:
                f(import_id=0)
            except ValueError:
                pass
            cache_mock.assert_not_called()
