import unittest

from pymongo.errors import PyMongoError

from application.handlers import shared
from tests import test_utils


class SharedTests(unittest.TestCase):
    def test_get_citizens_should_return_data_if_found(self):
        db = test_utils.get_fake_db()
        db['imports'].insert_one({'import_id': 0, 'citizens': [{'birth_date': 0, 'relatives': []}]})
        citizens = shared.get_citizens(0, db)
        self.assertEqual([{'birth_date': 0, 'relatives': []}], citizens)

    def test_get_citizens_should_raise_if_not_found(self):
        db = test_utils.get_fake_db()
        with self.assertRaises(PyMongoError):
            shared.get_citizens(0, db)

    def test_get_citizens_should_select_with_projection(self):
        db = test_utils.get_fake_db()
        db['imports'].insert_one({'import_id': 0, 'citizens': [{'citizen_id': 0, 'birth_date': 0, 'relatives': []}]})
        citizens = shared.get_citizens(0, db, {'citizens.citizen_id': 0})
        self.assertEqual([{'birth_date': 0, 'relatives': []}], citizens)
