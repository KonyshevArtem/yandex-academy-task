import unittest
from datetime import datetime

import application.handlers.get_birthdays_handler as get_birthdays_handler


class GetBirthdaysHandler(unittest.TestCase):
    def test_get_birthdays_should_return_empty_dict_when_citizens_empty(self):
        birthdays_data = get_birthdays_handler._get_birthdays_data([])
        self.assertEqual({}, birthdays_data)

    def test_get_birthdays_should_return_empty_when_no_relatives(self):
        citizens = [{'citizen_id': 0, 'relatives': []}, {'citizen_id': 1, 'relatives': []}]
        birthdays_data = get_birthdays_handler._get_birthdays_data(citizens)
        self.assertEqual({}, birthdays_data)

    def test_get_birthdays_when_one_relative(self):
        citizens = [{'citizen_id': 0, 'birth_date': datetime(2019, 2, 1), 'relatives': [1]},
                    {'citizen_id': 1, 'birth_date': datetime(2019, 3, 1), 'relatives': [0]}]
        birthdays_data = get_birthdays_handler._get_birthdays_data(citizens)
        self.assertEqual({2: {1: 1}, 3: {0: 1}}, birthdays_data)

    def test_get_birthdays_when_multiple_relatives(self):
        citizens = [{'citizen_id': 0, 'birth_date': datetime(2019, 2, 1), 'relatives': [1, 2]},
                    {'citizen_id': 1, 'birth_date': datetime(2019, 3, 1), 'relatives': [0]},
                    {'citizen_id': 2, 'birth_date': datetime(2019, 3, 1), 'relatives': [0, 1]}]
        birthdays_data = get_birthdays_handler._get_birthdays_data(citizens)
        self.assertEqual({2: {1: 1, 2: 1}, 3: {0: 2, 1: 1}}, birthdays_data)

    def test_get_representation_should_return_empty_when_empty_birthdays(self):
        birthday_representation = get_birthdays_handler._get_birthdays_representation({})
        self.assertEqual({'data': {str(i): [] for i in range(1, 13)}}, birthday_representation)

    def test_get_representation_when_birthdays_not_empty(self):
        birthdays_data = {2: {1: 1, 2: 1}, 3: {0: 2, 1: 1}}
        birthday_representation = get_birthdays_handler._get_birthdays_representation(birthdays_data)
        expected_representation = {'data': {str(i): [] for i in range(1, 13)}}
        expected_representation['data']['2'] = [{'citizen_id': 1, 'presents': 1}, {'citizen_id': 2, 'presents': 1}]
        expected_representation['data']['3'] = [{'citizen_id': 0, 'presents': 2}, {'citizen_id': 1, 'presents': 1}]
        self.assertEqual(expected_representation, birthday_representation)
