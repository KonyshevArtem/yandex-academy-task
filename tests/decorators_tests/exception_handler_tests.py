import logging
import unittest
from unittest import mock
from unittest.mock import MagicMock

from nose_parameterized import parameterized

from application.decorators import exception_handler


class ExceptionHandlerResponse(unittest.TestCase):
    def setUp(cls) -> None:
        cls.logger = logging.Logger(__file__)
        cls.logger.exception = MagicMock()

    @parameterized.expand([
        ['test1', 201],
        ['test2', 400]
    ])
    def test_make_error_response_should_return_passed_message_and_status(self, message: str, status_code: int):
        data, status = exception_handler._make_error_response(self.logger, message, status_code)
        self.assertEqual({'message': message}, data)
        self.assertEqual(status_code, status)

    def test_make_error_response_should_log_exception(self):
        exception_handler._make_error_response(self.logger, 'test', 400)
        self.logger.exception.assert_called()

    def test_decorator_should_return_function_result_when_no_exception(self):
        @exception_handler.handle_exceptions(self.logger)
        def f():
            return 2

        result = f()
        self.assertEqual(2, result)

    def test_decorator_return_error_response_when_exception_raised(self):
        @exception_handler.handle_exceptions(self.logger)
        def f():
            raise Exception('test')

        with mock.patch('application.decorators.exception_handler._make_error_response', return_value=1):
            result = f()
            self.assertEqual(1, result)
