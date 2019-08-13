import logging
from functools import wraps
from typing import Tuple

from jsonschema import ValidationError
from pymongo.errors import PyMongoError
from werkzeug.exceptions import BadRequest


def _make_error_response(logger: logging.Logger, message: str, status_code: int) -> Tuple[dict, int]:
    """
    Логирует ошибку и возвращает пару из объекта, содержащего сообщение, и кода ошибки

    :param logging.Logger logger: логгер, которым логируется ошибка
    :param str message: Сообщение, поясняющее ошибку
    :param int status_code: HTTP код ошибки
    :return: Пара из объекта, содержащего сообщение, и кода ошибки
    :rtype: Tuple[dict, int]
    """
    logger.exception(message)
    return {'message': message}, status_code


def handle_exceptions(logger: logging.Logger):
    """
    Декоратор, обворачивающий указанную функцию в блок обработки ошибок.

    Логирует все появивишиеся ошибки с помощью логгера, переданного на вход.
    :param logging.Logger logger: логгер, которым логируется возникающие ошибки
    """

    def decorator(f):
        @wraps(f)
        def wrap(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except ValidationError as e:
                return _make_error_response(logger, 'Input data is not valid: ' + str(e), 400)
            except BadRequest as e:
                return _make_error_response(logger, 'Error when parsing JSON: ' + str(e), 400)
            except PyMongoError as e:
                return _make_error_response(logger, 'Database error: ' + str(e), 400)
            except ValueError as e:
                return _make_error_response(logger, 'Value error: ' + str(e), 400)
            except Exception as e:
                return _make_error_response(logger, str(e), 400)

        return wrap

    return decorator
