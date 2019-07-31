import logging
import os
from functools import wraps
from typing import Tuple

from jsonschema import ValidationError
from pymongo.errors import PyMongoError
from werkzeug.exceptions import BadRequest

log_path = os.path.join(os.path.dirname(__file__), 'logs', 'service.log')
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logging.basicConfig(filename=log_path, format=f'[%(asctime)s] %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)


def make_error_response(message: str, status_code: int) -> Tuple[dict, int]:
    logger.exception(message)
    return {'message': message}, status_code


def handle_exceptions(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValidationError as e:
            return make_error_response('Input data is not valid: ' + str(e), 400)
        except BadRequest as e:
            return make_error_response('Error when parsing JSON: ' + str(e), 400)
        except PyMongoError as e:
            return make_error_response('Database error: ' + str(e), 400)
        except Exception as e:
            return make_error_response(str(e), 400)

    return wrap
