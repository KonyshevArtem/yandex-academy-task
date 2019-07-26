import configparser
import logging
import os
from typing import Tuple

from flask import Flask, request
from jsonschema.exceptions import ValidationError
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.results import InsertOneResult
from werkzeug.exceptions import BadRequest

from data_validator import DataValidator


def make_app(db: MongoClient, data_validator: DataValidator) -> Flask:
    app = Flask(__name__)

    def make_error_response(message: str, status_code: int) -> Tuple[dict, int]:
        app.logger.error(message)
        return {'message': message}, status_code

    @app.route('/imports', methods=['POST'])
    def imports():
        if not request.is_json:
            return make_error_response('Content-Type must be application/json', 400)

        try:
            import_data = request.get_json()
            data_validator.validate_import(import_data)

            import_id = db['imports'].count()
            import_data['import_id'] = import_id

            db_response: InsertOneResult = db['imports'].insert_one(import_data)
            if db_response.acknowledged:
                response = {"data": {'import_id': import_id}}
                return response, 201
            else:
                return make_error_response('Operation was not acknowledged', 400)
        except ValidationError as e:
            return make_error_response('Import data is not valid: ' + str(e), 400)
        except BadRequest as e:
            return make_error_response('Error when parsing JSON: ' + str(e), 400)
        except PyMongoError as e:
            return make_error_response('Database error: ' + str(e), 400)
        except Exception as e:
            return make_error_response(str(e), 400)

    return app


def main():
    logging.basicConfig(filename='logs/service.log')

    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.cfg')
    if not os.path.exists(config_path):
        raise FileNotFoundError(config_path)
    config.read(config_path)
    db_uri = config['DATABASE']['DATABASE_URI']
    db_name = config['DATABASE']['DATABASE_NAME']

    db = MongoClient(db_uri)[db_name]
    data_validator = DataValidator()
    app = make_app(db, data_validator)
    app.run(host='0.0.0.0', port=8080)


if __name__ == '__main__':
    main()
