import os
import configparser
from flask import Flask, request
from pymongo import MongoClient
from pymongo.results import InsertOneResult
from pymongo.errors import PyMongoError
from werkzeug.exceptions import BadRequest


def make_app(db: MongoClient) -> Flask:
    app = Flask(__name__)

    @app.route('/imports', methods=['POST'])
    def imports():
        if not request.is_json:
            return 'Content-Type must be application/json', 400

        try:
            import_data = request.get_json()
            import_id = db['imports'].count()
            import_data['import_id'] = import_id

            db_response: InsertOneResult = db['imports'].insert_one(import_data)
            if db_response.acknowledged:
                response = {"data": {'import_id': import_id}}
                return response, 201
            else:
                return 'Operation was not acknowledged', 400
        except BadRequest as e:
            return 'Error when parsing JSON: ' + str(e), 400
        except PyMongoError as e:
            return 'Database error: ' + str(e), 400
        except Exception as e:
            return str(e), 400

    return app


def main():
    config = configparser.ConfigParser()
    if not os.path.exists('config.cfg'):
        raise FileNotFoundError('config.cfg')
    config.read('config.cfg')
    db_uri = config['DATABASE']['DATABASE_URI']
    db_name = config['DATABASE']['DATABASE_NAME']

    db = MongoClient(db_uri)[db_name]
    app = make_app(db)
    app.run(host='0.0.0.0', port=8080)


if __name__ == '__main__':
    main()
