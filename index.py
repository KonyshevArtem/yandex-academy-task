import configparser
import os
from collections import defaultdict
from multiprocessing import Lock

from flask import Flask, request
from pymongo import MongoClient, ReturnDocument, UpdateMany
from pymongo.database import Database
from pymongo.errors import PyMongoError
from pymongo.results import InsertOneResult, BulkWriteResult
from werkzeug.exceptions import BadRequest

from data_validator import DataValidator
from exception_handler import handle_exceptions


def make_app(db: Database, data_validator: DataValidator) -> Flask:
    app = Flask(__name__)

    locks = defaultdict(Lock)

    @app.route('/imports', methods=['POST'])
    @handle_exceptions
    def imports():
        if not request.is_json:
            raise BadRequest('Content-Type must be application/json')

        import_data = request.get_json()
        data_validator.validate_import(import_data)

        with locks['post_imports']:
            import_id = db['imports'].count()
            import_data['import_id'] = import_id

            db_response: InsertOneResult = db['imports'].insert_one(import_data)
            if db_response.acknowledged:
                response = {'data': {'import_id': import_id}}
                return response, 201
            else:
                raise PyMongoError('Operation was not acknowledged')

    @app.route('/imports/<int:import_id>/citizens/<int:citizen_id>', methods=['PATCH'])
    @handle_exceptions
    def citizen(import_id: int, citizen_id: int):

        def make_update_relative_request(operation: str, relative_ids: list):
            return UpdateMany({'import_id': import_id},
                              {operation: {'citizens.$[element].relatives': citizen_id}},
                              array_filters=[{'element.citizen_id': {'$in': relative_ids}}])

        if not request.is_json:
            raise BadRequest('Content-Type must be application/json')

        patch_data = request.get_json()
        data_validator.validate_citizen_patch(citizen_id, patch_data)

        with locks[str(import_id)]:
            if 'relatives' in patch_data:
                old_relatives_response: dict = db['imports'].find_one({'import_id': import_id},
                                                                      {'citizens': {
                                                                          '$elemMatch': {
                                                                              'citizen_id': citizen_id}}})
                if old_relatives_response is None:
                    raise PyMongoError('Import or citizen with specified id not found')
                old_relatives = set(old_relatives_response['citizens'][0]['relatives'])
                new_relatives = set(patch_data['relatives'])
                to_push = new_relatives - old_relatives
                to_pull = old_relatives - new_relatives
                db_requests = []
                if to_push:
                    db_requests.append(make_update_relative_request('$push', list(to_push)))
                if to_pull:
                    db_requests.append(make_update_relative_request('$pull', list(to_pull)))
                if db_requests:
                    bulk_response: BulkWriteResult = db['imports'].bulk_write(db_requests)
                    if bulk_response.modified_count != len(db_requests):
                        raise PyMongoError('Relative with specified id not found')

            update_data = {
                '$set': {f'citizens.$.{key}': val for key, val in patch_data.items()}
            }
            projection = {
                '_id': 0,
                'import_id': 0,
                'citizens': {
                    '$elemMatch': {'citizen_id': citizen_id}
                }
            }
            db_response: dict = db['imports'].find_one_and_update(
                filter={'import_id': import_id, 'citizens.citizen_id': citizen_id}, update=update_data,
                projection=projection, return_document=ReturnDocument.AFTER)
            if db_response is None:
                raise PyMongoError('Import or citizen with specified id not found')
            return {'data': db_response['citizens'][0]}, 201

    @app.route('/imports/<int:import_id>/citizens', methods=['GET'])
    @handle_exceptions
    def citizens(import_id: int):
        with locks[str(import_id)]:
            import_data = db['imports'].find_one({'import_id': import_id}, {'_id': 0, 'import_id': 0})
            if import_data is None:
                raise PyMongoError('Import with specified id not found')
            for citizen in import_data['citizens']:
                citizen['birth_date'] = citizen['birth_date'].strftime('%d.%m.%Y')
            return {'data': import_data['citizens']}, 201

    return app


def main():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.cfg')
    config.read(config_path)
    db_uri = config['DATABASE']['DATABASE_URI']
    db_name = config['DATABASE']['DATABASE_NAME']

    db = MongoClient(db_uri)[db_name]
    data_validator = DataValidator()
    app = make_app(db, data_validator)
    app.run(host='0.0.0.0', port=8080)


if __name__ == '__main__':
    main()
