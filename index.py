import configparser
from flask import Flask, request
from pymongo import MongoClient


def make_app(db: MongoClient) -> Flask:
    app = Flask(__name__)

    @app.route('/imports', methods=['POST'])
    def imports():
        if request.content_type != 'application/json':
            return 'Content-Type must be application/json', 400

        import_data = request.get_json()
        import_id = db['imports'].count()
        import_data['import_id'] = import_id

        db_response = db['imports'].insert_one(import_data)
        if db_response.acknowledged:
            response = {"data": {'import_id': import_id}}
            return response, 201
        return 400

    return app


def main():
    config = configparser.ConfigParser()
    config.read('config.cfg')
    db_uri = config['DATABASE']['DATABASE_URI']
    db_name = config['DATABASE']['DATABASE_NAME']

    db = MongoClient(db_uri)[db_name]
    app = make_app(db)
    app.run(host='0.0.0.0', port=8080)


if __name__ == '__main__':
    main()
