import os

from mongolock import MongoLock

from application.data_validator import DataValidator
from application.custom_mongo_client import CustomMongoClient
from application.service import make_app

db_uri = os.environ['DATABASE_URI']
port = int(os.environ['DATABASE_PORT'])
db_name = os.environ['DATABASE_NAME']
replica_set = os.environ['REPLICA_SET']

client = CustomMongoClient(db_uri, port, replica_set)
lock = MongoLock(client=client, db=db_name)
with lock('indexes', str(os.getpid()), timeout=60, expire=10):
    client.create_db_indexes(db_name)
db = client[db_name]
data_validator = DataValidator()
app = make_app(db, data_validator, lock)

if __name__ == '__main__':
    app.run()
