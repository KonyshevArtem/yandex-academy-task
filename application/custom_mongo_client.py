import logging

from pymongo import MongoClient, IndexModel
from pymongo.errors import PyMongoError, OperationFailure

logger = logging.getLogger(__name__)


def _initiate_replica_set(host: str, port: int):
    """Инициализирует replica set через новое подключение к узлу монго.

    :param str host: адрес узла монго, на котором инициируется replica set
    :param int port: порт, который прослушивает узел монго
    """
    client = MongoClient(host, port)
    try:
        client.admin.command('replSetInitiate')
    except PyMongoError:
        logger.info('Replica set already initiated')
    finally:
        client.close()


class CustomMongoClient(MongoClient):
    """Класс для подключения к базе данных монго и автоматической инициализации replica set."""

    def __init__(self, host: str, port: int, replica_set: str):
        super().__init__(host, port, replicaset=replica_set)
        _initiate_replica_set(host, port)

    def create_db_indexes(self, db_name: str):
        """
        Создает необходимые индексы в базе данных.

        :param str db_name: имя базы данных, в которой необходимо создать индексы
        """
        self._create_index(db_name, 'imports', IndexModel([('import_id', 1)], unique=True))
        self._create_index(db_name, 'imports', IndexModel([('citizens.citizen_id', 1)]))
        self._create_index(db_name, 'imports', IndexModel([('import_id', 1), ('citizens.citizen_id', 1)], unique=True))
        self._create_index(db_name, 'birthdays', IndexModel([('import_id', 1)], unique=True))

    def _create_index(self, db_name: str, collection_name: str, index: IndexModel):
        """
        Создает индекс в указанной коллекции указанной базы данных.

        При наличии индекса с таким же именем, но другими параметрами, удаляет имеющийся индекс и создает новый.
        :param str db_name: имя базы данных
        :param str collection_name: имя коллекции
        :param IndexModel index: создаваемый индекс
        """
        try:
            self[db_name][collection_name].create_indexes([index])
        except OperationFailure:
            self[db_name][collection_name].drop_index(index.document['name'])
            self[db_name][collection_name].create_indexes([index])
