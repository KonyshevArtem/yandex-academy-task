import json
import os
from functools import wraps

from flask import Response
from mongolock import MongoLock
from pymongo.database import Database


def _get_cached_data(import_id: int, collection_name: str, db: Database) -> dict:
    """
    Возвращает закешированные ранее данные из указанной поставки.

    Если закешированные данные отсутствуют, возвращается None.
    :param int import_id: уникальный идентификатор поставки
    :param str collection_name: имя коллекции, в которой находятся кешированные данные
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях

    :return: Закешированные данные
    :rtype: dict
    """
    cached_data = db[collection_name].find_one({'import_id': import_id}, {'_id': 0, 'import_id': 0})
    return cached_data


def _cache_data(import_id: int, collection_name: str, response_data: dict, db: Database):
    """
    Сохраняет данные, полученные из указанной поставки в базу данных.

    :param int import_id: уникальный идентификатор поставки
    :param str collection_name: имя коллекции, в которую производится запись
    :param dict response_data: данные для закеширования
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    """
    data = {'import_id': import_id}
    data = {**data, **response_data}
    db[collection_name].insert_one(data)


def cache_response(collection_name: str, db: Database, lock: MongoLock):
    """
    Декоратор, проверяющий наличие закешированных данных в указанной коллекции перед выполнением обработчика.

    При отсутсвии закешированных данных выполняет обработчик и сохраняет результат его работы в указанную коллекцию.
    :param str collection_name: имя коллекции, в которой находятся кешированные данные
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    :param MongoLock lock: объект для ограничения одновременного доступа к ресурсам из разных процессов
    """

    def decorator(f):
        @wraps(f)
        def wrap(*args, **kwargs):
            import_id = kwargs['import_id']
            with lock(f'{collection_name}_{import_id}', str(os.getpid()), expire=60, timeout=10):
                cached_data = _get_cached_data(import_id, collection_name, db)
                if cached_data is not None:
                    return Response(json.dumps(cached_data, ensure_ascii=False), 201,
                                    mimetype='application/json; charset=utf-8')
                response: Response = f(*args, **kwargs)
                _cache_data(import_id, collection_name, response.json, db)
                return response

        return wrap

    return decorator
