import os
from datetime import datetime
from typing import Tuple

from mongolock import MongoLock
from pymongo.database import Database
from pymongo.errors import PyMongoError
from pymongo.results import InsertOneResult


def _parse_birth_date(import_data: dict):
    """
    Парсит поле birth_date у каждого жителя из строки в datetime.

    :param dict import_data: набор с данными о жителях
    """
    for citizen in import_data['citizens']:
        citizen['birth_date'] = datetime.strptime(citizen['birth_date'], '%d.%m.%Y')


def _add_import_id(import_data: dict, db: Database):
    """
    Добавляет в данные о жителях поле с уникальным идентификатором набора import_id.

    :param dict import_data: валидированный набор с данными о жителях
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    """
    import_id = db['imports'].count()
    import_data['import_id'] = import_id


def _write_to_db(import_data: dict, db: Database) -> Tuple[dict, int]:
    """
    Производит запись набора данных о жителях в базу данных.

    :param import_data: валидированный набор данных о жителях
    :param db: объект базы данных, в которую записываются наборы данных о жителях
    :raises: :class:`PyMongoError`: Операция записи в базу данных не была разрешена

    :returns: В случае успеха возвращается пару из ответа с идентификатором импорта и http кода 201
    :rtype: Tuple[dict, int]
    """
    db_response: InsertOneResult = db['imports'].insert_one(import_data)
    if db_response.acknowledged:
        response = {'data': {'import_id': import_data['import_id']}}
        return response, 201
    else:
        raise PyMongoError('Operation was not acknowledged')


def post_import(import_data: dict, lock: MongoLock, db: Database) -> Tuple[dict, int]:
    """
    Принимает на вход набор с данными о жителях в формате json
    и сохраняет его с уникальным идентификатором import_id.

    :param dict import_data: валидированный набор с данными о жителях
    :param MongoLock lock: объект для ограничения одновременного доступа к ресурсам из разных процессов
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    :raises: :class:`PyMongoError`: Операция записи в базу данных не была разрешена

    :returns: В случае успеха возвращается пару из ответа с идентификатором импорта и http кода 201
    :rtype: Tuple[dict, int]
    """
    _parse_birth_date(import_data)

    with lock('post_imports', str(os.getpid()), timeout=60, expire=10):
        _add_import_id(import_data, db)
        return _write_to_db(import_data, db)
