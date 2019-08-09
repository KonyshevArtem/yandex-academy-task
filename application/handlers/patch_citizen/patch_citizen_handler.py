import os
from datetime import datetime
from typing import Tuple

from mongolock import MongoLock
from pymongo import ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.errors import PyMongoError

from application.handlers.patch_citizen.update_relatives import update_relatives


def _parse_birth_date(patch_data: dict):
    """
    При наличии поля birth_date, парсит его из строки в datetime.

    :param dict patch_data: Новая информация о жителе
    """
    if 'birth_date' in patch_data:
        patch_data['birth_date'] = datetime.strptime(patch_data['birth_date'], '%d.%m.%Y')


def _write_citizen_update(citizen_id: int, import_id: int, patch_data: dict, db: Database,
                          session: ClientSession) -> dict:
    """
    Записывает обновление информации о жителе в базу данных.

    :param int citizen_id: Уникальный идентификатор модифицируемого жителя
    :param int import_id: Уникальный идентификатор поставки
    :param dict patch_data: Новая информация о жителе
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    :param ClientSession session: сессия соединения с базой данных, через которую производятся все запросы
    :raises: :class:`PyMongoError`: Объект с указанным уникальным идентификатором не был найден в базе данных

    :return: Обновленная информация о жителе
    :rtype: dict
    """
    update_data = {'$set': {f'citizens.$.{key}': val for key, val in patch_data.items()}}
    projection = {'_id': 0, 'import_id': 0, 'citizens': {'$elemMatch': {'citizen_id': citizen_id}}}

    db_response: dict = db['imports'].find_one_and_update(
        filter={'import_id': import_id, 'citizens.citizen_id': citizen_id}, update=update_data,
        projection=projection, return_document=ReturnDocument.AFTER, session=session)

    if db_response is None:
        raise PyMongoError('Import or citizen with specified id not found')
    return db_response


def _get_citizen_data(db_response: dict) -> dict:
    """
    Извлекает информацию о жителе из полученной из базы данных информации о поставке.

    Так же преобразует birth_date из datetime в строку
    :param dict db_response: информация о поставке из базы данных

    :return: Информация о жителе
    :rtype: dict
    """
    citizen_data = db_response['citizens'][0]
    birth_date = citizen_data['birth_date']
    citizen_data['birth_date'] = birth_date.strftime('%d.%m.%Y')
    return citizen_data


def patch_citizen(import_id: int, citizen_id: int, patch_data: dict, lock: MongoLock, db: Database) -> Tuple[dict, int]:
    """
    Изменяет информацию о жителе в указанном наборе данных.
    На вход подается JSON в котором можно указать любые данные о жителе.

    :param int import_id: Уникальный идентификатор поставки, в которой изменяется информация о жителе
    :param int citizen_id: Уникальный индентификатор жителя в поставке
    :param dict patch_data: Новая информация о жителе
    :param MongoLock lock: объект для ограничения одновременного доступа к ресурсам из разных процессов
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях

    :return: Пара из актуальной информации о жителе и http статуса
    :rtype: Tuple[dict, int]
    """
    _parse_birth_date(patch_data)

    with db.client.start_session() as session, \
            session.start_transaction(), \
            lock(str(import_id), str(os.getpid()), expire=60, timeout=10):
        update_relatives(citizen_id, import_id, patch_data, db, session)

        db_response = _write_citizen_update(citizen_id, import_id, patch_data, db, session)
        return {'data': _get_citizen_data(db_response)}, 201
