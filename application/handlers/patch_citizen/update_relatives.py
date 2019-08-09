from typing import Set, Tuple, List

from pymongo import UpdateMany
from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.errors import PyMongoError
from pymongo.results import BulkWriteResult


def _make_update_relatives_request(operation: str, import_id: int, citizen_id: int, relatives_ids: List[int]):
    """
    Создает объект запроса к базе данных для изменения
    списка уникальных идентификаторов родственников у нескольких жителей.

    В запросе происходит вставка/удаление citizen_id в/из списка родственников у всех жителей данной в данной
    поставке, индекс которых находится в relative_ids.
    :param str operation: Имя операции - $push или $pull для вставки и удаления родственника соответственно
    :param int import_id: Уникальный идентификатор поставки, в которой модифицируется житель
    :param int citizen_id: Уникальный идентификатор модифицируемого жителя
    :param List[int] relatives_ids: Уникальные идентификаторы жителей, для которых будет производится
        операция вставки или удаления
    :raises: :class:`ValueError`: Операция не является $push или $pull.

    :return: Объект запроса к базе данных для обновления нескольких документов
    :rtype: UpdateMany
    """
    if operation not in ['$push', '$pull']:
        raise ValueError(f'Operation {operation} is not valid operation')
    return UpdateMany({'import_id': import_id},
                      {operation: {'citizens.$[element].relatives': citizen_id}},
                      array_filters=[{'element.citizen_id': {'$in': relatives_ids}}])


def _make_db_requests(to_push: Set[int], to_pull: Set[int], import_id: int, citizen_id: int) -> List[UpdateMany]:
    """
    Создает запросы для вставки/удаления указанного citizen_id из поля relatives у всех жителей, чей индекс входит в
    to_push/to_pull в поставке с уникальным идентификатором import_id

    :param Set[int] to_push: Сет идентификаторов жителей, у которых нужно добавить citizen_id в relatives
    :param Set[int] to_pull: Сет идентификаторов жителей, у которых нужно удалить citizen_id из relatives
    :param int import_id: уникальный идентификатор поставки
    :param int citizen_id: уникальный идентификатор жителя

    :return: Список из запросов к базе данных на обновление множества документов
    :rtype: List[UpdateMany]
    """
    db_requests = []
    for operation, relatives in zip(['$push', '$pull'], [to_push, to_pull]):
        if relatives:
            db_requests.append(_make_update_relatives_request(operation, import_id, citizen_id, list(relatives)))
    return db_requests


def _get_relatives(citizen_id: int, import_id: int, db: Database, session: ClientSession) -> Set[int]:
    """
    Возвращает сет родственников указанного жителя в указанной поставке.

    :param int citizen_id: Уникальный идентификатор жителя, чьих родственников необходимо вернуть
    :param int import_id: Уникальный индетификатор поставки, в которой ищется житель
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    :param ClientSession session: сессия соединения с базой данных, через которую производятся все запросы
    :raises: :class:`PyMongoError`: Объект с указанным уникальным идентификатором не был найден в базе данных

    :return: сет родственников указанного жителя в указанной поставке
    :rtype: Set[int]
    """
    db_response: dict = db['imports'].find_one({'import_id': import_id, 'citizens.citizen_id': citizen_id},
                                               {'citizens': {'$elemMatch': {'citizen_id': citizen_id}}},
                                               session=session)
    if db_response is None:
        raise PyMongoError('Import or citizen with specified id not found')
    relatives = set(db_response['citizens'][0]['relatives'])
    return relatives


def _get_relatives_difference(old_relatives: Set[int], patch_data: dict) -> Tuple[Set[int], Set[int]]:
    """
    Находит уникальные идентификаторы родственников, у которых нужно добавить/убрать из поля relatives
    уникальный идентификатор модифицируемого жителя

    :param Set[int] old_relatives: старый список родственников модифицируемого жителя
    :param dict patch_data: новая информация о модифицирумом жителе

    :return: Пара сетов родственников, у которых у которых нужно добавить/убрать из поля relatives
    уникальный идентификатор модифицируемого жителя
    :rtype Tuple[Set[int], Set[int]]
    """
    new_relatives = set(patch_data['relatives'])
    to_push = new_relatives - old_relatives
    to_pull = old_relatives - new_relatives
    return to_push, to_pull


def _check_all_citizens_exist(citizens_ids: Set[int], import_id: int, db: Database, session: ClientSession):
    """
    Проверяет наличие всех жителей, указанных в relatives_ids в поставке с идентификатором import_id.

    :param Set[int] citizens_ids: Уникальные идентификаторы жителей
    :param int import_id: Уникальный идентификатор поставки
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    :param ClientSession session: сессия соединения с базой данных, через которую производятся все запросы
    :raises: :class:`PyMongoError`: Объект с указанным уникальным идентификатором не был найден в базе данных
    """
    if not citizens_ids:
        return

    count = db['imports'].count_documents(
        {'import_id': import_id, 'citizens.citizen_id': {'$all': list(citizens_ids)}}, session=session, limit=1)
    if count == 0:
        raise PyMongoError('Citizens with specified id not found')


def _write_relatives_update(db_requests: List[UpdateMany], db: Database, session: ClientSession):
    """
    При наличии запросов в db_request производит их запись в базу данных.

    :param List[UpdateMany] db_requests: Список запросов к базе данных на обновление множества документов
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    :param ClientSession session: сессия соединения с базой данных, через которую производятся все запросы
    :raises: :class:`PyMongoError`: Объект с указанным уникальным идентификатором не был найден в базе данных
    """
    if db_requests:
        bulk_response: BulkWriteResult = db['imports'].bulk_write(db_requests, session=session)
        if bulk_response.modified_count != len(db_requests):
            raise PyMongoError('Import with specified id not found')


def update_relatives(citizen_id: int, import_id: int, patch_data: dict, db: Database, session: ClientSession):
    """
    При наличии поля relatives в patch_data производит обновление поля relatives
    у всех родственников обновляемого жителя.

    :param int citizen_id: Уникальный индентификатор обновляемого жителя
    :param int import_id: Уникальный идентификатор поставки, в которой обновляется житель
    :param dict patch_data: Новые данные жителя
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    :param ClientSession session: сессия соединения с базой данных, через которую производятся все запросы
    """
    if 'relatives' not in patch_data:
        return

    old_relatives = _get_relatives(citizen_id, import_id, db, session)
    to_push, to_pull = _get_relatives_difference(old_relatives, patch_data)
    _check_all_citizens_exist(to_push, import_id, db, session)
    db_requests = _make_db_requests(to_push, to_pull, import_id, citizen_id)
    _write_relatives_update(db_requests, db, session)
