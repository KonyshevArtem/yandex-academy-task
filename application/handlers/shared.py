from typing import List

from pymongo.database import Database
from pymongo.errors import PyMongoError


def get_citizens(import_id: int, db: Database, projection: dict = None) -> List[dict]:
    """
    Возвращает список жителей в указанной поставке, выбранный с указанной проекцией.

    :param int import_id: уникальный идентификатор поставки
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    :param dict projection: словарь проекции выборки
    :raises :class:`PyMongoError`: Поставка с указанным уникальным идентификатором остутствует в базе данных

    :return: Список жителей
    :rtype: List[dict]
    """
    import_data = db['imports'].find_one({'import_id': import_id}, projection)
    if import_data is None:
        raise PyMongoError('Import with specified id not found')
    return import_data['citizens']
