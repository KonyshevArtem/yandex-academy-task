import os
from collections import defaultdict
from datetime import datetime
from typing import Tuple, List

import numpy as np
from mongolock import MongoLock
from pymongo.database import Database

from application.handlers import shared


def _get_cached_percentile_age(import_id: int, db: Database) -> dict:
    """
    Возвращает закешированные ранее данные о возрастах из указанной поставки.

    Если закешированные данные отсутствуют, возвращается None.
    :param int import_id: уникальный идентификатор поставки
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях

    :return: Данные о возрастах
    :rtype: dict
    """
    percentile_age_data = db['percentile_age'].find_one({'import_id': import_id}, {'_id': 0, 'import_id': 0})
    return percentile_age_data


def _calculate_age(citizens: List[dict]):
    """
    Вычисляет возраст всех жителей в переданном списке.

    :param List[dict] citizens: список жителей
    """
    days_in_year = 365.2425
    for citizen in citizens:
        citizen['age'] = int((datetime.now() - citizen['birth_date']).days / days_in_year)


def _group_by_town(citizens: List[dict]) -> dict:
    """
    Группирует возраст жителей по городам.

    :param dict citizens: список жителей

    :return: возрасты всех жителей, сгруппированные по городам
    :rtype: dict
    """
    grouped = defaultdict(list)
    for citizen in citizens:
        grouped[citizen['town']].append(citizen['age'])
    return grouped


def _calculate_percentile(grouped: dict):
    """
    Вычисляет процентили p50, p75, p99 по возрастам жителей в городах

    :param dict grouped: возраста жителей, сгруппированные по городам
    """
    for town in grouped:
        grouped[town] = [int(p) for p in np.percentile(grouped[town], [50, 75, 99], interpolation='linear')]


def _get_percentiles_representation(percentiles_data: dict) -> dict:
    """
    Преобразует данные о возрасте в формат для отправки ответа.

    :param dict percentiles_data: данные о возрасте

    :return: Данные о возрасте в формате для отправки
    :rtype: dict
    """
    representation = {'data': [{'town': town, 'p50': percentiles_data[town][0], 'p75': percentiles_data[town][1],
                                'p99': percentiles_data[town][2]} for town in percentiles_data]}
    return representation


def _cache_percentile_age_data(import_id: int, percentile_age_data: dict, db: Database):
    """
    Сохраняет данные о возрастах в указанной поставке в базу данных.

    :param dict percentile_age_data: данные о возрастах
    :param int import_id: уникальный идентификатор поставки
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    """
    data = {'import_id': import_id, 'data': percentile_age_data['data']}
    db['percentile_age'].insert_one(data)


def get_percentile_age(import_id: int, db: Database, lock: MongoLock) -> Tuple[dict, int]:
    """
    Возвращает статистику по городам для указанного набора данных в разрезе возраста (полных лет) жителей:
    p50, p75, p99, где число - это значение перцентиля.

    :param int import_id: уникальный идентификатор поставки
    :param Database db: объект базы данных, в которую записываются наборы данных о жителях
    :param MongoLock lock: объект для ограничения одновременного доступа к ресурсам из разных процессов

    :return: статистика по городам в разрезе возраста и http статус
    :rtype: Tuple[dict, int]
    """
    with lock(str(import_id), str(os.getpid()), expire=60, timeout=10), \
         lock(f'percentile_age_{import_id}', str(os.getpid()), expire=60, timeout=10):
        cached_percentile_age = _get_cached_percentile_age(import_id, db)
        if cached_percentile_age is not None:
            return cached_percentile_age, 201
        citizens = shared.get_citizens(import_id, db, {'citizens.birth_date': 1, 'citizens.town': 1})
        _calculate_age(citizens)
        grouped = _group_by_town(citizens)
        _calculate_percentile(grouped)
        percentiles_data = _get_percentiles_representation(grouped)
        _cache_percentile_age_data(import_id, percentiles_data, db)
        return percentiles_data, 201
