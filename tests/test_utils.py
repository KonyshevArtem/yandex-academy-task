import os
from typing import Tuple
from unittest.mock import MagicMock

from bson import json_util
from flask import Flask
from mongomock import MongoClient

from application.data_validator import DataValidator
from application.service import make_app


class MockMongoClient(MongoClient):
    """
    Фейковый класс подключения к монго.

    Добавляет механизм транзакций в mongomock, обходя исключения NotImplementedError.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        transaction = MagicMock()
        session_enter = MagicMock()
        session_enter.__bool__ = MagicMock(return_value=False)
        self.session = MagicMock()
        self.session.__enter__ = MagicMock(return_value=session_enter)
        self.session.start_transaction = MagicMock(return_value=transaction)

    def start_session(self):
        return self.session


def create_mock_validator() -> DataValidator:
    """
    Создает фейковый экземпляр класса DataValidator

    :return: фейковый экземпляр класса DataValidator
    :rtype: DataValidator
    """
    validator = DataValidator()
    validator.validate_import = MagicMock()
    validator.validate_citizen_patch = MagicMock()
    return validator


def read_data(filename: str) -> dict:
    """
    Считывает JSON из указанного файла.

    :param str filename: имя файла из которого нужно считать JSON

    :return: считанный объект
    :rtype: dict
    """
    with open(os.path.join(os.path.dirname(__file__), 'json', filename)) as f:
        import_data = json_util.loads(f.read())
    return import_data


def get_fake_db():
    """Создает экземпляр фейковой базы данных."""
    return MockMongoClient()['db']


def set_up_service() -> Tuple[Flask, MongoClient, DataValidator]:
    """
    Производит подготовку сервиса к тестированию.

    :return: Запущенный сервис, фейковый монго клиент, фейковый валидатор
    :rtype: Tuple[Flask, MongoClient, DataValidator]
    """
    db = get_fake_db()
    validator = create_mock_validator()
    app = make_app(db, validator).test_client()
    return app, db, validator
