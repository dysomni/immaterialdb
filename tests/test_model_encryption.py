import json
import logging
from decimal import Decimal
from typing import Optional

import pytest
from freezegun import freeze_time
from pydantic import BaseModel

from immaterialdb.config import RootConfig
from immaterialdb.constants import LOGGER
from immaterialdb.errors import RecordNotUniqueError
from immaterialdb.model import Model, QueryIndex, UniqueIndex, materialize_model
from immaterialdb.query import AllQuery, StandardQuery, StandardQueryStatement
from immaterialdb.testing import mock_immaterialdb

IMMATERIALDB = RootConfig("test_table")


@IMMATERIALDB.decorators.register_encryption
def encrypt_string(text: str) -> str:
    # base 64 for testing purposes - yes i know this is not encryption
    return text.encode("utf-8").hex()


@IMMATERIALDB.decorators.register_decryption
def decrypt_string(text: str) -> str:
    return bytes.fromhex(text).decode("utf-8")


@IMMATERIALDB.decorators.register_model(
    [
        QueryIndex(partition_fields=["name"], sort_fields=["age"]),
        UniqueIndex(unique_fields=["name"]),
    ],
    encrypted_fields=["my_secret"],
    auto_decrypt=True,
)
class MyModel(Model):
    name: str
    age: int
    my_secret: Optional[str]


@freeze_time("2021-01-01T00:00:00")
@mock_immaterialdb(IMMATERIALDB)
def test_encrypts_secrets_automatically():
    new_model = MyModel(id="temp", name="John", age=30, my_secret="password")
    new_model.save()

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    assert response["Count"] == 3
    assert response["Items"] == [
        {
            "node_type": "unique",
            "entity_name": "MyModel",
            "entity_id": "temp",
            "pk": "MyModel(name=John)",
            "sk": "unique",
            "unique_node_id": "temp",
            "fields": [["name", "John"]],
        },
        {
            "node_type": "query",
            "entity_name": "MyModel",
            "entity_id": "temp",
            "pk": "MyModel[name=John][age]",
            "sk": "##100000000000000000030##temp",
            "query_node_id": "temp",
            "partition_fields": [["name", "John"]],
            "sort_fields": [["age", Decimal("30")]],
            "raw_data": '{"id":"temp","created_at":"2021-01-01T00:00:00Z","updated_at":"2021-01-01T00:00:00Z","updated_hash":"9c45d0d88bfc8b91bddd5501719976d5","name":"John","age":30,"my_secret":"##encrypted##70617373776f7264"}',
        },
        {
            "node_type": "base",
            "entity_name": "MyModel",
            "entity_id": "temp",
            "pk": "temp",
            "sk": "temp",
            "base_node_id": "temp",
            "raw_data": '{"id":"temp","created_at":"2021-01-01T00:00:00Z","updated_at":"2021-01-01T00:00:00Z","updated_hash":"9c45d0d88bfc8b91bddd5501719976d5","name":"John","age":30,"my_secret":"##encrypted##70617373776f7264"}',
            "other_nodes": [
                ["MyModel[name=John][age]", "##100000000000000000030##temp"],
                ["MyModel(name=John)", "unique"],
            ],
        },
    ]


@freeze_time("2021-01-01T00:00:00")
@mock_immaterialdb(IMMATERIALDB)
def test_decrypts_secrets_automatically__get():
    new_model = MyModel(id="temp", name="John", age=30, my_secret="password")
    new_model.save()

    gotten_model = MyModel.get_by_id("temp")
    assert gotten_model.my_secret == "password"


@freeze_time("2021-01-01T00:00:00")
@mock_immaterialdb(IMMATERIALDB)
def test_decrypts_secrets_automatically__query():
    new_model = MyModel(id="temp", name="John", age=30, my_secret="password")
    new_model.save()

    query = MyModel.query(StandardQuery([StandardQueryStatement("name", "eq", "John")]))

    assert len(list(query.records)) == 1
    assert query.records[0].my_secret == "password"


@freeze_time("2021-01-01T00:00:00")
@mock_immaterialdb(IMMATERIALDB)
def test_encryption_skipped_if_not_string(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG, LOGGER.name)
    new_model = MyModel(id="temp", name="John", age=30, my_secret=None)
    new_model.save()

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    assert response["Count"] == 3
    assert response["Items"] == [
        {
            "node_type": "unique",
            "entity_name": "MyModel",
            "entity_id": "temp",
            "pk": "MyModel(name=John)",
            "sk": "unique",
            "unique_node_id": "temp",
            "fields": [["name", "John"]],
        },
        {
            "node_type": "query",
            "entity_name": "MyModel",
            "entity_id": "temp",
            "pk": "MyModel[name=John][age]",
            "sk": "##100000000000000000030##temp",
            "query_node_id": "temp",
            "partition_fields": [["name", "John"]],
            "sort_fields": [["age", Decimal("30")]],
            "raw_data": '{"id":"temp","created_at":"2021-01-01T00:00:00Z","updated_at":"2021-01-01T00:00:00Z","updated_hash":"510294a5f3754c23cbb18fc627dbe26a","name":"John","age":30,"my_secret":null}',
        },
        {
            "node_type": "base",
            "entity_name": "MyModel",
            "entity_id": "temp",
            "pk": "temp",
            "sk": "temp",
            "base_node_id": "temp",
            "raw_data": '{"id":"temp","created_at":"2021-01-01T00:00:00Z","updated_at":"2021-01-01T00:00:00Z","updated_hash":"510294a5f3754c23cbb18fc627dbe26a","name":"John","age":30,"my_secret":null}',
            "other_nodes": [
                ["MyModel[name=John][age]", "##100000000000000000030##temp"],
                ["MyModel(name=John)", "unique"],
            ],
        },
    ]

    gotten_model = MyModel.get_by_id("temp")
    assert gotten_model.my_secret is None

    assert "Field my_secret is not a string, skipping encryption" in caplog.text
    assert "Field my_secret is not a string, skipping decryption" in caplog.text
