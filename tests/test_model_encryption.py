import json
import logging
from decimal import Decimal
from typing import Optional

import pytest
from freezegun import freeze_time
from pydantic import BaseModel

from immaterialdb.config import RootConfig
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
            "raw_data": '{"id":"temp","created_at":"2021-01-01T00:00:00Z","updated_at":"2021-01-01T00:00:00Z","updated_hash":"38336b3e45cbaf3f7659381173ac13c0","name":"John","age":30,"my_secret":"70617373776f7264"}',
        },
        {
            "node_type": "base",
            "entity_name": "MyModel",
            "entity_id": "temp",
            "pk": "temp",
            "sk": "temp",
            "base_node_id": "temp",
            "raw_data": '{"id":"temp","created_at":"2021-01-01T00:00:00Z","updated_at":"2021-01-01T00:00:00Z","updated_hash":"38336b3e45cbaf3f7659381173ac13c0","name":"John","age":30,"my_secret":"70617373776f7264"}',
            "other_nodes": [
                ["MyModel[name=John][age]", "##100000000000000000030##temp"],
                ["MyModel(name=John)", "unique"],
            ],
        },
    ]
