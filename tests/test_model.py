import json
import logging
from decimal import Decimal

import pytest
from pydantic import BaseModel

from immaterialdb.config import RootConfig
from immaterialdb.errors import RecordNotUniqueError
from immaterialdb.model import Model, QueryIndex, UniqueIndex, materialize_model
from immaterialdb.query import AllQuery, StandardQuery, StandardQueryStatement
from immaterialdb.testing import mock_immaterialdb

IMMATERIALDB = RootConfig("test_table")


@IMMATERIALDB.decorators.register_model(
    [QueryIndex(partition_fields=["name"], sort_fields=["age"]), UniqueIndex(unique_fields=["name"])]
)
class MyModel(Model):
    name: str
    age: int
    money: Decimal


def test_adding_model():
    assert MyModel.immaterial_model_name() in IMMATERIALDB.registered_models


@mock_immaterialdb(IMMATERIALDB)
def test_model_save_and_get():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"))
    new_model.save()

    gotten_model = MyModel.get_by_id(new_model.id)
    assert gotten_model
    assert gotten_model == new_model

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    assert response["Count"] == 3
    expected_nodes = [json.loads(node.model_dump_json()) for node in materialize_model(new_model)]
    assert response["Items"][0] in expected_nodes
    assert response["Items"][1] in expected_nodes
    assert response["Items"][2] in expected_nodes


@mock_immaterialdb(IMMATERIALDB)
def test_model_update():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"))
    new_model.save()

    updated_model = new_model.copy()
    updated_model.age = 31
    updated_model.save()

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    expected_nodes = [json.loads(node.model_dump_json()) for node in materialize_model(updated_model)]
    assert response["Count"] == 3
    assert response["Items"][0] in expected_nodes
    assert response["Items"][1] in expected_nodes
    assert response["Items"][2] in expected_nodes


@mock_immaterialdb(IMMATERIALDB)
def test_model_unique_index():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"))
    new_model.save()

    duplicate_name_model = MyModel(name="John", age=-234, money=Decimal("-3424.00"))
    with pytest.raises(RecordNotUniqueError) as error:
        duplicate_name_model.save()

    assert str(error.value) == "Record already exists with unique key MyModel(name=John)"

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    expected_nodes = [json.loads(node.model_dump_json()) for node in materialize_model(new_model)]
    assert response["Count"] == 3
    assert response["Items"][0] in expected_nodes
    assert response["Items"][1] in expected_nodes
    assert response["Items"][2] in expected_nodes


@mock_immaterialdb(IMMATERIALDB)
def test_model_query():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"))
    new_model.save()

    response = MyModel.query(StandardQuery(statements=[StandardQueryStatement("name", "eq", "John")]))
    assert response.records[0] == new_model


@mock_immaterialdb(IMMATERIALDB)
def test_model_query_all():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"))
    new_model.save()

    response = MyModel.query(AllQuery())
    assert response.records[0] == new_model


@mock_immaterialdb(IMMATERIALDB)
def test_model_delete():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"))
    new_model.save()

    new_model.delete()

    gotten_model = MyModel.get_by_id(new_model.id)
    assert not gotten_model

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    assert response["Count"] == 0
