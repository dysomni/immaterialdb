import json
import logging
from decimal import Decimal

import pytest
from pydantic import BaseModel

from immaterialdb.config import RootConfig
from immaterialdb.errors import RecordNotUniqueError
from immaterialdb.model import Model, QueryIndex, UniqueIndex, materialize_model
from immaterialdb.query import StandardQuery, StandardQueryStatement
from immaterialdb.testing import mock_immaterialdb

IMMATERIALDB = RootConfig("test_table")


@IMMATERIALDB.decorators.register_model(
    [QueryIndex(partition_fields=["name"], sort_fields=["age"]), UniqueIndex(unique_fields=["name"])]
)
class MyModel(Model):
    name: str
    age: int
    money: Decimal


@mock_immaterialdb(IMMATERIALDB)
def test_adding_model():
    assert MyModel.immaterial_model_name() in IMMATERIALDB.registered_models

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

    gotten_model.name = "Jane"
    gotten_model.save()

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    expected_nodes = [json.loads(node.model_dump_json()) for node in materialize_model(gotten_model)]
    assert response["Count"] == 3
    assert response["Items"][0] in expected_nodes
    assert response["Items"][1] in expected_nodes
    assert response["Items"][2] in expected_nodes

    duplicate_name_model = MyModel(name="Jane", age=-234, money=Decimal("-3424.00"))
    with pytest.raises(RecordNotUniqueError) as error:
        duplicate_name_model.save()

    assert str(error.value) == "Record already exists with unique key MyModel(name=Jane)"

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    assert response["Count"] == 3

    response = MyModel.query(StandardQuery(statements=[StandardQueryStatement("name", "eq", "Jane")]))
    assert response.records[0]

    gotten_model.delete()

    gotten_model = MyModel.get_by_id(new_model.id)
    assert not gotten_model

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    assert response["Count"] == 0
