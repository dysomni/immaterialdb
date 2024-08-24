import logging
from decimal import Decimal

import pytest
from pydantic import BaseModel

from immaterialdb.config import RootConfig
from immaterialdb.errors import RecordNotUniqueError
from immaterialdb.model import Model, QueryIndex, UniqueIndex
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
    assert response["Items"] == [
        {
            "node_type": "base",
            "entity_name": "MyModel",
            "entity_id": gotten_model.id,
            "pk": gotten_model.id,
            "sk": gotten_model.id,
            "base_node": "base",
            "raw_data": gotten_model.model_dump_json(),
            "other_nodes": [
                ["MyModel[name=John][age]", f"##100000000000000000030##{gotten_model.id}"],
                ["MyModel(name=John)", "unique"],
            ],
        },
        {
            "node_type": "query",
            "entity_name": "MyModel",
            "entity_id": gotten_model.id,
            "pk": "MyModel[name=John][age]",
            "sk": f"##100000000000000000030##{gotten_model.id}",
            "query_node": "query",
            "partition_fields": [["name", "John"]],
            "sort_fields": [["age", Decimal("30")]],
            "raw_data": gotten_model.model_dump_json(),
        },
        {
            "node_type": "unique",
            "entity_name": "MyModel",
            "entity_id": gotten_model.id,
            "pk": "MyModel(name=John)",
            "sk": "unique",
            "unique_node": "unique",
            "fields": [["name", "John"]],
        },
    ]

    gotten_model.name = "Jane"
    gotten_model.save()

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    assert response["Items"] == [
        {
            "node_type": "base",
            "entity_name": "MyModel",
            "entity_id": gotten_model.id,
            "pk": gotten_model.id,
            "sk": gotten_model.id,
            "base_node": "base",
            "raw_data": gotten_model.model_dump_json(),
            "other_nodes": [
                ["MyModel[name=Jane][age]", f"##100000000000000000030##{gotten_model.id}"],
                ["MyModel(name=Jane)", "unique"],
            ],
        },
        {
            "node_type": "query",
            "entity_name": "MyModel",
            "entity_id": gotten_model.id,
            "pk": "MyModel[name=Jane][age]",
            "sk": f"##100000000000000000030##{gotten_model.id}",
            "query_node": "query",
            "partition_fields": [["name", "Jane"]],
            "sort_fields": [["age", Decimal("30")]],
            "raw_data": gotten_model.model_dump_json(),
        },
        {
            "node_type": "unique",
            "entity_name": "MyModel",
            "entity_id": gotten_model.id,
            "pk": "MyModel(name=Jane)",
            "sk": "unique",
            "unique_node": "unique",
            "fields": [["name", "Jane"]],
        },
    ]

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
