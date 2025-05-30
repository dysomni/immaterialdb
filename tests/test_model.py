import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

import dateutil.parser
import pytest
from pydantic import BaseModel

from immaterialdb.config import RootConfig
from immaterialdb.errors import LockNotAcquiredError, RecordNotUniqueError
from immaterialdb.model import Model, QueryIndex, UniqueIndex, materialize_model
from immaterialdb.query import AllQuery, StandardQuery, StandardQueryStatement
from immaterialdb.testing import mock_immaterialdb

IMMATERIALDB = RootConfig("test_table")


@IMMATERIALDB.decorators.register_model(
    [
        QueryIndex(partition_fields=["name"], sort_fields=["age"]),
        QueryIndex(partition_fields=["name"], sort_fields=["awesome"]),
        UniqueIndex(unique_fields=["name"]),
    ],
)
class MyModel(Model):
    name: str
    age: int
    money: Decimal
    awesome: datetime


@IMMATERIALDB.decorators.register_model(counter_fields=["age"])
class MyModelWithCounter(Model):
    age: int


def test_adding_model():
    assert MyModel.immaterial_model_name() in IMMATERIALDB.registered_models


@mock_immaterialdb(IMMATERIALDB)
def test_model_save_and_get():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"), awesome=datetime.now())
    new_model.save()

    gotten_model = MyModel.get_by_id(new_model.id)
    assert gotten_model
    assert gotten_model == new_model

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    assert response["Count"] == 4
    expected_nodes = [json.loads(node.model_dump_json()) for node in materialize_model(new_model)]
    assert response["Items"][0] in expected_nodes
    assert response["Items"][1] in expected_nodes
    assert response["Items"][2] in expected_nodes
    assert response["Items"][3] in expected_nodes


@mock_immaterialdb(IMMATERIALDB)
def test_model_update():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"), awesome=datetime.now())
    new_model.save()

    updated_model = new_model.copy()
    updated_model.age = 31
    updated_model.save()

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    expected_nodes = [json.loads(node.model_dump_json()) for node in materialize_model(updated_model)]
    assert response["Count"] == 4
    assert response["Items"][0] in expected_nodes
    assert response["Items"][1] in expected_nodes
    assert response["Items"][2] in expected_nodes
    assert response["Items"][3] in expected_nodes


@mock_immaterialdb(IMMATERIALDB)
def test_model_unique_index():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"), awesome=datetime.now())
    new_model.save()

    duplicate_name_model = MyModel(name="John", age=-234, money=Decimal("-3424.00"), awesome=datetime.now())
    with pytest.raises(RecordNotUniqueError) as error:
        duplicate_name_model.save()

    assert str(error.value) == "Record already exists with unique key MyModel(name=John)"

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    expected_nodes = [json.loads(node.model_dump_json()) for node in materialize_model(new_model)]
    assert response["Count"] == 4
    assert response["Items"][0] in expected_nodes
    assert response["Items"][1] in expected_nodes
    assert response["Items"][2] in expected_nodes
    assert response["Items"][3] in expected_nodes


@mock_immaterialdb(IMMATERIALDB)
def test_model_query():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"), awesome=datetime.now())
    new_model.save()

    response = MyModel.query(StandardQuery(statements=[StandardQueryStatement("name", "eq", "John")]))
    assert response.records[0] == new_model


@mock_immaterialdb(IMMATERIALDB)
def test_model_query_all():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"), awesome=datetime.now())
    new_model.save()

    response = MyModel.query(AllQuery())
    assert response.records[0] == new_model


@mock_immaterialdb(IMMATERIALDB)
def test_model_delete():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"), awesome=datetime.now())
    new_model.save()

    new_model.delete()

    gotten_model = MyModel.get_by_id(new_model.id)
    assert not gotten_model

    response = IMMATERIALDB.dynamodb_provider.table.scan()
    assert response["Count"] == 0


@mock_immaterialdb(IMMATERIALDB)
def test_model_register_save_hooks():
    @IMMATERIALDB.decorators.register_model()
    class NewModel(Model):
        name: str
        age: int

    @NewModel.register_pre_save_hook()
    def pre_save_hook(model: NewModel, decrypted_copy: NewModel):
        model.age += 1

    @NewModel.register_post_save_hook()
    def post_save_hook(model: NewModel, decrypted_copy: NewModel):
        model.age += 1

    new_model = NewModel(name="John", age=30)
    new_model.save()
    assert new_model.age == 32

    saved_model = NewModel.get_by_id(new_model.id)
    assert saved_model
    assert saved_model.age == 31


@mock_immaterialdb(IMMATERIALDB)
def test_counter():
    record = MyModelWithCounter(age=30)
    record.save()

    record.increment_counter("age")
    assert record.age == 31
    record.increment_counter("age", 10)
    assert record.age == 41

    seperate_thread_record = MyModelWithCounter.get_by_id(record.id)
    assert seperate_thread_record
    # increment returns the new value
    assert seperate_thread_record.increment_counter("age") == 42

    # can sync
    assert record.age == 41
    record.refresh_counters()
    assert record.age == 42

    # can decrement
    assert record.increment_counter("age", -1) == 41
    assert record.age == 41


@mock_immaterialdb(IMMATERIALDB)
def test_record_lock():
    record = MyModel(name="John", age=30, money=Decimal("100.00"), awesome=datetime.now())
    record.save()

    with record.record_lock(ttl=5, wait=1):
        record.age += 1
        record.save()
        with pytest.raises(LockNotAcquiredError):
            with record.record_lock(ttl=1, wait=1):
                record.age += 1
                record.save()

    assert record.age == 31
