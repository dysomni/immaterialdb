import json
import logging
from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import BaseModel

from immaterialdb.config import RootConfig
from immaterialdb.errors import CounterNotSavedError, RecordNotUniqueError
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
    counter_fields=["my_count"],
)
class MyModel(Model):
    name: str
    age: int
    money: Decimal
    awesome: datetime
    my_count: int


@mock_immaterialdb(IMMATERIALDB)
def test_model_save_counters():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"), awesome=datetime.now(), my_count=100)
    new_model.save()

    current_count = new_model.increment_counter("my_count", 1)
    assert current_count == 101
    assert new_model.my_count == 101


@mock_immaterialdb(IMMATERIALDB)
def test_model_sync_before_save_does_nothing():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"), awesome=datetime.now(), my_count=100)
    new_model.sync_counter_fields()
    assert new_model.my_count == 100


@mock_immaterialdb(IMMATERIALDB)
def test_model_increment_before_save_fails():
    new_model = MyModel(name="John", age=30, money=Decimal("100.00"), awesome=datetime.now(), my_count=100)
    with pytest.raises(CounterNotSavedError):
        new_model.increment_counter("my_count", 1)

    new_model.save()
    new_model.increment_counter("my_count", 1)
    assert new_model.my_count == 101
