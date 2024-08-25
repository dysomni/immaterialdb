from decimal import Decimal
from typing import TypeVar

from pytest import fixture

from immaterialdb.config import RootConfig
from immaterialdb.model import Model, QueryIndex, UniqueIndex
from immaterialdb.query import AllQuery, Queries, StandardQueryStatement
from immaterialdb.testing import mock_immaterialdb

IMMATERIALDB = RootConfig("test_table")


@IMMATERIALDB.decorators.register_model(
    [
        QueryIndex(partition_fields=[], sort_fields=["name"]),
        QueryIndex(partition_fields=["name"], sort_fields=["age"]),
        QueryIndex(partition_fields=[], sort_fields=["age"]),
        UniqueIndex(unique_fields=["name"]),
    ]
)
class MyModel(Model):
    name: str
    age: int
    money: Decimal


Number = TypeVar("Number", int, Decimal)


def sign_swapper(num: Number) -> Number:
    if int(num % 2) == 0:
        return num

    return -num


@fixture(scope="module")
def setup_db():
    with mock_immaterialdb(IMMATERIALDB):
        for i in range(250):
            MyModel(name=f"John{i}", age=sign_swapper(i), money=sign_swapper(Decimal(f"{i}.00"))).save()

        yield


def test_query_all_with_max_and_desc(setup_db):
    query = MyModel.query(Queries.All(), max_records=49, descending=True, lazy=False)
    assert len(list(query.records)) == 49
    assert query.last_evaluated_key is not None
    assert query.records[0].age == -249
    assert query.records[47].age == 202
    assert query.records[48].age == -201


def test_query_lt(setup_db):
    query = MyModel.query(Queries.Standard([StandardQueryStatement("age", "lt", 20)]), descending=True)
    assert len(list(query.records)) == 135
    assert query.last_evaluated_key is None
    assert query.records[0].age == 18
    assert query.records[1].age == 16


def test_query_lte(setup_db):
    query = MyModel.query(Queries.Standard([StandardQueryStatement("age", "lte", 20)]), descending=True)
    assert len(list(query.records)) == 136
    assert query.last_evaluated_key is None
    assert query.records[0].age == 20
    assert query.records[1].age == 18
    assert query.records[2].age == 16


def test_query_gt(setup_db):
    query = MyModel.query(Queries.Standard([StandardQueryStatement("age", "gt", 20)]))
    assert len(list(query.records)) == 114
    assert query.last_evaluated_key is None
    assert query.records[0].age == 22
    assert query.records[1].age == 24


def test_query_gte(setup_db):
    query = MyModel.query(Queries.Standard([StandardQueryStatement("age", "gte", 20)]))
    assert len(list(query.records)) == 115
    assert query.last_evaluated_key is None
    assert query.records[0].age == 20
    assert query.records[1].age == 22
    assert query.records[2].age == 24


def test_query_starts_with(setup_db):
    query = MyModel.query(Queries.Standard([StandardQueryStatement("name", "begins_with", "John1")]))
    assert query.records[0].name == "John1"
    assert query.records[1].name == "John10"
    assert query.records[2].name == "John100"
    assert query.records[3].name == "John101"


def test_query_eq(setup_db):
    query = MyModel.query(Queries.Standard([StandardQueryStatement("name", "eq", "John1")]))
    assert len(list(query.records)) == 1
    assert query.last_evaluated_key is None
    assert query.records[0].name == "John1"
