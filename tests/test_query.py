from decimal import Decimal

from pytest import fixture

from immaterialdb.config import RootConfig
from immaterialdb.model import Model, QueryIndex, UniqueIndex
from immaterialdb.query import AllQuery, Queries
from immaterialdb.testing import mock_immaterialdb

IMMATERIALDB = RootConfig("test_table")


@IMMATERIALDB.decorators.register_model(
    [QueryIndex(partition_fields=["name"], sort_fields=["age"]), UniqueIndex(unique_fields=["name"])]
)
class MyModel(Model):
    name: str
    age: int
    money: Decimal


@fixture(scope="module")
def setup_db():
    with mock_immaterialdb(IMMATERIALDB):
        for i in range(250):
            MyModel(name=f"John{i}", age=i, money=Decimal(f"{i}.00")).save()

        yield


def test_query_all_with_max_and_desc(setup_db):
    query = MyModel.query(Queries.All(), max_records=49, descending=True, lazy=False)
    assert len(list(query.records)) == 49
    assert query.last_evaluated_key is not None
    assert query.records[0].age == 249
    assert query.records[48].age == 201
