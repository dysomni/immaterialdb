from decimal import Decimal

from immaterialdb.config import RootConfig
from immaterialdb.model import Model, QueryIndex
from immaterialdb.testing import mock_immaterialdb

IMMATERIALDB = RootConfig("test_table")


@mock_immaterialdb(IMMATERIALDB)
def test_adding_model():
    @IMMATERIALDB.decorators.register_model([QueryIndex(partition_fields=["name"], sort_fields=["age"])])
    class TestModel(Model):
        name: str
        age: int
        money: Decimal

    assert TestModel.immaterial_model_name() in IMMATERIALDB.registered_models
