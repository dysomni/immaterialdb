import pytest

from immaterialdb.config import RootConfig
from immaterialdb.errors import ModelMisconfigurationError
from immaterialdb.model import Indices, Model

IMMATERIALDB = RootConfig("test_table")


def test_redundant_unique_index():
    with pytest.raises(ModelMisconfigurationError) as e:

        @IMMATERIALDB.decorators.register_model(
            [
                Indices.Unique(unique_fields=["name"]),
                Indices.Unique(unique_fields=["age", "name"]),
            ]
        )
        class MyModel(Model):
            name: str
            age: int

    assert (
        str(e.value)
        == "Unique index ['age', 'name'] is redundant to ['name'] because the latter is a subset of the former"
    )


def test_redundant_query_index():
    with pytest.raises(ModelMisconfigurationError) as e:

        @IMMATERIALDB.decorators.register_model(
            [
                Indices.Query(partition_fields=["name"], sort_fields=["age"]),
                Indices.Query(partition_fields=["name"], sort_fields=["age", "name"]),
            ]
        )
        class MyModel(Model):
            name: str
            age: int

    assert (
        str(e.value)
        == "Query index pk ['name'] sk ['age'] is redundant to pk ['name'] sk ['age', 'name'] because the former is a subset of the latter"
    )
