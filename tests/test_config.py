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


def test_invalid_ttl_field_missing():
    with pytest.raises(ModelMisconfigurationError) as e:

        @IMMATERIALDB.decorators.register_model(ttl_field="expires_at")
        class MyModel(Model):
            name: str

    assert "TTL field expires_at is not present" in str(e.value)


def test_invalid_ttl_field_type():
    with pytest.raises(ModelMisconfigurationError) as e:

        @IMMATERIALDB.decorators.register_model(ttl_field="expires_at")
        class MyModel(Model):
            name: str
            expires_at: str  # should be int or Optional[int]

    assert "TTL field expires_at must be an integer or nullable integer" in str(e.value)


def test_invalid_counter_field_missing():
    with pytest.raises(ModelMisconfigurationError) as e:

        @IMMATERIALDB.decorators.register_model(counter_fields=["count"])
        class MyModel(Model):
            name: str

    assert "Counter field count is not present" in str(e.value)


def test_invalid_counter_field_type():
    with pytest.raises(ModelMisconfigurationError) as e:

        @IMMATERIALDB.decorators.register_model(counter_fields=["count", "count2"])
        class MyModel(Model):
            name: str
            count: str  # should be int

    assert "Counter field count must be an integer" in str(e.value)


def test_invalid_counter_field_type_none():
    with pytest.raises(ModelMisconfigurationError) as e:

        @IMMATERIALDB.decorators.register_model(counter_fields=["count"])
        class MyModel(Model):
            name: str
            count: int | None

    assert "Counter field count must be an integer" in str(e.value)


def test_invalid_encrypted_field_missing():
    with pytest.raises(ModelMisconfigurationError) as e:

        @IMMATERIALDB.decorators.register_model(encrypted_fields=["secret"])
        class MyModel(Model):
            name: str

    assert "Encrypted field secret is not present" in str(e.value)


def test_invalid_encrypted_field_type():
    with pytest.raises(ModelMisconfigurationError) as e:

        @IMMATERIALDB.decorators.register_model(encrypted_fields=["secret"])
        class MyModel(Model):
            name: str
            secret: int  # should be str or Optional[str]

    assert "Encrypted field secret must be a string or nullable string" in str(e.value)


def test_invalid_unique_index_field():
    with pytest.raises(ModelMisconfigurationError) as e:

        @IMMATERIALDB.decorators.register_model([Indices.Unique(unique_fields=["not_a_field"])])
        class MyModel(Model):
            name: str

    assert "Unique index field not_a_field is not present" in str(e.value)


def test_invalid_query_index_field():
    with pytest.raises(ModelMisconfigurationError) as e:

        @IMMATERIALDB.decorators.register_model([Indices.Query(partition_fields=["name"], sort_fields=["not_a_field"])])
        class MyModel(Model):
            name: str

    assert "Query index field not_a_field is not present" in str(e.value)
