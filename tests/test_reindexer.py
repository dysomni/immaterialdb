import pytest

from immaterialdb.config import RootConfig
from immaterialdb.model import Model, QueryIndex
from immaterialdb.reindexer import QueueForModel, ReindexEntity, Reindexer
from immaterialdb.testing import mock_immaterialdb

mock_queue: list[str] = []
IMMATERIALDB = RootConfig("test_table")


# clear the queue before running the tests using the pytest teardown
@pytest.fixture(scope="function", autouse=True)
def clear_queue():
    mock_queue.clear()


@IMMATERIALDB.decorators.register_model([QueryIndex(partition_fields=["name"], sort_fields=["age"])])
class MyModel(Model):
    name: str
    age: int


@IMMATERIALDB.decorators.register_model([QueryIndex(partition_fields=["name"], sort_fields=["age"])])
class MyModel2(Model):
    name: str
    age: int


class MyReindexer(Reindexer):
    def enqueue(self, payloads: list[str]):
        mock_queue.extend(payloads)


class IsTruthy:
    def __eq__(self, other):
        return bool(other)


class IsFalsy:
    def __eq__(self, other):
        return not bool(other)


@mock_immaterialdb(IMMATERIALDB)
def test_reindexer_flow():
    MyModel.save(MyModel(name="John", age=20))
    MyModel.save(MyModel(name="John", age=21))
    MyModel2.save(MyModel2(name="Jane", age=21))
    MyModel2.save(MyModel2(name="Jane", age=22))
    reindexer = MyReindexer(IMMATERIALDB)
    assert len(IMMATERIALDB.registered_models) == 2
    reindexer.start_reindex(batch_size=1)
    assert len(mock_queue) == 2
    assert QueueForModel.model_validate_json(mock_queue[0]).model_dump(exclude={"index_job_id"}) == {
        "command": "queue_for_model",
        "entity_name": "MyModel",
        "last_evaluated_key": "",
        "batch_size": 1,
    }
    assert QueueForModel.model_validate_json(mock_queue[1]).model_dump(exclude={"index_job_id"}) == {
        "command": "queue_for_model",
        "entity_name": "MyModel2",
        "last_evaluated_key": "",
        "batch_size": 1,
    }

    next_command = mock_queue.pop(0)

    reindexer.process([next_command])
    assert len(mock_queue) == 3
    assert QueueForModel.model_validate_json(mock_queue[0]).model_dump(exclude={"index_job_id"}) == {
        "command": "queue_for_model",
        "entity_name": "MyModel2",
        "last_evaluated_key": IsFalsy(),
        "batch_size": 1,
    }
    assert QueueForModel.model_validate_json(mock_queue[1]).model_dump(exclude={"index_job_id"}) == {
        "command": "queue_for_model",
        "entity_name": "MyModel",
        "last_evaluated_key": IsTruthy(),
        "batch_size": 1,
    }
    assert ReindexEntity.model_validate_json(mock_queue[2]).model_dump(exclude={"index_job_id"}) == {
        "command": "reindex_entity",
        "entity_name": "MyModel",
        "entity_id": IsTruthy(),
    }

    next_two = [mock_queue.pop(0), mock_queue.pop(0)]
    reindexer.process(next_two)
    assert len(mock_queue) == 4
    assert ReindexEntity.model_validate_json(mock_queue[0]).model_dump(exclude={"index_job_id"}) == {
        "command": "reindex_entity",
        "entity_name": "MyModel",
        "entity_id": IsTruthy(),
    }
    assert QueueForModel.model_validate_json(mock_queue[1]).model_dump(exclude={"index_job_id"}) == {
        "command": "queue_for_model",
        "entity_name": "MyModel2",
        "last_evaluated_key": IsTruthy(),
        "batch_size": 1,
    }
    assert ReindexEntity.model_validate_json(mock_queue[2]).model_dump(exclude={"index_job_id"}) == {
        "command": "reindex_entity",
        "entity_name": "MyModel2",
        "entity_id": IsTruthy(),
    }
    assert ReindexEntity.model_validate_json(mock_queue[3]).model_dump(exclude={"index_job_id"}) == {
        "command": "reindex_entity",
        "entity_name": "MyModel",
        "entity_id": IsTruthy(),
    }

    rest = [mock_queue.pop(0), mock_queue.pop(0), mock_queue.pop(0), mock_queue.pop(0)]
    reindexer.process(rest)
    assert len(mock_queue) == 1
    assert ReindexEntity.model_validate_json(mock_queue[0]).model_dump(exclude={"index_job_id"}) == {
        "command": "reindex_entity",
        "entity_name": "MyModel2",
        "entity_id": IsTruthy(),
    }

    final_command = mock_queue.pop(0)
    reindexer.process([final_command])
    assert len(mock_queue) == 0
