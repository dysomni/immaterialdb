from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, Literal, NamedTuple, Self, TypeVar

from boto3.dynamodb.conditions import Key

from immaterialdb.dynamo_provider import DynamodbConnectionProvider
from immaterialdb.errors import QueryNotSupportedError
from immaterialdb.types import FieldValue, LastEvaluatedKey
from immaterialdb.value_serializers import (
    serialize_for_query_node_partial_sort_key,
    serialize_for_query_node_partition_key,
)

if TYPE_CHECKING:
    from immaterialdb.model import Model

T = TypeVar("T", bound="Model")


class QueryResultSingleIterator(Generic[T]):
    _parent: "QueryResult[T]"
    _current_index: int

    def __init__(self, parent: "QueryResult[T]"):
        self._parent = parent
        self._current_index = 0

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> T:
        if self._current_index >= len(self._parent._flattened_items):
            new_items: list[T] = []
            while not new_items:
                new_items = next(self._parent)

        entity = self._parent._flattened_items[self._current_index]
        self._current_index += 1
        return entity


class QueryResult(Generic[T]):
    batches: list[list[T]]
    items: QueryResultSingleIterator[T]
    last_evaluated_key: LastEvaluatedKey | None
    more_to_query: bool
    querier: "Querier"

    _flattened_items: list[T]
    _current_batch_index: int
    _max_items: int | None

    def __init__(self, querier: "Querier[T]", lazy: bool = True, max_items: int | None = None):
        self.batches = []
        self._current_batch_index = 0
        self.last_evaluated_key = None
        self._flattened_items = []
        self.more_to_query = True
        self.querier = querier
        self.items = QueryResultSingleIterator(self)
        self._max_items = max_items

        if not lazy:
            list(self)  # trigger the iteration to run the query
            self._current_batch_index = 0  # reset the iterator

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> list[T]:
        if self._current_batch_index >= len(self.batches):
            if not self.more_to_query or self.reached_max_items:
                raise StopIteration

            result = self.querier.query(self.last_evaluated_key, self.next_limit)
            self.batches.append(result.items)
            self.last_evaluated_key = result.last_evaluated_key
            self.more_to_query = result.more_to_query

        batch = self.batches[self._current_batch_index]
        self._current_batch_index += 1
        return batch

    @property
    def reached_max_items(self) -> bool:
        return self._max_items is not None and len(self._flattened_items) >= self._max_items

    @property
    def next_limit(self) -> int:
        batch_size = 50
        if self._max_items and self._max_items <= len(self._flattened_items):
            return 0

        if self._max_items:
            remaining = self._max_items - len(self._flattened_items)
            return min(remaining, batch_size)

        return batch_size


class QueryActionResult(Generic[T]):
    items: list[T]
    last_evaluated_key: LastEvaluatedKey | None
    more_to_query: bool

    def __init__(self, items: list[T], last_evaluated_key: LastEvaluatedKey | None, more_to_query: bool):
        self.items = items
        self.last_evaluated_key = last_evaluated_key
        self.more_to_query = more_to_query


class Querier(Generic[T]):
    model_cls: type[T]
    given_query: "QueryTypes"
    dynamodb_provider: DynamodbConnectionProvider
    scan_index_forward: bool
    consistent_read: bool

    def __init__(
        self,
        model_cls: type[T],
        query: "QueryTypes",
        dynamodb_provider: DynamodbConnectionProvider,
        scan_index_forward: bool = True,
        consistent_read: bool = True,
    ):
        self.model_cls = model_cls
        self.given_query = query
        self.dynamodb_provider = dynamodb_provider
        self.scan_index_forward = scan_index_forward
        self.consistent_read = consistent_read

    def query(self, last_evaluated_key: LastEvaluatedKey | None = None, limit: int = 50) -> QueryActionResult[T]:
        if not all(map(lambda statement: statement.operation == "eq", self.given_query.statements)):
            raise QueryNotSupportedError("Only 'eq' operations are supported in queries.")

        index = self.model_cls._map_query_fields_to_index(self.given_query)
        if not index:
            raise QueryNotSupportedError("No index found for the given query fields.")

        pk_fields = [
            FieldValue(name=statement.field, value=statement.value)
            for statement in self.given_query.statements[: len(index.partition_fields)]
        ]
        sk_fields = [
            FieldValue(name=statement.field, value=statement.value)
            for statement in self.given_query.statements[len(index.partition_fields) :]
        ]

        pk = serialize_for_query_node_partition_key(self.model_cls.model_name(), pk_fields, index.sort_fields)
        sk = serialize_for_query_node_partial_sort_key(sk_fields)

        result = self.dynamodb_provider.table.query(
            KeyConditionExpression=Key("pk").eq(pk) & Key("sk").begins_with(sk),
            ConsistentRead=self.consistent_read,
            ExclusiveStartKey=last_evaluated_key if last_evaluated_key else {},
            ScanIndexForward=self.scan_index_forward,
            Limit=limit,
        )

        items = [self.model_cls.model_validate(item) for item in result.get("Items", [])]
        last_evaluated_key = result["LastEvaluatedKey"] if "LastEvaluatedKey" in result else None
        more_to_query = "LastEvaluatedKey" in result

        return QueryActionResult(items, last_evaluated_key, more_to_query)


StandardQueryStatement = NamedTuple(
    "StandardQueryStatement", [("field", str), ("operation", Literal["eq"]), ("value", Any)]
)


class StandardQuery:
    def __init__(self, statements: list[StandardQueryStatement]):
        self.statements = statements

    statements: list[StandardQueryStatement]

    @cached_property
    def all_fields(self) -> list[str]:
        return [statement.field for statement in self.statements]


QueryTypes = StandardQuery
