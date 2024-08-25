from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, Literal, NamedTuple, Self, TypeVar

from boto3.dynamodb.conditions import Key
from mypy_boto3_dynamodb.type_defs import ConditionBaseImportTypeDef

from immaterialdb.dynamo_provider import DynamodbConnectionProvider, GsiNames
from immaterialdb.errors import QueryNotSupportedError
from immaterialdb.nodes import BaseNode, QueryNode
from immaterialdb.types import FieldValue, LastEvaluatedKey
from immaterialdb.value_serializers import (
    serialize_for_query_node_partial_sort_key,
    serialize_for_query_node_partition_key,
)

if TYPE_CHECKING:
    from immaterialdb.model import Model

T = TypeVar("T", bound="Model")


class RecordQueryResult(Generic[T]):
    _parent: "BatchQueryResult[T]"
    _current_index: int

    def __init__(self, parent: "BatchQueryResult[T]"):
        self._parent = parent
        self._current_index = 0

    def __iter__(self) -> Self:
        self._current_index = 0
        return self

    def __next__(self) -> T:
        while self._current_index >= len(self._parent._flattened_records):
            next(self._parent)

        entity = self._parent._flattened_records[self._current_index]
        self._current_index += 1
        return entity

    def __getitem__(self, index: int) -> T:
        try:
            while True:
                try:
                    return self._parent._flattened_records[index]
                except IndexError:
                    next(self._parent)
        except StopIteration:
            raise IndexError("Index out of range")


class BatchQueryResult(Generic[T]):
    records: RecordQueryResult[T]
    last_evaluated_key: LastEvaluatedKey | None
    more_to_query: bool
    querier: "Querier"

    _batches: list[list[T]]
    _flattened_records: list[T]
    _current_batch_index: int
    _max_records: int | None

    def __init__(
        self,
        querier: "Querier[T]",
        lazy: bool = True,
        max_records: int | None = None,
        last_evaluated_key: LastEvaluatedKey | None = None,
    ):
        self._batches = []
        self._current_batch_index = 0
        self.last_evaluated_key = last_evaluated_key
        self._flattened_records = []
        self.more_to_query = True
        self.querier = querier
        self.records = RecordQueryResult(self)
        self._max_records = max_records

        if not lazy:
            list(self)  # trigger the iteration to run the query
            self._current_batch_index = 0  # reset the iterator

    def __iter__(self) -> Self:
        self._current_batch_index = 0
        return self

    def __next__(self) -> list[T]:
        if self._current_batch_index >= len(self._batches):
            if not self.more_to_query or self.reached_max_records:
                raise StopIteration

            self.make_query()

        batch = self._batches[self._current_batch_index]
        self._current_batch_index += 1
        return batch

    def make_query(self) -> None:
        if not self.more_to_query:
            return

        result = self.querier.query(self.last_evaluated_key, self.next_limit)
        self._batches.append(result.records)
        self._flattened_records.extend(result.records)
        self.last_evaluated_key = result.last_evaluated_key
        self.more_to_query = result.more_to_query

    @property
    def reached_max_records(self) -> bool:
        return self._max_records is not None and len(self._flattened_records) >= self._max_records

    @property
    def next_limit(self) -> int:
        batch_size = 50
        if self._max_records and self._max_records <= len(self._flattened_records):
            return 0

        if self._max_records:
            remaining = self._max_records - len(self._flattened_records)
            return min(remaining, batch_size)

        return batch_size

    def next_batch(self) -> list[T] | None:
        try:
            return next(self)
        except StopIteration:
            return None


class QueryActionResult(Generic[T]):
    records: list[T]
    last_evaluated_key: LastEvaluatedKey | None
    more_to_query: bool

    def __init__(self, records: list[T], last_evaluated_key: LastEvaluatedKey | None, more_to_query: bool):
        self.records = records
        self.last_evaluated_key = last_evaluated_key
        self.more_to_query = more_to_query


class Querier(Generic[T]):
    model_cls: type[T]
    given_query: "QueryTypes"
    dynamodb_provider: DynamodbConnectionProvider
    scan_index_forward: bool

    def __init__(
        self,
        model_cls: type[T],
        query: "QueryTypes",
        dynamodb_provider: DynamodbConnectionProvider,
        scan_index_forward: bool = True,
    ):
        self.model_cls = model_cls
        self.given_query = query
        self.dynamodb_provider = dynamodb_provider
        self.scan_index_forward = scan_index_forward

    def query(self, last_evaluated_key: LastEvaluatedKey | None = None, limit: int = 50) -> QueryActionResult[T]:
        extra = {"ConsistentRead": True}
        if isinstance(self.given_query, StandardQuery):
            key_condition = self.standard_query_to_key_condition(self.given_query)
            extra["ConsistentRead"] = self.given_query.consistent_read
            node_type_cls = QueryNode
        elif isinstance(self.given_query, KeyConditionQuery):
            key_condition = self.given_query.key_condition
            extra["ConsistentRead"] = self.given_query.consistent_read
            if self.given_query.gsi_name:
                extra["IndexName"] = self.given_query.gsi_name
            node_type_cls = QueryNode
        elif isinstance(self.given_query, AllQuery):
            key_condition = Key("entity_name").eq(self.model_cls.immaterial_model_name())
            extra["IndexName"] = GsiNames.model_scan
            extra["ConsistentRead"] = False
            node_type_cls = BaseNode
        else:
            raise QueryNotSupportedError(f"Query type {type(self.given_query)} is not supported.")

        if last_evaluated_key:
            extra["ExclusiveStartKey"] = last_evaluated_key

        result = self.dynamodb_provider.table.query(
            KeyConditionExpression=key_condition,
            ScanIndexForward=self.scan_index_forward,
            Limit=limit,
            **extra,
        )

        query_nodes = [node_type_cls.model_validate(item) for item in result.get("Items", [])]
        records = [self.model_cls.model_validate_json(node.raw_data) for node in query_nodes]

        last_evaluated_key = result["LastEvaluatedKey"] if "LastEvaluatedKey" in result else None
        more_to_query = "LastEvaluatedKey" in result

        return QueryActionResult(records, last_evaluated_key, more_to_query)

    def standard_query_to_key_condition(self, standard_query: "StandardQuery") -> ConditionBaseImportTypeDef:
        if not all(map(lambda statement: statement.operation == "eq", standard_query.statements)):
            raise QueryNotSupportedError("Only 'eq' operations are supported in queries.")

        index = self.model_cls._map_query_fields_to_index(standard_query)
        if not index:
            raise QueryNotSupportedError("No index found for the given query fields.")

        pk_fields = [
            FieldValue(name=statement.field, value=statement.value)
            for statement in standard_query.statements[: len(index.partition_fields)]
        ]
        sk_fields = [
            FieldValue(name=statement.field, value=statement.value)
            for statement in standard_query.statements[len(index.partition_fields) :]
        ]

        pk = serialize_for_query_node_partition_key(
            self.model_cls.immaterial_model_name(), pk_fields, index.sort_fields
        )
        sk = serialize_for_query_node_partial_sort_key(sk_fields)

        return Key("pk").eq(pk) & Key("sk").begins_with(sk)


StandardQueryStatement = NamedTuple(
    "StandardQueryStatement", [("field", str), ("operation", Literal["eq"]), ("value", Any)]
)


class StandardQuery:
    statements: list[StandardQueryStatement]
    consistent_read: bool

    def __init__(self, statements: list[StandardQueryStatement], consistent_read: bool = True):
        self.statements = statements
        self.consistent_read = consistent_read

    @cached_property
    def all_fields(self) -> list[str]:
        return [statement.field for statement in self.statements]


class KeyConditionQuery:
    key_condition: ConditionBaseImportTypeDef
    consistent_read: bool
    gsi_name: str | None

    def __init__(
        self, key_condition: ConditionBaseImportTypeDef, consistent_read: bool = True, gsi_name: str | None = None
    ):
        self.key_condition = key_condition
        self.consistent_read = consistent_read
        self.gsi_name = gsi_name


class AllQuery:
    pass


QueryTypes = StandardQuery | KeyConditionQuery | AllQuery


class Queries:
    Standard = StandardQuery
    KeyCondition = KeyConditionQuery
    All = AllQuery
