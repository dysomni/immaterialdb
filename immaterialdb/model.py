import hashlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, ClassVar, Literal, Self

import ulid
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.service_resource import Table
from pydantic import BaseModel, ConfigDict, Field, model_validator

from immaterialdb.constants import ENCRYPTED_FIELD_PREFIX, LOGGER
from immaterialdb.error_boundaries import transaction_write_error_boundary
from immaterialdb.errors import FieldMisconfigurationError
from immaterialdb.nodes import (
    BaseNode,
    NodeTransactionItem,
    NodeTransactionList,
    NodeTypeList,
    NodeTypes,
    QueryNode,
    UniqueNode,
)
from immaterialdb.query import BatchQueryResult, Querier, QueryTypes, StandardQuery
from immaterialdb.types import FieldValue, LastEvaluatedKey, PrimaryKey
from immaterialdb.value_serializers import serialize_field_values_for_dynamo

if TYPE_CHECKING:
    from immaterialdb.config import RootConfig


class UniqueIndex(BaseModel):
    """
    Represents a unique index on a model.

    This index ensures that all records in the model are unique based on the specified fields.

    Attributes:
        index_type (Literal["unique"]): The type of index, always "unique".
        unique_fields (list[str]): The fields that must be unique across all records.

    Example:
        >>> from immaterialdb import Indices
        >>> indices = [Indices.Unique(unique_fields=["name", "age"])]
    """

    index_type: Literal["unique"] = "unique"
    unique_fields: list[str]


class QueryIndex(BaseModel):
    """
    Represents a queryable index on a model, defining how records can be efficiently queried using partition and sort keys.

    QueryIndex specifies a set of fields that together form a DynamoDB index for efficient querying. The partition_fields define the primary grouping (partition key), while sort_fields allow for range queries and ordering within each partition. This structure enables complex queries, such as filtering by one or more fields and sorting or paginating results.

    QueryIndex is used by the query planner to match user queries (e.g., StandardQuery) to the most appropriate index, ensuring that queries are executed efficiently and in accordance with DynamoDB's indexing rules. It is also used during model materialization to create the necessary index nodes for each record.

    Attributes:
        index_type (Literal["query"]): The type of index, always "query" for this class.
        partition_fields (list[str]): The fields that make up the partition (hash) key for the index.
        sort_fields (list[str]): The fields that make up the sort (range) key for the index.

    Properties:
        all_fields (list[str]): Returns the concatenation of partition_fields and sort_fields, representing the full index key order.

    Example:
        >>> from immaterialdb import Indices
        >>> indices = [
        ...     Indices.Query(partition_fields=["name"], sort_fields=["age"]),
        ... ]
        >>> # This allows queries like:
        >>> # - All records with a given name, sorted by age
        >>> # - All records sorted by creation time
    """

    index_type: Literal["query"] = "query"
    partition_fields: list[str]
    sort_fields: list[str]

    @property
    def all_fields(self) -> list[str]:
        return self.partition_fields + self.sort_fields


class Indices:
    """
    A helper class for constructing indices. Includes aliases for the different index types for convenience.

    Example:
        >>> from immaterialdb import Indices
        >>> indices = [Indices.Unique(unique_fields=["name", "age"])]
    """

    Unique = UniqueIndex
    Query = QueryIndex


IndicesType = list[UniqueIndex | QueryIndex]


class ModelConfig:
    root_config: "RootConfig"
    indices: IndicesType
    encrypted_fields: list[str]
    auto_decrypt: bool

    def __init__(
        self,
        root_config: "RootConfig",
        indices: IndicesType,
        encrypted_fields: list[str] | None = None,
        auto_decrypt: bool = True,
    ):
        self.root_config = root_config
        self.indices = indices
        self.encrypted_fields = encrypted_fields or []
        self.auto_decrypt = auto_decrypt


class Model(BaseModel):
    """
    Base class for all user-defined models in immaterialdb.

    Inherit from this class to define your application's data models. The Model class provides built-in support for DynamoDB-backed storage, automatic ULID-based IDs, timestamp management, and integration with immaterialdb's indexing, encryption, and query system.

    Included Attributes:
        id (str):
            The unique identifier for the record (ULID by default).
        created_at (datetime):
            The UTC timestamp when the record was created.
        updated_at (datetime):
            The UTC timestamp when the record was last updated.
        updated_hash (str | None):
            A hash of the record's data for change detection.

    Class Attributes:
        __immaterial_root_config__ (RootConfig):
            The root configuration for the database (set automatically).
        __immaterial_model_config__ (ModelConfig):
            The model's configuration, including indices and encryption settings.
        __immaterial_model_name__ (str | None):
            The name used for the model in the database (defaults to class name).

    Methods:
        save():
            Save or update the record in the database.
        get_by_id(id):
            Retrieve a record by its ID.
        query(...):
            Query records using StandardQuery, KeyConditionQuery, or AllQuery.
        delete():
            Delete the record from the database.
        delete_by_id(id):
            Delete a record by its ID.
        encrypt_fields():
            Encrypt all configured fields before saving.
        decrypt_fields():
            Decrypt all configured fields after loading.

    Example:
        >>> from immaterialdb import RootConfig, Indices, Model
        >>> db = RootConfig("my_table")
        >>> @db.decorators.register_model([
        ...     Indices.Unique(unique_fields=["email"]),
        ...     Indices.Query(partition_fields=["email"], sort_fields=["created_at"])
        ... ])
        ... class User(Model):
        ...     email: str
        ...     created_at: str
        >>> user = User(email="alice@example.com", created_at="2024-06-30T12:00:00Z")
        >>> user.save()
        >>> fetched = User.get_by_id(user.id)
        >>> assert fetched.email == "alice@example.com"
        >>> for record in User.query(Queries.All()).records:
        ...     print(record)
    """

    __immaterial_root_config__: ClassVar["RootConfig"]
    __immaterial_model_config__: ClassVar[ModelConfig]
    __immaterial_model_name__: ClassVar[str | None] = None

    model_config = ConfigDict(validate_assignment=True)

    id: str = Field(default_factory=lambda: ulid.new().str)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_hash: str | None = None

    @property
    def hash_for_update(self) -> str:
        return hashlib.md5(self.model_dump_json(exclude={"updated_hash", "updated_at"}).encode("utf-8")).hexdigest()

    @model_validator(mode="after")
    def check_for_updates(self) -> Self:
        new_hash = self.hash_for_update
        if self.updated_hash != new_hash:
            self.updated_hash = new_hash
            self.updated_at = datetime.now(timezone.utc)

        return self

    def fetch_field_values(self, field_list: list[str]) -> list[FieldValue]:
        field_values: list[FieldValue] = []
        for field in field_list:
            try:
                field_values.append(FieldValue(field, getattr(self, field)))
            except AttributeError as e:
                raise FieldMisconfigurationError(
                    f"Field {field} is not present in the model {self.immaterial_model_name()}"
                ) from e

        return field_values

    @classmethod
    def immaterial_model_name(cls) -> str:
        return cls.__immaterial_model_name__ or cls.__name__

    def save(self):
        with self.__immaterial_root_config__.dynamodb_provider.lock(self.id):
            current_nodes = materialize_model(self)
            existing_nodes: NodeTypeList
            existing_base_node = self._get_base_node(self.id)
            if existing_base_node:
                existing_nodes = [existing_base_node, *self._get_other_nodes(existing_base_node)]
            else:
                existing_nodes = []

            for_deletion = [node for node in existing_nodes if node not in current_nodes]

            transaction_items = [
                *[NodeTransactionItem(node, "put") for node in current_nodes],
                *[NodeTransactionItem(node, "delete") for node in for_deletion],
            ]
            self._write_transaction(transaction_items)

    @classmethod
    def get_by_id(cls, id: str) -> Self | None:
        base_node = cls._get_base_node(id)
        if not base_node:
            return None

        model = cls.model_validate_json(base_node.raw_data)
        if cls.__immaterial_model_config__.auto_decrypt:
            model.decrypt_fields()
        return model

    @classmethod
    def query(
        cls,
        query: QueryTypes,
        descending: bool = False,
        max_records: int | None = None,
        lazy: bool = True,
        last_evaluated_key: LastEvaluatedKey | None = None,
    ) -> BatchQueryResult[Self]:
        querier = Querier(
            cls,
            query,
            cls.__immaterial_root_config__.dynamodb_provider,
            scan_index_forward=not descending,
            auto_decrypt=cls.__immaterial_model_config__.auto_decrypt,
        )
        return BatchQueryResult(
            querier=querier, lazy=lazy, max_records=max_records, last_evaluated_key=last_evaluated_key
        )

    def delete(self):
        self.delete_by_id(self.id)

    @classmethod
    def delete_by_id(cls, id: str):
        with cls.__immaterial_root_config__.dynamodb_provider.lock(id):
            base_node = cls._get_base_node(id)
            if not base_node:
                return

            other_nodes = cls._get_other_nodes(base_node)
            transaction_items = [
                NodeTransactionItem(base_node, "delete"),
                *[NodeTransactionItem(node, "delete") for node in other_nodes],
            ]
            cls._write_transaction(transaction_items)

    def encrypt_fields(self):
        for field in self.__immaterial_model_config__.encrypted_fields:
            if not hasattr(self, field):
                raise FieldMisconfigurationError(
                    f"Field for encryption {field} is not present in the model {self.immaterial_model_name()}"
                )

            value = getattr(self, field)
            if not isinstance(value, str):
                LOGGER.debug(f"Field {field} is not a string, skipping encryption")
                continue

            if value.startswith(ENCRYPTED_FIELD_PREFIX):
                LOGGER.debug(f"Field {field} is already encrypted, skipping encryption")
                continue

            setattr(self, field, ENCRYPTED_FIELD_PREFIX + self.__immaterial_root_config__._encrypt_string(value))

    def decrypt_fields(self):
        for field in self.__immaterial_model_config__.encrypted_fields:
            if not hasattr(self, field):
                raise FieldMisconfigurationError(
                    f"Field for decryption {field} is not present in the model {self.immaterial_model_name()}"
                )

            value = getattr(self, field)
            if not isinstance(value, str):
                LOGGER.debug(f"Field {field} is not a string, skipping decryption")
                continue

            if not value.startswith(ENCRYPTED_FIELD_PREFIX):
                LOGGER.debug(f"Field {field} is not encrypted, skipping decryption")
                continue

            value = value.replace(ENCRYPTED_FIELD_PREFIX, "")
            setattr(self, field, self.__immaterial_root_config__._decrypt_string(value))

    @classmethod
    def _get_base_node(cls, id: str) -> BaseNode | None:
        response = cls._table().get_item(Key={"pk": id, "sk": id}, ConsistentRead=True)
        item = response.get("Item")
        return BaseNode.model_validate(item) if item else None

    @classmethod
    def _get_other_nodes(cls, base_node: BaseNode) -> NodeTypeList:
        other_nodes: NodeTypeList = []
        for node in base_node.other_nodes:
            response = cls._table().get_item(Key={"pk": node.pk, "sk": node.sk}, ConsistentRead=True)
            item = response.get("Item")
            if item and item.get("node_type") == NodeTypes.unique:
                other_nodes.append(UniqueNode.model_validate(item))
            elif item and item.get("node_type") == NodeTypes.query:
                other_nodes.append(QueryNode.model_validate(item))
        return other_nodes

    @classmethod
    def _write_transaction(cls, transaction_items: NodeTransactionList):
        items = [
            *[
                node.assemble_transaction_item_delete(cls._table_name())
                for node, action in transaction_items
                if action == "delete"
            ],
            *[
                node.assemble_transaction_item_put(cls._table_name())
                for node, action in transaction_items
                if action == "put"
            ],
        ]
        LOGGER.info(f"Writing transaction items: {items}")
        with transaction_write_error_boundary(items):
            cls._client().transact_write_items(TransactItems=items)

    @classmethod
    def _table(cls) -> Table:
        return cls.__immaterial_root_config__.dynamodb_provider.table

    @classmethod
    def _client(cls) -> DynamoDBClient:
        return cls.__immaterial_root_config__.dynamodb_provider.client

    @classmethod
    def _table_name(cls) -> str:
        return cls.__immaterial_root_config__.table_name

    @classmethod
    def _map_query_fields_to_index(cls, standard_query: StandardQuery) -> QueryIndex | None:
        for index in cls.__immaterial_model_config__.indices:
            if not index.index_type == "query":
                continue

            if len(standard_query.all_fields) > len(index.all_fields):
                continue

            # ensure the fields of the standard query at least match the partition fields on the index
            if standard_query.all_fields[: len(index.partition_fields)] != index.partition_fields:
                continue

            # ensure that the index contains all the fields of the standard query
            if index.all_fields[: len(standard_query.all_fields)] != standard_query.all_fields:
                continue

            return index

        return None


def materialize_model(model: Model) -> NodeTypeList:
    nodes: NodeTypeList = []
    model = model.model_copy()
    model.encrypt_fields()

    for index in model.__immaterial_model_config__.indices:
        if index.index_type == "unique":
            field_values = model.fetch_field_values(index.unique_fields)
            unique_node = UniqueNode.create(
                entity_name=model.immaterial_model_name(),
                entity_id=model.id,
                fields=serialize_field_values_for_dynamo(field_values),
            )
            nodes.append(unique_node)

        elif index.index_type == "query":
            partition_field_values = model.fetch_field_values(index.partition_fields)
            sort_field_values = model.fetch_field_values(index.sort_fields)
            index_node = QueryNode.create(
                entity_name=model.immaterial_model_name(),
                entity_id=model.id,
                partition_fields=serialize_field_values_for_dynamo(partition_field_values),
                sort_fields=serialize_field_values_for_dynamo(sort_field_values),
                raw_data=model.model_dump_json(),
            )
            nodes.append(index_node)

    other_nodes = [PrimaryKey(pk=node.pk, sk=node.sk) for node in nodes]

    nodes.append(
        BaseNode(
            entity_name=model.immaterial_model_name(),
            entity_id=model.id,
            base_node_id=model.id,
            raw_data=model.model_dump_json(),
            pk=model.id,
            sk=model.id,
            other_nodes=other_nodes,
        )
    )

    return nodes
