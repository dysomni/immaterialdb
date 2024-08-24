import hashlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, ClassVar, Literal, Self

import ulid
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.service_resource import Table
from pydantic import BaseModel, Field, model_validator

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
from immaterialdb.query import Querier, Query, QueryResult, QueryTypes, StandardQuery
from immaterialdb.types import FieldValue, PrimaryKey

if TYPE_CHECKING:
    from immaterialdb.config import RootConfig


class UniqueIndex(BaseModel):
    node_name: Literal["unique_fields"]
    unique_fields: list[str]


class QueryIndex(BaseModel):
    node_name: Literal["index_fields"]
    partition_fields: list[str]
    sort_fields: list[str]

    @property
    def all_fields(self) -> list[str]:
        return self.partition_fields + self.sort_fields

    @property
    def gen_pk(self) -> str:
        return f"index_fields_{self.partition_fields}_{self.sort_fields}"


Indices = list[UniqueIndex | QueryIndex]


class ModelConfig:
    root_config: "RootConfig"
    indices: Indices

    def __init__(
        self,
        root_config: "RootConfig",
        indices: Indices,
    ):
        self.root_config = root_config
        self.indices = indices


class Model(BaseModel):
    __immaterial_root_config__: ClassVar["RootConfig"]
    __immaterial_model_config__: ClassVar[ModelConfig]
    __immaterial_model_name__: ClassVar[str | None] = None

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
                    f"Field {field} is not present in the model {self.model_name()}"
                ) from e

        return field_values

    @classmethod
    def model_name(cls) -> str:
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

        return cls.model_validate_json(base_node.raw_data)

    @classmethod
    def query(
        cls, query: QueryTypes, descending: bool = False, max_items: int | None = None, lazy: bool = True
    ) -> QueryResult[Self]:
        querier = Querier(
            cls, query, cls.__immaterial_root_config__.dynamodb_provider, scan_index_forward=not descending
        )
        return QueryResult(querier=querier, lazy=lazy, max_items=max_items)

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
        cls._client().transact_write_items(
            TransactItems=[
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
        )

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
            if not index.node_name == "index_fields":
                continue

            if len(standard_query.all_fields) > len(index.all_fields):
                continue

            # ensure the fields of the standard query at least match the partition fields on the index
            if standard_query.all_fields[: len(index.partition_fields)] != index.partition_fields:
                continue

            # ensure that the index contains all the fields of the standard query
            if index.all_fields[: len(standard_query.all_fields)] != index.all_fields:
                continue

            return index

        return None

    class Config:
        validate_assignment = True


def materialize_model(model: Model) -> NodeTypeList:
    nodes: NodeTypeList = []

    for index in model.__immaterial_model_config__.indices:
        if index.node_name == "unique_fields":
            field_values = model.fetch_field_values(index.unique_fields)
            unique_node = UniqueNode.create(
                model_name=model.model_name(),
                entity_id=model.id,
                fields=field_values,
            )
            nodes.append(unique_node)

        elif index.node_name == "index_fields":
            partition_field_values = model.fetch_field_values(index.partition_fields)
            sort_field_values = model.fetch_field_values(index.sort_fields)
            index_node = QueryNode.create(
                model_name=model.model_name(),
                entity_id=model.id,
                partition_fields=partition_field_values,
                sort_fields=sort_field_values,
            )
            nodes.append(index_node)

    other_nodes = [PrimaryKey(pk=node.pk, sk=node.sk) for node in nodes]

    nodes.append(
        BaseNode(
            model_name=model.model_name(),
            entity_id=model.id,
            raw_data=model.model_dump_json(),
            pk=model.id,
            sk=model.id,
            other_nodes=other_nodes,
        )
    )

    return nodes
