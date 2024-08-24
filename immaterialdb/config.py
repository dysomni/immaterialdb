import hashlib
from datetime import datetime, timezone
from typing import Any, Callable, ClassVar, Literal, Protocol, Self, Type

import ulid
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.service_resource import Table
from pydantic import BaseModel, Field, model_validator

from immaterialdb.dynamo_provider import DynamodbConnectionProvider
from immaterialdb.nodes import (
    BaseNode,
    NodeTransactionItem,
    NodeTransactionList,
    NodeTypeList,
    NodeTypes,
    QueryNode,
    UniqueNode,
)
from immaterialdb.types import FieldValue, PrimaryKey


class RootConfig:
    table_name: str
    registered_models: dict[str, Type["Model"]]
    dynamodb_provider: DynamodbConnectionProvider

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.registered_models = {}
        self.dynamodb_provider = DynamodbConnectionProvider(table_name=table_name, region="us-east-1")

    @staticmethod
    def _encrypt_string(text: str) -> str:
        register_encryption_message = (
            "You must register an encryption function before using encryption. "
            "Use the `register_encryption` decorator to register an encryption function. "
            "This function should have the following signature: `def encrypt_string(text: str) -> str`."
        )
        raise NotImplementedError(register_encryption_message)

    @staticmethod
    def _decrypt_string(text: str) -> str:
        register_decryption_message = (
            "You must register a decryption function before using decryption. "
            f"Use the `register_decryption` decorator to register a decryption function. "
            "This function should have the following signature: `def decrypt_string(text: str) -> str`."
        )
        raise NotImplementedError(register_decryption_message)

    @property
    def decorators(self):
        return ImmaterialDecorators(self)


class UniqueIndex(BaseModel):
    node_name: Literal["unique_fields"]
    unique_fields: list[str]


class QueryIndex(BaseModel):
    node_name: Literal["index_fields"]
    partition_fields: list[str]
    sort_fields: list[str]


Indices = list[UniqueIndex | QueryIndex]


class ModelConfig:
    root_config: RootConfig
    indices: Indices

    def __init__(
        self,
        root_config: RootConfig,
        indices: Indices,
    ):
        self.root_config = root_config
        self.indices = indices


class FieldMisconfigurationError(Exception):
    pass


class Model(BaseModel):
    __immaterial_root_config__: ClassVar[RootConfig]
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
    def query(cls):
        pass

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


class EncryptionFuncType(Protocol):
    def __call__(self, text: str) -> str: ...


class ImmaterialDecorators:
    def __init__(self, config: RootConfig):
        self.config = config

    @property
    def register_encryption(self) -> Callable[[EncryptionFuncType], EncryptionFuncType]:
        def decorator(func: EncryptionFuncType) -> EncryptionFuncType:
            setattr(self.config, "_encrypt_string", func)
            return func

        return decorator

    @property
    def register_decryption(self) -> Callable[[EncryptionFuncType], EncryptionFuncType]:
        def decorator(func: EncryptionFuncType) -> EncryptionFuncType:
            setattr(self.config, "_decrypt_string", func)
            return func

        return decorator

    def register_model(self, indices: Indices) -> Callable[[Type[Model]], Type[Model]]:
        def decorator(model_cls: Type[Model]) -> Type[Model]:
            model_cls.__immaterial_model_config__ = ModelConfig(root_config=self.config, indices=indices)
            model_cls.__immaterial_root_config__ = self.config
            self.config.registered_models[model_cls.model_name()] = model_cls
            return model_cls

        return decorator


IMMATERIAL = RootConfig("my_table")


@IMMATERIAL.decorators.register_encryption
def encrypt_string(text: str) -> str:
    return text[::-1]


@IMMATERIAL.decorators.register_decryption
def decrypt_string(text: str) -> str:
    return text[::-1]


@IMMATERIAL.decorators.register_model([])
class User(Model):
    username: str
    email: str
    password: str
