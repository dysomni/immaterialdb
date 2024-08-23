import hashlib
from datetime import datetime, timezone
from enum import StrEnum, auto
from typing import Any, Callable, ClassVar, Literal, NamedTuple, Protocol, Self, Type

import ulid
from pydantic import BaseModel, Field, model_validator


class RootConfig:
    table_name: str
    registered_models: dict[str, Type["Model"]]

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.registered_models = {}

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
        return hashlib.md5(
            self.model_dump_json(exclude={"updated_hash", "updated_at"}).encode("utf-8")
        ).hexdigest()

    @model_validator(mode="after")
    def check_for_updates(self) -> Self:
        new_hash = self.hash_for_update
        if self.updated_hash != new_hash:
            self.updated_hash = new_hash
            self.updated_at = datetime.now(timezone.utc)

        return self

    def fetch_values_from_field_list(self, field_list: list[str]) -> list[Any]:
        values = []
        for field in field_list:
            try:
                values.append(getattr(self, field))
            except AttributeError as e:
                raise FieldMisconfigurationError(
                    f"Field {field} is not present in the model {self.model_name()}"
                ) from e

        return values

    @classmethod
    def model_name(cls) -> str:
        return cls.__immaterial_model_name__ or cls.__name__

    def save(self):
        pass

    @classmethod
    def get_by_id(cls, id: str):
        pass

    @classmethod
    def query(cls):
        pass

    def delete(self):
        pass

    @classmethod
    def delete_by_id(cls, id: str):
        pass

    class Config:
        validate_assignment = True


class NodeTypes(StrEnum):
    base = auto()
    unique = auto()
    query = auto()


class Node(BaseModel):
    node_type: NodeTypes
    model_name: str
    entity_id: str
    pk: str
    sk: str


PrimaryKey = NamedTuple("PrimaryKey", [("pk", str), ("sk", str)])
PrimaryKeys = list[PrimaryKey]


class BaseNode(Node):
    node_type: Literal[NodeTypes.base] = NodeTypes.base
    base_node: Literal[NodeTypes.base] = NodeTypes.base
    raw_data: str
    other_nodes: PrimaryKeys


class UniqueNode(Node):
    node_type: Literal[NodeTypes.unique] = NodeTypes.unique
    unique_node: Literal[NodeTypes.unique] = NodeTypes.unique
    field_names: list[str]
    field_values: list[str]

    @model_validator(mode="before")
    @classmethod
    def gen_pk_and_sk(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        field_names: list[str] = data.get("field_names", [])
        field_values: list[str] = data.get("field_values", [])
        fields_and_values = zip(field_names, field_values)
        pk_string = "##".join(
            map(lambda x: f"{x[0]}@{x[1]}", fields_and_values)
        )  # TODO: serialize
        data["pk"] = data.get("model_name", "") + "{" + pk_string + "}"
        data["sk"] = data.get("entity_id")
        return data


class QueryNode(Node):
    node_type: Literal[NodeTypes.query] = NodeTypes.query
    query_node: Literal[NodeTypes.query] = NodeTypes.query
    partition_field_names: list[str]
    partition_field_values: list[str]
    sort_field_names: list[str]
    sort_field_values: list[str]

    @property
    def pk(self) -> str:
        return self.partition_field_values[0]

    @property
    def sk(self) -> str:
        return self.sort_field_values[0]


NodeType = BaseNode | UniqueNode | QueryNode
NodeTypeList = list[NodeType]


def materialize_model(model: Model) -> NodeTypeList:
    nodes: NodeTypeList = []

    for index in model.__immaterial_model_config__.indices:
        if index.node_name == "unique_fields":
            field_values = model.fetch_values_from_field_list(index.unique_fields)
            field_and_values = zip(index.unique_fields, field_values)
            unique_node = UniqueNode(
                model_name=model.model_name(),
                entity_id=model.id,
                pk=model.id,
                sk=model.id,
            )
            nodes.append(unique_node)

        elif index.node_name == "index_fields":
            index_node = QueryNode(
                model_name=model.model_name(),
                entity_id=model.id,
                pk=model.id,
                sk="index",
            )
            nodes.append(index_node)
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
            model_cls.__immaterial_model_config__ = ModelConfig(
                root_config=self.config, indices=indices
            )
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
