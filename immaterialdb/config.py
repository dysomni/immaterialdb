from typing import Callable, Protocol, Type, TypeVar

from immaterialdb.constants import SEPERATOR
from immaterialdb.dynamo_provider import DynamodbConnectionProvider
from immaterialdb.errors import ModelMisconfigurationError
from immaterialdb.model import IndicesType, Model, ModelConfig, QueryIndex, UniqueIndex


class RootConfig:
    """
    The root configuration object for an immaterialdb application.

    RootConfig manages the global settings, DynamoDB connection, and model registration for an immaterialdb instance. It acts as the entry point for configuring your database, registering models and indices, and setting up encryption/decryption for sensitive fields.

    Attributes:
        table_name (str):
            The name of the DynamoDB table used for storage.
        registered_models (dict[str, Type[Model]]):
            A mapping of model names to their registered model classes.
        dynamodb_provider (DynamodbConnectionProvider):
            The provider for DynamoDB connections and table management.

    Methods:
        decorators (ImmaterialDecorators):
            Provides decorators for registering models, encryption, and decryption functions.

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
        >>> # Optionally, register encryption/decryption
        >>> @db.decorators.register_encryption
        ... def encrypt_string(text: str) -> str:
        ...     return text[::-1]  # Example only!
        >>> @db.decorators.register_decryption
        ... def decrypt_string(text: str) -> str:
        ...     return text[::-1]
    """

    table_name: str
    registered_models: dict[str, Type[Model]]
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


class EncryptionFuncType(Protocol):
    def __call__(self, text: str) -> str: ...


ModelType = TypeVar("ModelType", bound=Model)


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

    def register_model(
        self,
        indices: IndicesType | None = None,
        encrypted_fields: list[str] | None = None,
        auto_decrypt: bool = True,
        counter_fields: list[str] | None = None,
    ) -> Callable[[Type[ModelType]], Type[ModelType]]:
        _validate_indices(indices) if indices else None

        def decorator(model_cls: Type[ModelType]) -> Type[ModelType]:
            # TODO: validate field names are on model
            model_cls.__immaterial_model_config__ = ModelConfig(
                root_config=self.config,
                indices=indices or [],
                encrypted_fields=encrypted_fields,
                auto_decrypt=auto_decrypt,
                counter_fields=counter_fields,
            )
            model_cls.__immaterial_root_config__ = self.config
            self.config.registered_models[model_cls.immaterial_model_name()] = model_cls
            return model_cls

        return decorator


def _validate_indices(indices: IndicesType):
    # ensure unique indices arent redundant - eg a unique index of name, age is redundant if there is a unique index of name
    unique_indices = [index for index in indices if isinstance(index, UniqueIndex)]
    for unique_index in unique_indices:
        # find another unique index that is a subset of this one
        for other_unique_index in unique_indices:
            if (
                set(unique_index.unique_fields).issubset(set(other_unique_index.unique_fields))
                and unique_index.unique_fields != other_unique_index.unique_fields
            ):
                raise ModelMisconfigurationError(
                    f"Unique index {other_unique_index.unique_fields} is redundant to {unique_index.unique_fields} because the latter is a subset of the former"
                )

    # ensure query indices arent redundant - eg a query with a the same pk and an sk of age is redundant to a query with a sk of age, name
    query_indices = [index for index in indices if isinstance(index, QueryIndex)]
    for query_index in query_indices:
        for other_query_index in query_indices:
            if (
                query_index.partition_fields == other_query_index.partition_fields
                and set(query_index.sort_fields).issubset(set(other_query_index.sort_fields))
                and query_index.sort_fields != other_query_index.sort_fields
            ):
                raise ModelMisconfigurationError(
                    f"Query index pk {query_index.partition_fields} sk {query_index.sort_fields} is redundant to pk {other_query_index.partition_fields} sk {other_query_index.sort_fields} because the former is a subset of the latter"
                )


# for a later day - the ability for immaterialdb to manage its own internal models for keeping track of usage
# INTERNAL_MODEL_PREFIX = "immaterial_internal_"
# INTERNAL_MODEL_RECORD_NAME = INTERNAL_MODEL_PREFIX + "entity_types"


# def create_internal_models(root_config: RootConfig):
#     @root_config.decorators.register_model()
#     class ImmaterialInternalModelRecord(Model):
#         __immaterial_model_name__ = INTERNAL_MODEL_RECORD_NAME
#         entity_name: str
#         entity_schema: dict
#         indicies: IndicesType

#         @classmethod
#         def gen_id(cls, entity_name: str) -> str:
#             return f"{cls.immaterial_model_name()}{SEPERATOR}{entity_name}"
