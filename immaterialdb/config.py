from typing import Callable, Protocol, Type, TypeVar

from immaterialdb.constants import SEPERATOR
from immaterialdb.dynamo_provider import DynamodbConnectionProvider
from immaterialdb.model import IndicesType, Model, ModelConfig, UniqueIndex


class RootConfig:
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
        self, indices: IndicesType | None = None, encrypted_fields: list[str] | None = None, auto_decrypt: bool = True
    ) -> Callable[[Type[ModelType]], Type[ModelType]]:
        # TODO - validate no overlapping indices
        # TODO - auto ordering of indices for best match (prefering sort key over partition key)
        def decorator(model_cls: Type[ModelType]) -> Type[ModelType]:
            model_cls.__immaterial_model_config__ = ModelConfig(
                root_config=self.config,
                indices=indices or [],
                encrypted_fields=encrypted_fields,
                auto_decrypt=auto_decrypt,
            )
            model_cls.__immaterial_root_config__ = self.config
            self.config.registered_models[model_cls.immaterial_model_name()] = model_cls
            return model_cls

        return decorator


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
