from typing import Callable, Protocol, Type

from immaterialdb.dynamo_provider import DynamodbConnectionProvider
from immaterialdb.model import Indices, Model, ModelConfig


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

    def register_model(self, indices: Indices | None = None) -> Callable[[Type[Model]], Type[Model]]:
        def decorator(model_cls: Type[Model]) -> Type[Model]:
            model_cls.__immaterial_model_config__ = ModelConfig(root_config=self.config, indices=indices or [])
            model_cls.__immaterial_root_config__ = self.config
            self.config.registered_models[model_cls.immaterial_model_name()] = model_cls
            return model_cls

        return decorator


# IMMATERIAL = RootConfig("my_table")


# @IMMATERIAL.decorators.register_encryption
# def encrypt_string(text: str) -> str:
#     return text[::-1]


# @IMMATERIAL.decorators.register_decryption
# def decrypt_string(text: str) -> str:
#     return text[::-1]


# @IMMATERIAL.decorators.register_model([])
# class User(Model):
#     username: str
#     email: str
#     password: str
