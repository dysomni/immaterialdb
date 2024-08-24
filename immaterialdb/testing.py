from contextlib import contextmanager

from moto import mock_dynamodb

from immaterialdb.config import RootConfig


@contextmanager
def mock_immaterialdb(root_config: RootConfig):
    with mock_dynamodb():
        try:
            del root_config.dynamodb_provider.client
        except AttributeError:
            pass
        try:
            del root_config.dynamodb_provider.resource
        except AttributeError:
            pass
        root_config.dynamodb_provider.create_table()
        yield
