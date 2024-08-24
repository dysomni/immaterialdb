from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import cached_property
from time import sleep

import boto3
import ulid
from botocore.exceptions import ClientError
from mypy_boto3_dynamodb import DynamoDBServiceResource
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_dynamodb.service_resource import Table

from immaterialdb.constants import LOGGER


class DynamodbConnectionProvider:
    table_name: str
    region: str

    def __init__(self, table_name: str, region: str):
        self.table_name = table_name
        self.region = region

    @cached_property
    def client(self) -> DynamoDBClient:
        return boto3.client("dynamodb", region_name=self.region)

    @cached_property
    def resource(self) -> DynamoDBServiceResource:
        return boto3.resource("dynamodb", region_name=self.region)

    @cached_property
    def table(self) -> Table:
        return self.resource.Table(self.table_name)

    @contextmanager
    def lock(self, id: str, ttl: int = 15, wait: int = 5):
        start_time = now = datetime.now(timezone.utc)
        end_time = start_time + timedelta(seconds=wait)
        expiration_time = now + timedelta(seconds=ttl)
        lock_value = ulid.new().str

        while now <= end_time:
            try:
                LOGGER.debug(f"Attempting to acquire lock for {id}")
                self.table.put_item(
                    Item={"pk": id, "sk": lock_value, "expire_time": expiration_time.isoformat()},
                    ConditionExpression="attribute_not_exists(pk) OR expire_time < :now",
                    ExpressionAttributeValues={":now": now.isoformat()},
                )
                LOGGER.info(f"Lock acquired for {id}")
                break
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    LOGGER.info(f"Lock held for {id}, retrying...")
                    sleep(0.5)
                    now = datetime.now(timezone.utc)
                else:
                    raise
        else:
            LOGGER.error(f"Failed to acquire lock for {id}")
            raise Exception(f"Lock is already held for key {id}")

        try:
            yield
        finally:
            try:
                # Release the lock
                self.table.delete_item(Key={"pk": id, "sk": lock_value})
                LOGGER.debug(f"Lock released for {id}")
            except ClientError as e:
                LOGGER.warning(f"Failed to release lock for {id}, {e}")

    def create_table(self):
        table = self.resource.create_table(
            TableName=self.table_name,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName=self.table_name)
        LOGGER.info(f"Table {self.table_name} created")
