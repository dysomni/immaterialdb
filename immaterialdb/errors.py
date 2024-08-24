from contextlib import contextmanager
from typing import NamedTuple

import botocore.exceptions
from mypy_boto3_dynamodb.type_defs import TransactWriteItemTypeDef

from immaterialdb.object_helpers import safe_dot_access


class FieldMisconfigurationError(Exception):
    pass


class QueryNotSupportedError(Exception):
    pass


class RecordNotUniqueError(Exception):
    pass


@contextmanager
def transaction_write_error_boundary(items: list[TransactWriteItemTypeDef]):
    try:
        yield
    except botocore.exceptions.ClientError as e:
        if (
            "Error" in e.response
            and "Code" in e.response["Error"]
            and e.response["Error"]["Code"] == "TransactionCanceledException"
            and "CancellationReasons" in e.response
        ):
            reasons = e.response["CancellationReasons"]
            transaction_reasons = zip(items, reasons)
            for item, reason in transaction_reasons:
                if (
                    "Code" in reason
                    and reason["Code"] == "ConditionalCheckFailed"
                    and "Put" in item
                    and "ConditionExpression" in item["Put"]
                    and "attribute_not_exists(pk)" in item["Put"]["ConditionExpression"]
                ):
                    pk = safe_dot_access(item, "Put.Item.pk.S")
                    entity_name = safe_dot_access(item, "Put.Item.entity_name.S")
                    raise RecordNotUniqueError(f"Record already exists with unique key {pk}")

        raise e
