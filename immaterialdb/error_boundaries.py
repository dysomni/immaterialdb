from contextlib import contextmanager

import botocore.exceptions
from mypy_boto3_dynamodb.type_defs import TransactWriteItemTypeDef

from immaterialdb.errors import CounterNotSavedError, RecordNotUniqueError
from immaterialdb.object_helpers import safe_dot_access


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
                    and safe_dot_access(item, "Put.ConditionExpression") == "attribute_not_exists(pk)"
                    and safe_dot_access(item, "Put.Item.unique_node_id.S")
                ):
                    pk = safe_dot_access(item, "Put.Item.pk.S")
                    raise RecordNotUniqueError(f"Record already exists with unique key {pk}")

                if (
                    "Code" in reason
                    and reason["Code"] == "ConditionalCheckFailed"
                    and safe_dot_access(item, "Update.ConditionExpression") == "attribute_exists(pk)"
                    and safe_dot_access(item, "Update.UpdateExpression") == "ADD #count :amount"
                ):
                    pk = safe_dot_access(item, "Update.Key.pk.S")
                    raise CounterNotSavedError(f"Record must be saved before incrementing counter {pk}")

        raise e
