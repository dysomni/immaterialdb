from typing import Any, NamedTuple

from mypy_boto3_dynamodb.type_defs import TableAttributeValueTypeDef

FieldValue = NamedTuple("FieldValue", [("name", str), ("value", Any)])
PrimaryKey = NamedTuple("PrimaryKey", [("pk", str), ("sk", str)])
PrimaryKeys = list[PrimaryKey]


LastEvaluatedKey = dict[str, TableAttributeValueTypeDef]
