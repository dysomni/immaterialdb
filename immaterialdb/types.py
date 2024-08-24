from typing import Any, NamedTuple

FieldValue = NamedTuple("FieldValue", [("name", str), ("value", Any)])
PrimaryKey = NamedTuple("PrimaryKey", [("pk", str), ("sk", str)])
PrimaryKeys = list[PrimaryKey]
