from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

import ulid

from immaterialdb.constants import LOGGER, SEPERATOR
from immaterialdb.types import FieldValue, PrimaryKey

INT_MAX_LENGTH = 20
FLOAT_INT_MAX_LENGTH = 10
FLOAT_FRAC_MAX_LENGTH = 10


def serialize_for_query_node_primary_key(
    entity_name: str, entity_id: str, pk_fields: list[FieldValue], sk_fields: list[FieldValue]
) -> PrimaryKey:
    pk = serialize_for_query_node_partition_key(entity_name, pk_fields, [field.name for field in sk_fields])
    sort_field_values = serialize_for_query_node_partial_sort_key(sk_fields)
    sk = f"{sort_field_values}{SEPERATOR}{entity_id}"
    return PrimaryKey(pk, sk)


def serialize_for_query_node_partition_key(
    entity_name: str, pk_fields: list[FieldValue], sort_field_names: list[str]
) -> str:
    key_value_pairs = ",".join([f"{field.name}={serialize_for_index(field.value)}" for field in pk_fields])
    pk = f"{entity_name}[{key_value_pairs}][{','.join(sort_field_names)}]"
    return pk


def serialize_for_query_node_partial_sort_key(sk_fields: list[FieldValue]) -> str:
    sort_field_values = SEPERATOR.join(
        [serialize_for_index(field.value, ensure_lexigraphic_sortability=True) for field in sk_fields]
    )
    return f"{SEPERATOR}{sort_field_values}"


def serialize_for_unique_node_primary_key(entity_name: str, unique_fields: list[FieldValue]) -> PrimaryKey:
    key_value_pairs = ",".join([f"{field.name}={serialize_for_index(field.value)}" for field in unique_fields])
    pk = f"{entity_name}({key_value_pairs})"
    sk = "unique"
    return PrimaryKey(pk, sk)


def serialize_for_index(value: Any, ensure_lexigraphic_sortability: bool = False) -> str:
    if isinstance(value, str):
        return value
    elif isinstance(value, int):
        if ensure_lexigraphic_sortability:
            return int_to_lexicographic_string(value)
        return str(value)
    elif isinstance(value, float):
        if ensure_lexigraphic_sortability:
            return float_to_lexicographic_string(value)
        return str(value)
    elif isinstance(value, Decimal):
        if ensure_lexigraphic_sortability:
            return decimal_to_lexicographic_string(value)
        return str(value)
    elif isinstance(value, bool):
        return "true" if value else "false"
    elif value is None:
        return "null"
    elif isinstance(value, datetime):
        # if there isnt a timezone, assume it is UTC
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, UUID):
        return str(value)
    elif isinstance(value, ulid.ULID):
        return value.str
    elif isinstance(value, Enum):
        return value.value
    else:
        LOGGER.warning(
            f"Value {value} of type {value.__class__.__name__} is not a currently supported type for "
            "serialization into an index. Queries may be inconsistent."
        )
        return str(value)


def serialize_for_dynamo_value(value: Any) -> Any:
    if isinstance(value, datetime):
        # if there isnt a timezone, assume it is UTC
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, UUID):
        return str(value)
    elif isinstance(value, ulid.ULID):
        return value.str
    elif isinstance(value, Enum):
        return value.value
    else:
        return value


def serialize_field_values_for_dynamo(field_values: list[FieldValue]) -> list[FieldValue]:
    return [FieldValue(field.name, serialize_for_dynamo_value(field.value)) for field in field_values]


def int_to_lexicographic_string(n: int, width=INT_MAX_LENGTH) -> str:
    if n < 0:
        sign = "0"  # Use '0' for negative numbers
        n = abs(n)
        n_padded = str(10**width - n).zfill(width)
    else:
        sign = "1"  # Use '1' for positive numbers
        n_padded = str(n).zfill(width)

    return f"{sign}{n_padded}"


def float_to_lexicographic_string(f: float, int_width=FLOAT_INT_MAX_LENGTH, frac_width=FLOAT_FRAC_MAX_LENGTH):
    if f < 0:
        sign = "0"  # Use '0' for negative numbers
        f = -f  # Make the number positive for easier formatting
        integer_part, fractional_part = f"{f:.{frac_width}f}".split(".")
        # Reverse the digits for negative numbers
        integer_part_padded = str(10**int_width - int(integer_part)).zfill(int_width)
        fractional_part_padded = str(10**frac_width - int(fractional_part)).zfill(frac_width)
    else:
        sign = "1"  # Use '1' for positive numbers
        integer_part, fractional_part = f"{f:.{frac_width}f}".split(".")
        # Pad the integer part with zeros
        integer_part_padded = integer_part.zfill(int_width)
        fractional_part_padded = fractional_part.ljust(frac_width, "0")

    return f"{sign}{integer_part_padded}.{fractional_part_padded}"


def decimal_to_lexicographic_string(d: Decimal, int_width=INT_MAX_LENGTH, frac_width=INT_MAX_LENGTH):
    if d < 0:
        sign = "0"  # Use '0' for negative numbers
        d = -d
        integer_part, fractional_part = str(d).split(".")
        # Reverse the digits for negative numbers
        integer_part_padded = str(10**int_width - int(integer_part)).zfill(int_width)
        fractional_part_padded = str(10**frac_width - int(fractional_part)).zfill(frac_width)
    else:
        sign = "1"
        integer_part, fractional_part = str(d).split(".")
        integer_part_padded = integer_part.zfill(int_width)
        fractional_part_padded = fractional_part.ljust(frac_width, "0")

    return f"{sign}{integer_part_padded}.{fractional_part_padded}"
