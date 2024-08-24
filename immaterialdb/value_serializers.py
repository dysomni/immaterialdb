from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

import ulid

from immaterialdb.constants import LOGGER
from immaterialdb.types import FieldValue, PrimaryKey

INT_MAX_LENGTH = 20
FLOAT_INT_MAX_LENGTH = 10
FLOAT_FRAC_MAX_LENGTH = 10


def serialize_for_query_node_primary_key(
    model_name: str, pk_fields: list[FieldValue], sk_fields: list[FieldValue]
) -> PrimaryKey:
    return PrimaryKey("", "")


def serialize_for_unique_node_primary_key(
    model_name: str, unique_fields: list[FieldValue]
) -> PrimaryKey:
    return PrimaryKey("", "")


def serialize_for_index(
    value: Any, ensure_lexigraphic_sortability: bool = False
) -> str:
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


def int_to_lexicographic_string(n: int, width=INT_MAX_LENGTH) -> str:
    if n < 0:
        sign = "0"  # Use '0' for negative numbers
        n = abs(n)
        n_padded = str(10**width - n).zfill(width)
    else:
        sign = "1"  # Use '1' for positive numbers
        n_padded = str(n).zfill(width)

    return f"{sign}{n_padded}"


def float_to_lexicographic_string(
    f: float, int_width=FLOAT_INT_MAX_LENGTH, frac_width=FLOAT_FRAC_MAX_LENGTH
):
    if f < 0:
        sign = "0"  # Use '0' for negative numbers
        f = -f  # Make the number positive for easier formatting
        integer_part, fractional_part = f"{f:.{frac_width}f}".split(".")
        # Reverse the digits for negative numbers
        integer_part_padded = str(10**int_width - int(integer_part)).zfill(int_width)
        fractional_part_padded = str(10**frac_width - int(fractional_part)).zfill(
            frac_width
        )
    else:
        sign = "1"  # Use '1' for positive numbers
        integer_part, fractional_part = f"{f:.{frac_width}f}".split(".")
        # Pad the integer part with zeros
        integer_part_padded = integer_part.zfill(int_width)
        fractional_part_padded = fractional_part.ljust(frac_width, "0")

    return f"{sign}{integer_part_padded}.{fractional_part_padded}"


def decimal_to_lexicographic_string(
    d: Decimal, int_width=INT_MAX_LENGTH, frac_width=INT_MAX_LENGTH
):
    if d < 0:
        sign = "0"  # Use '0' for negative numbers
        d = -d
        integer_part, fractional_part = str(d).split(".")
        # Reverse the digits for negative numbers
        integer_part_padded = str(10**int_width - int(integer_part)).zfill(int_width)
        fractional_part_padded = str(10**frac_width - int(fractional_part)).zfill(
            frac_width
        )
    else:
        sign = "1"
        integer_part, fractional_part = str(d).split(".")
        integer_part_padded = integer_part.zfill(int_width)
        fractional_part_padded = fractional_part.ljust(frac_width, "0")

    return f"{sign}{integer_part_padded}.{fractional_part_padded}"
