"""
Encode Python values as Query v3 parameter entries.

The server wants every value as a JSON string (NULL is JSON null) together
with a type name it recognises; raw JSON numbers/booleans and unknown type
names are rejected with "invalid JSON query parameters". Numeric carries
`precision`/`scale` keys, and arrays carry an `inner` type object.
"""

import json
import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict

from ..exceptions import ProgrammingError

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_MAX_NUMERIC_PRECISION = 38


def _numeric(value: Decimal) -> Dict[str, Any]:
    if not value.is_finite():
        raise ProgrammingError(f"Cannot bind {value!r}: numeric parameters must be finite")
    sign, digits, exponent = value.as_tuple()
    scale = max(-exponent, 0)
    precision = max(len(digits) + max(exponent, 0), scale, 1)
    if precision > _MAX_NUMERIC_PRECISION:
        raise ProgrammingError(f"Cannot bind {value!r}: more than {_MAX_NUMERIC_PRECISION} digits")
    return {"type": "numeric", "precision": precision, "scale": scale, "value": format(value, "f")}


def encode_parameter(value: Any) -> Dict[str, Any]:
    """Return the v3 parameter entry for a Python value (raises ProgrammingError if unsupported)."""
    if value is None:
        return {"type": "varchar", "value": None}
    if isinstance(value, bool):
        return {"type": "bool", "value": "true" if value else "false"}
    if isinstance(value, int):
        if _INT64_MIN <= value <= _INT64_MAX:
            return {"type": "bigint", "value": int.__repr__(value)}
        return _numeric(Decimal(value))
    if isinstance(value, float):
        if math.isnan(value):
            text = "NaN"
        elif math.isinf(value):
            text = "Infinity" if value > 0 else "-Infinity"
        else:
            text = float.__repr__(value)
        return {"type": "float8", "value": text}
    if isinstance(value, Decimal):
        return _numeric(value)
    if isinstance(value, datetime):  # before date: datetime is a date subclass
        return {"type": "timestamp" if value.tzinfo is None else "timestamptz", "value": value.isoformat()}
    if isinstance(value, date):
        return {"type": "date", "value": value.isoformat()}
    if isinstance(value, time):
        if value.tzinfo is not None:
            raise ProgrammingError(f"Cannot bind {value!r}: the server has no time-with-time-zone type")
        return {"type": "time", "value": value.isoformat()}
    if isinstance(value, str):
        return {"type": "varchar", "value": value}
    if isinstance(value, (list, tuple)):
        return _array(value)
    raise ProgrammingError(f"Cannot bind parameter of type {type(value).__name__}: {value!r}")


def _array(values: Any) -> Dict[str, Any]:
    inner_entries = [encode_parameter(v) for v in values if v is not None]
    types = {e["type"] for e in inner_entries}
    if len(types) > 1:
        raise ProgrammingError(f"Cannot bind array with mixed element types: {values!r}")
    inner = {k: v for k, v in inner_entries[0].items() if k != "value"} if inner_entries else {"type": "varchar"}
    if inner["type"] == "array":
        raise ProgrammingError("Cannot bind nested arrays")
    if inner["type"] == "numeric":
        inner = _widest_numeric(inner_entries, values)
    elements = [_array_element(v) for v in values]
    return {"type": "array", "inner": inner, "value": json.dumps(elements)}


def _widest_numeric(entries: Any, values: Any) -> Dict[str, Any]:
    """One numeric type that holds every element: the most integer digits and the most decimals."""
    scale = max(e["scale"] for e in entries)
    precision = max(e["precision"] - e["scale"] for e in entries) + scale
    if precision > _MAX_NUMERIC_PRECISION:
        raise ProgrammingError(
            f"Cannot bind array {values!r}: its elements need more than {_MAX_NUMERIC_PRECISION} digits"
        )
    return {"type": "numeric", "precision": precision, "scale": scale}


def _array_element(value: Any) -> Any:
    """Array elements are JSON-native for bool/int/float, strings otherwise."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int) and _INT64_MIN <= value <= _INT64_MAX:
        return int(value)
    if isinstance(value, float) and math.isfinite(value):
        return float(value)
    return encode_parameter(value)["value"]
