"""
Type system for Salesforce Data Cloud Driver.

This module handles type conversions between Data Cloud types and Python types,
and implements DB-API 2.0 type objects.
"""

import json
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Optional

from dateutil import parser as dateutil_parser


# DB-API 2.0 Type Objects
# These are used to describe column types in cursor.description
class DBAPITypeObject:
    """Base class for DB-API 2.0 type objects."""

    def __init__(self, *values):
        self.values = frozenset(values)

    def __eq__(self, other):
        if isinstance(other, DBAPITypeObject):
            return self.values == other.values
        return other in self.values

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.values)

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(repr(v) for v in self.values)})"


# DB-API 2.0 mandated type objects
STRING = DBAPITypeObject("VARCHAR", "CHAR", "TEXT", "STRING", "CLOB")
BINARY = DBAPITypeObject("BINARY", "VARBINARY", "BLOB")
NUMBER = DBAPITypeObject("NUMERIC", "DECIMAL", "INTEGER", "SMALLINT", "BIGINT", "FLOAT", "DOUBLE")
DATETIME = DBAPITypeObject("TIMESTAMP", "TIMESTAMPTZ", "DATE", "TIME", "DATETIME")
ROWID = DBAPITypeObject("ROWID")


# Data Cloud type name to DB-API 2.0 type object mapping. The capitalized
# keys are the PG metadata catalog's vocabulary (_metadata_pg.py); the
# lowercase-only keys below are additional v3 Query API spec discriminators
# (HyperQueryV3Response.toV3SpecType) that have no capitalized counterpart.
DATACLOUD_TYPE_TO_DBAPI = {
    "Varchar": STRING,
    "Numeric": NUMBER,
    "Timestamp": DATETIME,
    "TimestampTZ": DATETIME,
    "Boolean": NUMBER,  # Booleans are often categorized as NUMBER in DB-API
    "Date": DATETIME,
    "Time": DATETIME,
    "Integer": NUMBER,
    "BigInt": NUMBER,
    "Float": NUMBER,
    "Double": NUMBER,
    "Text": STRING,
    "bool": NUMBER,
    "smallint": NUMBER,
    "float4": NUMBER,
    "float8": NUMBER,
    "oid": NUMBER,
    "char": STRING,
    "json": STRING,
    # No DB-API type object maps cleanly onto a time-span: Hyper intervals
    # have no fixed month/day ratio, so convert_datacloud_value passes them
    # through unconverted rather than coercing to timedelta/DATETIME.
    "interval": STRING,
    "bytea": BINARY,
}


# Case-insensitive index derived from the canonical map above. The off-core Query v3
# API returns lowercase type names (e.g. "varchar", "timestamptz"), while the Postgres
# metadata catalog (_metadata_pg.py) emits the canonical capitalized names. Normalizing
# the lookup key lets both resolve to the same DB-API type object.
_DATACLOUD_TYPE_TO_DBAPI_LOWER = {key.lower(): value for key, value in DATACLOUD_TYPE_TO_DBAPI.items()}


def convert_datacloud_value(value: Any, datacloud_type: str,
                           precision: Optional[int] = None,
                           scale: Optional[int] = None) -> Any:
    """
    Convert a value from Data Cloud type to Python type.

    Args:
        value: The value from Data Cloud API response
        datacloud_type: The Data Cloud type name (e.g., "Varchar", "Numeric", "TimestampTZ")
        precision: Optional precision for Numeric types
        scale: Optional scale for Numeric types

    Returns:
        The value converted to the appropriate Python type

    Raises:
        ValueError: If the value cannot be converted to the target type
    """
    # Handle NULL values
    if value is None:
        return None

    # v3 returns lowercase type names; the PG catalog emits capitalized ones.
    # Normalize so both resolve. `or ""` guards against a missing/None type.
    normalized_type = (datacloud_type or "").lower()

    try:
        # Varchar/char → str. char is physically indistinguishable from
        # varchar over Arrow (both arrive as a plain STRING column) and is a
        # plain string over JSON too, so it reuses the same conversion.
        if normalized_type in ("varchar", "char"):
            return str(value)

        # Numeric → Decimal/int/float
        elif normalized_type == "numeric":
            # If scale is 0, return as int
            if scale == 0:
                return int(value)
            # If precision/scale not specified or low precision, use float
            elif precision is None or precision <= 15:
                return float(value)
            # For high precision, use Decimal
            else:
                return Decimal(str(value))

        # Integer types → int. oid is Hyper's unsigned 32-bit id type; it has
        # no distinct Python representation, so it collapses into plain int.
        elif normalized_type in ("integer", "bigint", "smallint", "oid"):
            return int(value)

        # Float types → float. float4/float8 are the real v3 spec names for
        # REAL/DOUBLE PRECISION; "float"/"double" are kept for the PG
        # metadata catalog's capitalized "Float"/"Double".
        elif normalized_type in ("float", "double", "float4", "float8"):
            return float(value)

        # bytea → bytes, unchanged. Confirmed live that bytea only ever
        # arrives via the Arrow output format (the JSON sink rejects it
        # outright with a 501), where nanoarrow already decodes it to native
        # Python bytes -- no base64 decoding is needed here.
        elif normalized_type == "bytea":
            return value

        # json → parsed Python object, regardless of output format. The JSON
        # output format pre-parses json columns into native dicts/lists, but
        # the Arrow output format gives the raw JSON text as a plain string
        # (confirmed live) -- normalize the Arrow case so callers see the
        # same Python value either way. str(value) is deliberately avoided:
        # it would turn an already-parsed dict into its repr(), not JSON.
        elif normalized_type == "json":
            if isinstance(value, str):
                return json.loads(value)
            return value

        # interval → passed through unconverted. The wire representation
        # differs by output format (JSON: ISO-8601 duration string, e.g.
        # 'P1D'; Arrow: a raw (months, days, nanoseconds) tuple, confirmed
        # live) and neither maps losslessly onto a Python stdlib type
        # (timedelta cannot exactly represent month-based durations), so this
        # is a deliberate, documented pass-through rather than a conversion.
        elif normalized_type == "interval":
            return value

        # Timestamp / TimestampTZ → datetime (naive or tz-aware, respectively)
        elif normalized_type in ("timestamp", "timestamptz"):
            if isinstance(value, datetime):
                return value
            # Parse string timestamp
            return dateutil_parser.parse(value)

        # Date → date
        elif normalized_type == "date":
            if isinstance(value, date):
                return value
            # Parse string date
            dt = dateutil_parser.parse(value)
            return dt.date()

        # Time → time
        elif normalized_type == "time":
            if isinstance(value, time):
                return value
            # Parse string time
            dt = dateutil_parser.parse(value)
            return dt.time()

        # Boolean → bool
        elif normalized_type in ("boolean", "bool"):
            if isinstance(value, bool):
                return value
            # Handle string representations
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "t")
            return bool(value)

        # Text → str
        elif normalized_type == "text":
            return str(value)

        # Unknown type - return as-is
        else:
            return value

    except (ValueError, TypeError) as e:
        raise ValueError(
            f"Cannot convert value {value!r} to type {datacloud_type}: {e}"
        ) from e


def infer_sql_parameter_type(value: Any) -> str:
    """
    Infer the Data Cloud SQL parameter type from a Python value.

    This is used when converting named parameters to the sqlParameters array format.

    Args:
        value: Python value

    Returns:
        Data Cloud type name (e.g., "Varchar", "Numeric", "Boolean")
    """
    if value is None:
        return "Varchar"  # Default for NULL
    elif isinstance(value, bool):
        return "Boolean"
    elif isinstance(value, int):
        return "Numeric"
    elif isinstance(value, float):
        return "Numeric"
    elif isinstance(value, Decimal):
        return "Numeric"
    elif isinstance(value, datetime):
        return "TimestampTZ"
    elif isinstance(value, date):
        return "Date"
    elif isinstance(value, time):
        return "Time"
    elif isinstance(value, str):
        return "Varchar"
    else:
        # Default to Varchar for unknown types (will be stringified)
        return "Varchar"


def build_description_tuple(column_metadata: dict) -> tuple:
    """
    Build a DB-API 2.0 cursor.description tuple from column metadata.

    The description tuple format is:
    (name, type_code, display_size, internal_size, precision, scale, null_ok)

    Args:
        column_metadata: Column metadata from Data Cloud API response

    Returns:
        A 7-element tuple conforming to DB-API 2.0 specification
    """
    name = column_metadata.get("name")
    datacloud_type = column_metadata.get("type", "Varchar")
    nullable = column_metadata.get("nullable", True)
    precision = column_metadata.get("precision")
    scale = column_metadata.get("scale")

    # Get DB-API type code (case-insensitive: v3 lowercase + PG-catalog capitalized)
    type_code = _DATACLOUD_TYPE_TO_DBAPI_LOWER.get((datacloud_type or "").lower(), STRING)

    # display_size and internal_size are often None in DB-API implementations
    display_size = None
    internal_size = None

    return (
        name,           # name
        type_code,      # type_code
        display_size,   # display_size
        internal_size,  # internal_size
        precision,      # precision
        scale,          # scale
        nullable,       # null_ok
    )
