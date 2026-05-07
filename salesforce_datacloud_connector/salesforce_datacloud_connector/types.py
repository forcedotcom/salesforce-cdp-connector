"""
Type system for Salesforce Data Cloud Driver.

This module handles type conversions between Data Cloud types and Python types,
and implements DB-API 2.0 type objects.
"""

from datetime import date, datetime
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


# Data Cloud type name to DB-API 2.0 type object mapping
DATACLOUD_TYPE_TO_DBAPI = {
    "Varchar": STRING,
    "Numeric": NUMBER,
    "TimestampTZ": DATETIME,
    "Boolean": NUMBER,  # Booleans are often categorized as NUMBER in DB-API
    "Date": DATETIME,
    "Integer": NUMBER,
    "BigInt": NUMBER,
    "Float": NUMBER,
    "Double": NUMBER,
    "Text": STRING,
}


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

    try:
        # Varchar → str
        if datacloud_type == "Varchar":
            return str(value)

        # Numeric → Decimal/int/float
        elif datacloud_type == "Numeric":
            # If scale is 0, return as int
            if scale == 0:
                return int(value)
            # If precision/scale not specified or low precision, use float
            elif precision is None or precision <= 15:
                return float(value)
            # For high precision, use Decimal
            else:
                return Decimal(str(value))

        # Integer types → int
        elif datacloud_type in ("Integer", "BigInt"):
            return int(value)

        # Float types → float
        elif datacloud_type in ("Float", "Double"):
            return float(value)

        # TimestampTZ → datetime with timezone
        elif datacloud_type == "TimestampTZ":
            if isinstance(value, datetime):
                return value
            # Parse string timestamp
            return dateutil_parser.parse(value)

        # Date → date
        elif datacloud_type == "Date":
            if isinstance(value, date):
                return value
            # Parse string date
            dt = dateutil_parser.parse(value)
            return dt.date()

        # Boolean → bool
        elif datacloud_type == "Boolean":
            if isinstance(value, bool):
                return value
            # Handle string representations
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "t")
            return bool(value)

        # Text → str
        elif datacloud_type == "Text":
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

    # Get DB-API type code
    type_code = DATACLOUD_TYPE_TO_DBAPI.get(datacloud_type, STRING)

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
