"""
Internal module for querying PostgreSQL system catalogs (pg_catalog) for metadata.

This module provides table and column metadata by querying pg_catalog views,
matching the JDBC driver's approach. This is an internal module (prefix with _)
and should not be used directly by users.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .metadata import DataCloudTable, Field


def _load_sql_query(filename: str) -> str:
    """Load SQL query from file."""
    sql_dir = Path(__file__).parent / "sql"
    sql_file = sql_dir / filename
    return sql_file.read_text().strip()


GET_TABLES_QUERY = _load_sql_query("get_tables_query.sql")
GET_COLUMNS_QUERY = _load_sql_query("get_columns_query.sql")

# PostgreSQL format_type() output → Data Cloud type mapping
PG_TYPE_TO_DATACLOUD = {
    "character varying": "Varchar",
    "varchar": "Varchar",
    "text": "Text",
    "character": "Varchar",
    "char": "Varchar",
    "integer": "Integer",
    "int4": "Integer",
    "bigint": "BigInt",
    "int8": "BigInt",
    "smallint": "Integer",
    "int2": "Integer",
    "numeric": "Numeric",
    "decimal": "Numeric",
    "double precision": "Double",
    "float8": "Double",
    "real": "Float",
    "float4": "Float",
    "boolean": "Boolean",
    "bool": "Boolean",
    "timestamp with time zone": "TimestampTZ",
    "timestamptz": "TimestampTZ",
    "timestamp without time zone": "TimestampTZ",
    "timestamp": "TimestampTZ",
    "date": "Date",
    "time": "Time",
}

# Mapping from table type strings to pg relkind values
TABLE_TYPE_TO_RELKIND = {
    "TABLE": "r",
    "VIEW": "v",
    "MATERIALIZED VIEW": "m",
    "FOREIGN TABLE": "f",
    "PARTITIONED TABLE": "p",
}


def list_tables_pg(
    cursor: Any,
    schema_pattern: Optional[str] = None,
    table_name_pattern: Optional[str] = None,
    table_types: Optional[List[str]] = None,
    dataspace: Optional[str] = None,
) -> List[DataCloudTable]:
    """
    List tables by querying pg_catalog.

    Args:
        cursor: Cursor object for executing queries
        schema_pattern: SQL LIKE pattern for schema (e.g., "public", "sales%")
        table_name_pattern: SQL LIKE pattern for table name (e.g., "Account%")
        table_types: List of table types (e.g., ["TABLE", "VIEW"]), defaults to all
        dataspace: Dataspace name

    Returns:
        List of DataCloudTable objects with populated fields.
    """
    # Build and execute tables query
    tables_sql, tables_params = _build_tables_query(
        schema_pattern, table_name_pattern, table_types
    )
    cursor.execute(tables_sql, tables_params)
    table_rows = cursor.fetchall()

    if not table_rows:
        return []

    # Build and execute columns query for all tables
    columns_sql, columns_params = _build_columns_query(
        schema_pattern, table_name_pattern
    )
    cursor.execute(columns_sql, columns_params)
    column_rows = cursor.fetchall()

    # Group columns by (schema, table)
    columns_by_table: Dict[Tuple[str, str], List[Any]] = {}
    for row in column_rows:
        schema_name = row[0] if isinstance(row, (list, tuple)) else row
        table_name = row[1] if isinstance(row, (list, tuple)) else getattr(row, cursor.description[1][0])
        key = (schema_name, table_name)
        if key not in columns_by_table:
            columns_by_table[key] = []
        columns_by_table[key].append(row)

    # Create DataCloudTable objects
    datacloud_tables = []
    for table_row in table_rows:
        # Extract values based on cursor.description
        if isinstance(table_row, (list, tuple)):
            schema_name = table_row[0]
            table_name = table_row[1]
            table_type = table_row[2]
            remarks = table_row[3] if len(table_row) > 3 else None
        else:
            # Handle named tuple or object
            schema_name = getattr(table_row, cursor.description[0][0])
            table_name = getattr(table_row, cursor.description[1][0])
            table_type = getattr(table_row, cursor.description[2][0])
            remarks = getattr(table_row, cursor.description[3][0]) if len(cursor.description) > 3 else None

        # Get columns for this table
        table_key = (schema_name, table_name)
        table_columns = columns_by_table.get(table_key, [])

        # Create DataCloudTable
        datacloud_table = _create_datacloud_table_from_rows(
            schema_name, table_name, table_type, remarks, table_columns, dataspace, cursor
        )
        datacloud_tables.append(datacloud_table)

    return datacloud_tables


def get_table_metadata_pg(
    cursor: Any,
    schema: Optional[str] = None,
    table_name: Optional[str] = None,
    dataspace: Optional[str] = None,
) -> Optional[DataCloudTable]:
    """
    Get full metadata for a specific table by querying pg_catalog.

    Args:
        cursor: Cursor object for executing queries
        schema: Schema name (if None, searches all non-system schemas)
        table_name: Table name (required)
        dataspace: Dataspace name

    Returns:
        DataCloudTable object or None if not found.
    """
    if not table_name:
        raise ValueError("table_name is required")

    # List tables with exact name match
    tables = list_tables_pg(
        cursor,
        schema_pattern=schema,
        table_name_pattern=table_name,
        table_types=None,
        dataspace=dataspace,
    )

    if not tables:
        return None

    if len(tables) > 1:
        # Multiple matches - if no schema specified, this is ambiguous
        if schema is None:
            from .exceptions import InterfaceError
            table_list = ", ".join([f"{t.category}.{t.name}" for t in tables])
            raise InterfaceError(
                f"Multiple tables named '{table_name}' found: {table_list}. "
                f"Please specify a schema parameter."
            )

    return tables[0]


def _build_tables_query(
    schema_pattern: Optional[str],
    table_name_pattern: Optional[str],
    table_types: Optional[List[str]],
) -> Tuple[str, Dict[str, Any]]:
    """
    Build tables query with filters.

    Returns:
        Tuple of (sql_string, parameters_dict)
    """
    sql = GET_TABLES_QUERY
    params = {}

    if schema_pattern:
        sql += " AND n.nspname LIKE :schema_pattern"
        params["schema_pattern"] = schema_pattern

    if table_name_pattern:
        sql += " AND c.relname LIKE :table_name_pattern"
        params["table_name_pattern"] = table_name_pattern

    if table_types:
        # Convert table type names to relkind values
        relkinds = [TABLE_TYPE_TO_RELKIND.get(tt.upper(), tt) for tt in table_types]
        # Build IN clause manually since we can't use list parameters easily
        relkind_list = ", ".join([f"'{rk}'" for rk in relkinds])
        sql += f" AND c.relkind IN ({relkind_list})"

    sql += " ORDER BY n.nspname, c.relname"

    return sql, params


def _build_columns_query(
    schema_pattern: Optional[str],
    table_name_pattern: Optional[str],
) -> Tuple[str, Dict[str, Any]]:
    """
    Build columns query with filters.

    Returns:
        Tuple of (sql_string, parameters_dict)
    """
    sql = GET_COLUMNS_QUERY
    params = {}

    if schema_pattern:
        sql += " AND n.nspname LIKE :schema_pattern"
        params["schema_pattern"] = schema_pattern

    if table_name_pattern:
        sql += " AND c.relname LIKE :table_name_pattern"
        params["table_name_pattern"] = table_name_pattern

    sql += " ORDER BY n.nspname, c.relname, a.attnum"

    return sql, params


def _map_pg_type_to_datacloud(pg_type: str) -> Tuple[str, Optional[int], Optional[int]]:
    """
    Parse PostgreSQL type to Data Cloud type with precision/scale.

    Examples:
        "numeric(18,2)" → ("Numeric", 18, 2)
        "character varying(255)" → ("Varchar", 255, None)
        "integer" → ("Integer", None, None)

    Args:
        pg_type: PostgreSQL format_type() output

    Returns:
        Tuple of (datacloud_type, precision, scale)
    """
    # Parse type with optional precision/scale: type_name(precision,scale) or type_name(precision)
    match = re.match(r"^([a-z\s]+)(?:\((\d+)(?:,(\d+))?\))?$", pg_type.strip().lower())

    if match:
        base_type = match.group(1).strip()
        precision_str = match.group(2)
        scale_str = match.group(3)

        precision = int(precision_str) if precision_str else None
        scale = int(scale_str) if scale_str else None

        # Map base type to Data Cloud type
        datacloud_type = PG_TYPE_TO_DATACLOUD.get(base_type, "Varchar")  # Default to Varchar

        return datacloud_type, precision, scale
    else:
        # Couldn't parse, return as-is with defaults
        return PG_TYPE_TO_DATACLOUD.get(pg_type.strip().lower(), "Varchar"), None, None


def _create_datacloud_table_from_rows(
    schema_name: str,
    table_name: str,
    table_type: str,
    remarks: Optional[str],
    column_rows: List[Any],
    dataspace: str,
    cursor: Any,
) -> DataCloudTable:
    """
    Map query result rows to DataCloudTable object.

    Args:
        schema_name: Schema name from table query
        table_name: Table name from table query
        table_type: Table type (TABLE, VIEW, etc.)
        remarks: Table remarks/description
        column_rows: List of column rows from column query
        dataspace: Dataspace name
        cursor: Cursor object (for accessing column descriptions)

    Returns:
        DataCloudTable object with fields populated
    """
    # Create Field objects from column rows
    fields = [_create_field_from_row(col_row, cursor) for col_row in column_rows]

    # Create DataCloudTable with available data
    datacloud_table = DataCloudTable(
        name=table_name,
        display_name=table_name,
        category=schema_name,
        dataspace=dataspace,
        fields=fields,
    )

    return datacloud_table


def _create_field_from_row(column_row: Any, cursor: Any) -> Field:
    """
    Map column query row to Field object.

    Args:
        column_row: Single row from column query
        cursor: Cursor object (for accessing column descriptions)

    Returns:
        Field object
    """
    # Extract values based on cursor.description
    # Column order: schema_name, table_name, column_name, ordinal_position,
    #               data_type, is_nullable, column_default, description
    if isinstance(column_row, (list, tuple)):
        column_name = column_row[2]
        data_type = column_row[4]
        is_nullable = column_row[5]
    else:
        column_name = getattr(column_row, cursor.description[2][0])
        data_type = getattr(column_row, cursor.description[4][0])
        is_nullable = getattr(column_row, cursor.description[5][0])

    # Map PostgreSQL type to Data Cloud type with precision/scale
    datacloud_type, precision, scale = _map_pg_type_to_datacloud(data_type)

    # Create Field object
    field = Field(
        name=column_name,
        display_name=column_name,
        type=datacloud_type,
        nullable=bool(is_nullable),
        precision=precision,
        scale=scale,
    )

    return field
