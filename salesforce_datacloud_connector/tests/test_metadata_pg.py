"""
Unit tests for pg_catalog metadata queries.

These tests verify the pg_catalog query approach without requiring a real connection.
"""

import pytest
from unittest.mock import Mock
from salesforce_datacloud_connector._metadata_pg import (
    list_tables_pg,
    get_table_metadata_pg,
    _build_tables_query,
    _build_columns_query,
    _map_pg_type_to_datacloud,
    _create_field_from_row,
)
from salesforce_datacloud_connector.metadata import DataCloudTable


class TestBuildQueries:
    """Test SQL query building functions."""

    def test_build_tables_query_no_filters(self):
        """Test building tables query without filters."""
        sql, params = _build_tables_query(None, None, None)
        assert "FROM pg_catalog.pg_namespace n" in sql
        assert "JOIN pg_catalog.pg_class c" in sql
        assert "ORDER BY n.nspname, c.relname" in sql
        assert params == {}

    def test_build_tables_query_with_schema_filter(self):
        """Test building tables query with schema pattern."""
        sql, params = _build_tables_query("public", None, None)
        assert "n.nspname LIKE :schema_pattern" in sql
        assert params["schema_pattern"] == "public"

    def test_build_tables_query_with_table_filter(self):
        """Test building tables query with table name pattern."""
        sql, params = _build_tables_query(None, "Account%", None)
        assert "c.relname LIKE :table_name_pattern" in sql
        assert params["table_name_pattern"] == "Account%"

    def test_build_tables_query_with_type_filter(self):
        """Test building tables query with table types."""
        sql, params = _build_tables_query(None, None, ["TABLE", "VIEW"])
        assert "c.relkind IN ('r', 'v')" in sql

    def test_build_columns_query_no_filters(self):
        """Test building columns query without filters."""
        sql, params = _build_columns_query(None, None)
        assert "FROM pg_catalog.pg_namespace n" in sql
        assert "JOIN pg_catalog.pg_attribute a" in sql
        assert "ORDER BY n.nspname, c.relname, a.attnum" in sql
        assert params == {}

    def test_build_columns_query_with_filters(self):
        """Test building columns query with filters."""
        sql, params = _build_columns_query("public", "Account%")
        assert "n.nspname LIKE :schema_pattern" in sql
        assert "c.relname LIKE :table_name_pattern" in sql
        assert params["schema_pattern"] == "public"
        assert params["table_name_pattern"] == "Account%"


class TestTypeMapping:
    """Test PostgreSQL type to Data Cloud type mapping."""

    def test_map_simple_integer_type(self):
        """Test mapping simple integer type."""
        dc_type, precision, scale = _map_pg_type_to_datacloud("integer")
        assert dc_type == "Integer"
        assert precision is None
        assert scale is None

    def test_map_varchar_with_length(self):
        """Test mapping varchar with length."""
        dc_type, precision, scale = _map_pg_type_to_datacloud("character varying(255)")
        assert dc_type == "Varchar"
        assert precision == 255
        assert scale is None

    def test_map_numeric_with_precision_and_scale(self):
        """Test mapping numeric with precision and scale."""
        dc_type, precision, scale = _map_pg_type_to_datacloud("numeric(18,2)")
        assert dc_type == "Numeric"
        assert precision == 18
        assert scale == 2

    def test_map_timestamp_with_timezone(self):
        """Test mapping timestamp with time zone."""
        dc_type, precision, scale = _map_pg_type_to_datacloud("timestamp with time zone")
        assert dc_type == "TimestampTZ"
        assert precision is None
        assert scale is None

    def test_map_boolean_type(self):
        """Test mapping boolean type."""
        dc_type, precision, scale = _map_pg_type_to_datacloud("boolean")
        assert dc_type == "Boolean"
        assert precision is None
        assert scale is None

    def test_map_unknown_type_defaults_to_varchar(self):
        """Test that unknown types default to Varchar."""
        dc_type, precision, scale = _map_pg_type_to_datacloud("unknown_type")
        assert dc_type == "Varchar"


class TestCreateFieldFromRow:
    """Test creating Field objects from query rows."""

    def test_create_field_from_tuple_row(self):
        """Test creating Field from tuple row."""
        # Mock cursor description
        cursor = Mock()
        cursor.description = [
            ("schema_name", None),
            ("table_name", None),
            ("column_name", None),
            ("ordinal_position", None),
            ("data_type", None),
            ("is_nullable", None),
        ]

        # Row: schema, table, column_name, ordinal, data_type, is_nullable, default, description
        row = ("public", "Account", "Id", 1, "character varying(18)", True, None, None)

        field = _create_field_from_row(row, cursor)

        assert field.name == "Id"
        assert field.display_name == "Id"
        assert field.type == "Varchar"
        assert field.nullable is True
        assert field.precision == 18
        assert field.scale is None

    def test_create_field_numeric_type(self):
        """Test creating Field with numeric type."""
        cursor = Mock()
        cursor.description = [
            ("schema_name", None),
            ("table_name", None),
            ("column_name", None),
            ("ordinal_position", None),
            ("data_type", None),
            ("is_nullable", None),
        ]

        row = ("public", "Account", "Amount", 5, "numeric(18,2)", False, None, None)

        field = _create_field_from_row(row, cursor)

        assert field.name == "Amount"
        assert field.type == "Numeric"
        assert field.nullable is False
        assert field.precision == 18
        assert field.scale == 2


class TestListTablesPg:
    """Test list_tables_pg function."""

    def test_list_tables_pg_empty_result(self):
        """Test list_tables_pg with no tables."""
        cursor = Mock()
        cursor.fetchall.return_value = []

        result = list_tables_pg(cursor, dataspace="default")

        assert result == []
        assert cursor.execute.called

    def test_list_tables_pg_with_tables(self):
        """Test list_tables_pg with tables."""
        cursor = Mock()
        cursor.description = [
            ("schema_name", None),
            ("table_name", None),
            ("table_type", None),
            ("remarks", None),
        ]

        # Mock table rows
        table_rows = [
            ("public", "Account", "TABLE", "Account table"),
            ("public", "Contact", "TABLE", "Contact table"),
        ]

        # Mock column rows
        column_rows = [
            ("public", "Account", "Id", 1, "character varying(18)", False, None, None),
            ("public", "Account", "Name", 2, "character varying(255)", True, None, None),
            ("public", "Contact", "Id", 1, "character varying(18)", False, None, None),
        ]

        cursor.fetchall.side_effect = [table_rows, column_rows]

        result = list_tables_pg(cursor, dataspace="test_dataspace")

        assert len(result) == 2
        assert all(isinstance(table, DataCloudTable) for table in result)
        assert result[0].name == "Account"
        assert result[0].category == "public"
        assert result[0].dataspace == "test_dataspace"
        assert len(result[0].fields) == 2

        assert result[1].name == "Contact"
        assert len(result[1].fields) == 1


class TestGetTableMetadataPg:
    """Test get_table_metadata_pg function."""

    def test_get_table_metadata_pg_not_found(self):
        """Test get_table_metadata_pg with table not found."""
        cursor = Mock()
        cursor.fetchall.return_value = []

        result = get_table_metadata_pg(cursor, table_name="NonExistent", dataspace="default")

        assert result is None

    def test_get_table_metadata_pg_found(self):
        """Test get_table_metadata_pg with table found."""
        cursor = Mock()
        cursor.description = [
            ("schema_name", None),
            ("table_name", None),
            ("table_type", None),
            ("remarks", None),
        ]

        table_rows = [("public", "Account", "TABLE", None)]
        column_rows = [
            ("public", "Account", "Id", 1, "character varying(18)", False, None, None),
        ]

        cursor.fetchall.side_effect = [table_rows, column_rows]

        result = get_table_metadata_pg(cursor, table_name="Account", schema="public", dataspace="default")

        assert result is not None
        assert isinstance(result, DataCloudTable)
        assert result.name == "Account"
        assert len(result.fields) == 1

    def test_get_table_metadata_pg_requires_table_name(self):
        """Test that get_table_metadata_pg requires table_name."""
        cursor = Mock()

        with pytest.raises(ValueError, match="table_name is required"):
            get_table_metadata_pg(cursor, table_name=None, dataspace="default")


