"""
Tests for DB-API 2.0 Cursor.
"""

from unittest.mock import Mock

import pytest

from salesforce_datacloud_connector.api.client import DataCloudQueryClient
from salesforce_datacloud_connector.api.models import ColumnMetadata, QueryResponse, QueryStatus
from salesforce_datacloud_connector.cursor import Cursor
from salesforce_datacloud_connector.exceptions import InterfaceError, NotSupportedError


def create_mock_client():
    """Create a mock API client for testing."""
    return Mock(spec=DataCloudQueryClient)


def test_cursor_description_before_execute():
    """Test that description is None before executing a query."""
    client = create_mock_client()
    cursor = Cursor(client)

    assert cursor.description is None


def test_cursor_rowcount():
    """Test that rowcount returns -1 for SELECT queries."""
    client = create_mock_client()
    cursor = Cursor(client)

    assert cursor.rowcount == -1


def test_execute_sync_query():
    """Test executing a query with synchronous response."""
    client = create_mock_client()
    cursor = Cursor(client)

    # Mock response
    client.execute_query.return_value = QueryResponse(
        data=[["Alice", 30], ["Bob", 25]],
        metadata=[
            ColumnMetadata(name="name", type="Varchar", nullable=True),
            ColumnMetadata(name="age", type="Numeric", nullable=False, scale=0),
        ],
        returned_rows=2,
        status=QueryStatus(
            query_id="q1",
            completion_status="ResultsProduced",
            progress=1.0,
            row_count=2,
            chunk_count=1,
        ),
    )

    cursor.execute("SELECT name, age FROM users")

    assert cursor.description is not None
    assert len(cursor.description) == 2
    assert cursor.description[0][0] == "name"  # Column name
    assert cursor.description[1][0] == "age"


def test_execute_async_query():
    """Test executing a query with asynchronous response."""
    client = create_mock_client()
    cursor = Cursor(client)

    # Mock async response
    client.execute_query.return_value = QueryResponse(
        data=[],
        metadata=[ColumnMetadata(name="name", type="Varchar", nullable=True)],
        returned_rows=0,
        status=QueryStatus(
            query_id="q1",
            completion_status="Running",
            progress=0.5,
            row_count=0,
            chunk_count=0,
        ),
    )

    # Mock polling
    client.poll_until_complete.return_value = QueryStatus(
        query_id="q1",
        completion_status="Finished",
        progress=1.0,
        row_count=100,
        chunk_count=1,
    )

    # Mock first chunk fetch
    client.fetch_results.return_value = QueryResponse(
        data=[["Alice"], ["Bob"]],
        metadata=[],
        returned_rows=2,
    )

    cursor.execute("SELECT name FROM large_table")

    # Should have polled until complete
    client.poll_until_complete.assert_called_once_with("q1")


def test_execute_with_parameters():
    """Test executing a parameterized query."""
    client = create_mock_client()
    cursor = Cursor(client)

    client.execute_query.return_value = QueryResponse(
        data=[],
        metadata=[],
        returned_rows=0,
        status=QueryStatus(
            query_id="q1",
            completion_status="ResultsProduced",
            progress=1.0,
            row_count=0,
            chunk_count=0,
        ),
    )

    cursor.execute("SELECT * FROM users WHERE status = :status", {"status": "Active"})

    client.execute_query.assert_called_once()
    args = client.execute_query.call_args
    assert args[0][0] == "SELECT * FROM users WHERE status = :status"
    assert args[0][1] == {"status": "Active"}


def test_execute_unsupported_operations():
    """Test that DML/DDL operations raise NotSupportedError."""
    client = create_mock_client()
    cursor = Cursor(client)

    unsupported_queries = [
        "INSERT INTO users VALUES (1, 'Alice')",
        "UPDATE users SET name = 'Bob'",
        "DELETE FROM users WHERE id = 1",
        "CREATE TABLE test (id INT)",
        "DROP TABLE test",
        "ALTER TABLE users ADD COLUMN email VARCHAR",
        "TRUNCATE TABLE users",
    ]

    for query in unsupported_queries:
        with pytest.raises(NotSupportedError):
            cursor.execute(query)


def test_fetchone():
    """Test fetchone() method."""
    client = create_mock_client()
    cursor = Cursor(client)

    client.execute_query.return_value = QueryResponse(
        data=[["Alice", 30], ["Bob", 25]],
        metadata=[
            ColumnMetadata(name="name", type="Varchar"),
            ColumnMetadata(name="age", type="Numeric", scale=0),
        ],
        returned_rows=2,
        status=QueryStatus(
            query_id="q1",
            completion_status="ResultsProduced",
            progress=1.0,
            row_count=2,
            chunk_count=1,
        ),
    )

    cursor.execute("SELECT name, age FROM users")

    row1 = cursor.fetchone()
    assert row1 == ("Alice", 30)

    row2 = cursor.fetchone()
    assert row2 == ("Bob", 25)

    row3 = cursor.fetchone()
    assert row3 is None


def test_fetchmany():
    """Test fetchmany() method."""
    client = create_mock_client()
    cursor = Cursor(client)

    client.execute_query.return_value = QueryResponse(
        data=[["Alice"], ["Bob"], ["Charlie"], ["David"]],
        metadata=[ColumnMetadata(name="name", type="Varchar")],
        returned_rows=4,
        status=QueryStatus(
            query_id="q1",
            completion_status="ResultsProduced",
            progress=1.0,
            row_count=4,
            chunk_count=1,
        ),
    )

    cursor.execute("SELECT name FROM users")

    rows = cursor.fetchmany(2)
    assert len(rows) == 2
    assert rows[0] == ("Alice",)
    assert rows[1] == ("Bob",)

    rows = cursor.fetchmany(5)  # Request more than available
    assert len(rows) == 2
    assert rows[0] == ("Charlie",)
    assert rows[1] == ("David",)


def test_fetchall():
    """Test fetchall() method."""
    client = create_mock_client()
    cursor = Cursor(client)

    client.execute_query.return_value = QueryResponse(
        data=[["Alice"], ["Bob"], ["Charlie"]],
        metadata=[ColumnMetadata(name="name", type="Varchar")],
        returned_rows=3,
        status=QueryStatus(
            query_id="q1",
            completion_status="ResultsProduced",
            progress=1.0,
            row_count=3,
            chunk_count=1,
        ),
    )

    cursor.execute("SELECT name FROM users")

    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0] == ("Alice",)
    assert rows[1] == ("Bob",)
    assert rows[2] == ("Charlie",)


def test_fetchall_with_pagination():
    """Test fetchall() with multi-chunk pagination."""
    client = create_mock_client()
    cursor = Cursor(client)

    # Initial response with first chunk
    client.execute_query.return_value = QueryResponse(
        data=[["Row1"], ["Row2"]],
        metadata=[ColumnMetadata(name="data", type="Varchar")],
        returned_rows=2,
        status=QueryStatus(
            query_id="q1",
            completion_status="ResultsProduced",
            progress=1.0,
            row_count=4,  # Total 4 rows
            chunk_count=2,
        ),
    )

    # Second chunk
    client.fetch_results.return_value = QueryResponse(
        data=[["Row3"], ["Row4"]],
        metadata=[],
        returned_rows=2,
    )

    cursor.execute("SELECT data FROM table")

    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[2] == ("Row3",)
    assert rows[3] == ("Row4",)


def test_cursor_iteration():
    """Test iterating over cursor results."""
    client = create_mock_client()
    cursor = Cursor(client)

    client.execute_query.return_value = QueryResponse(
        data=[["Alice"], ["Bob"]],
        metadata=[ColumnMetadata(name="name", type="Varchar")],
        returned_rows=2,
        status=QueryStatus(
            query_id="q1",
            completion_status="ResultsProduced",
            progress=1.0,
            row_count=2,
            chunk_count=1,
        ),
    )

    cursor.execute("SELECT name FROM users")

    rows = list(cursor)
    assert len(rows) == 2
    assert rows[0] == ("Alice",)
    assert rows[1] == ("Bob",)


def test_close_cursor():
    """Test closing a cursor."""
    client = create_mock_client()
    cursor = Cursor(client)

    client.execute_query.return_value = QueryResponse(
        data=[["Alice"]],
        metadata=[ColumnMetadata(name="name", type="Varchar")],
        returned_rows=1,
        status=QueryStatus(
            query_id="q1",
            completion_status="ResultsProduced",
            progress=1.0,
            row_count=1,
            chunk_count=1,
        ),
    )

    cursor.execute("SELECT name FROM users")
    cursor.close()

    # Operations after close should raise InterfaceError
    with pytest.raises(InterfaceError):
        cursor.execute("SELECT 1")

    with pytest.raises(InterfaceError):
        cursor.fetchone()


def test_cancel_query():
    """Test canceling a query."""
    client = create_mock_client()
    cursor = Cursor(client)

    client.execute_query.return_value = QueryResponse(
        data=[],
        metadata=[],
        returned_rows=0,
        status=QueryStatus(
            query_id="q1",
            completion_status="ResultsProduced",
            progress=1.0,
            row_count=0,
            chunk_count=0,
        ),
    )

    cursor.execute("SELECT 1")
    cursor.cancel()

    client.cancel_query.assert_called_once_with("q1")


def test_context_manager():
    """Test cursor as context manager."""
    client = create_mock_client()

    client.execute_query.return_value = QueryResponse(
        data=[["Alice"]],
        metadata=[ColumnMetadata(name="name", type="Varchar")],
        returned_rows=1,
        status=QueryStatus(
            query_id="q1",
            completion_status="ResultsProduced",
            progress=1.0,
            row_count=1,
            chunk_count=1,
        ),
    )

    with Cursor(client) as cursor:
        cursor.execute("SELECT name FROM users")
        row = cursor.fetchone()
        assert row == ("Alice",)

    # Cursor should be closed after exiting context
    with pytest.raises(InterfaceError):
        cursor.fetchone()


def test_fetch_before_execute():
    """Test that fetch methods raise error if no query executed."""
    client = create_mock_client()
    cursor = Cursor(client)

    with pytest.raises(InterfaceError):
        cursor.fetchone()

    with pytest.raises(InterfaceError):
        cursor.fetchall()

    with pytest.raises(InterfaceError):
        cursor.fetchmany(10)
