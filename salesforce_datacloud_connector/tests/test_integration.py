"""
End-to-end integration tests for the driver.

These tests verify the complete flow: connect → execute → fetch → close
"""

import datetime
import json
from decimal import Decimal

import pyarrow as pa
import pytest
import responses

import salesforce_datacloud_connector as sfdc
from salesforce_datacloud_connector.exceptions import NotSupportedError, ProgrammingError

from ._arrow_fixtures import build_arrow_ipc_bytes


@responses.activate
def test_end_to_end_sync_query():
    """Test complete flow with synchronous query."""
    # Mock OAuth token request
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "token123", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={"access_token": "cdp_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock query execution
    status_header = {
        "queryId": "q1",
        "completionStatus": "RESULTS_PRODUCED",
        "progress": 1.0,
        "rowCount": 3,
        "chunkCount": 1,
    }
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/api/v3/query",
        json={
            "metadata": {
                "columns": [
                    {"name": "name", "type": "varchar", "nullable": True},
                    {"name": "age", "type": "numeric", "nullable": False, "scale": 0},
                ]
            },
            "data": [["Alice", 30], ["Bob", 25], ["Charlie", 35]],
            "returnedRows": 3,
        },
        headers={
            "x-hyperdb-status": json.dumps(status_header)
        },
        status=200,
    )

    # Connect
    conn = sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="username_password",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
        output_format="json",
    )

    try:
        # Execute query
        cursor = conn.cursor()
        cursor.execute("SELECT name, age FROM users")

        # Verify description
        assert len(cursor.description) == 2
        assert cursor.description[0][0] == "name"
        assert cursor.description[1][0] == "age"

        # Fetch results
        rows = cursor.fetchall()
        assert len(rows) == 3
        assert rows[0] == ("Alice", 30)
        assert rows[1] == ("Bob", 25)
        assert rows[2] == ("Charlie", 35)

        cursor.close()
    finally:
        conn.close()


@responses.activate
def test_end_to_end_arrow_default_comprehensive_type_palette():
    """Test the complete connect → execute → fetch flow using the Arrow
    output format (the connector's default -- no output_format override),
    across a broad type palette, confirming ColumnMetadata.type and
    convert_datacloud_value agree end to end for every type reachable only
    via Arrow's physical type system (smallint/oid/float4/float8/bytea/
    interval), not just the JSON-wire names covered elsewhere."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "token123", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={"access_token": "cdp_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    fields = [
        pa.field("flag", pa.bool_(), nullable=True),
        pa.field("small", pa.int16(), nullable=True),
        pa.field("cnt", pa.int32(), nullable=True),
        pa.field("big", pa.int64(), nullable=True),
        pa.field("row_id", pa.uint32(), nullable=True),
        pa.field("ratio", pa.float32(), nullable=True),
        pa.field("amount", pa.float64(), nullable=True),
        pa.field("price", pa.decimal128(20, 3), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("payload", pa.binary(), nullable=True),
        pa.field("day", pa.date32(), nullable=True),
        pa.field("start_time", pa.time64("us"), nullable=True),
        pa.field("created", pa.timestamp("us"), nullable=True),
        pa.field("created_tz", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("span", pa.month_day_nano_interval(), nullable=True),
    ]
    row = (
        True, 7, 42, 9000000000, 100000, 1.5, 2.25, Decimal("12345.678"), "Alice", b"hello",
        datetime.date(2024, 1, 15),
        datetime.time(14, 30, 0),
        datetime.datetime(2024, 1, 15, 10, 30, 0),
        datetime.datetime(2024, 1, 15, 10, 30, 0, tzinfo=datetime.timezone.utc),
        (0, 1, 7200000000000),
    )
    arrow_body = build_arrow_ipc_bytes(fields, [row])

    def check_request(request):
        assert request.headers.get("Accept") == "application/vnd.apache.arrow.stream"
        status_header = {
            "queryId": "q1", "completionStatus": "RESULTS_PRODUCED",
            "progress": 1.0, "rowCount": 1, "chunkCount": 1,
        }
        return (200, {"x-hyperdb-status": json.dumps(status_header)}, arrow_body)

    responses.add_callback(
        responses.POST,
        "https://myorg.my.salesforce.com/api/v3/query",
        callback=check_request,
    )

    with sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="username_password",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM everything")

            expected_types = [
                "bool", "smallint", "integer", "bigint", "oid", "float4", "float8", "numeric",
                "varchar", "bytea", "date", "time", "timestamp", "timestamptz", "interval",
            ]
            assert [col[0] for col in cursor.description] == [f.name for f in fields]
            assert [col.type for col in cursor._metadata] == expected_types

            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0] == (
                True, 7, 42, 9000000000, 100000,
                pytest.approx(1.5), pytest.approx(2.25), Decimal("12345.678"),
                "Alice", b"hello",
                datetime.date(2024, 1, 15),
                datetime.time(14, 30, 0),
                datetime.datetime(2024, 1, 15, 10, 30, 0),
                datetime.datetime(2024, 1, 15, 10, 30, 0, tzinfo=datetime.timezone.utc),
                (0, 1, 7200000000000),
            )


@responses.activate
def test_end_to_end_with_context_managers():
    """Test complete flow using context managers."""
    # Mock OAuth
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "token123", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={"access_token": "cdp_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock query
    status_header = {
        "queryId": "q1",
        "completionStatus": "RESULTS_PRODUCED",
        "progress": 1.0,
        "rowCount": 1,
        "chunkCount": 1,
    }
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/api/v3/query",
        json={
            "metadata": {
                "columns": [{"name": "col", "type": "varchar", "nullable": True}]
            },
            "data": [["Result"]],
            "returnedRows": 1,
        },
        headers={
            "x-hyperdb-status": json.dumps(status_header)
        },
        status=200,
    )

    with sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="username_password",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
        output_format="json",
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            row = cursor.fetchone()
            assert row == ("Result",)


@responses.activate
def test_parameterized_query():
    """Test end-to-end with parameterized query."""
    # Mock OAuth
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "token123", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={"access_token": "cdp_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock query with parameter validation
    def check_parameters(request):
        body = request.body.decode("utf-8")
        assert '"value": "Active"' in body
        assert '"name"' not in body  # v3 params carry no parameter name
        status_header = {
            "queryId": "q1",
            "completionStatus": "RESULTS_PRODUCED",
            "progress": 1.0,
            "rowCount": 1,
            "chunkCount": 1
        }
        return (
            200,
            {"x-hyperdb-status": json.dumps(status_header)},
            """{
                "metadata": {"columns": [{"name": "name", "type": "varchar", "nullable": true}]},
                "data": [["Alice"]],
                "returnedRows": 1
            }""",
        )

    responses.add_callback(
        responses.POST,
        "https://myorg.my.salesforce.com/api/v3/query",
        callback=check_parameters,
    )

    with sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="username_password",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
        output_format="json",
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM users WHERE status = :status", {"status": "Active"}
        )
        rows = cursor.fetchall()
        assert len(rows) == 1


@responses.activate
def test_large_result_set_with_pagination():
    """Test fetching large result set with multiple chunks."""
    # Mock OAuth
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "token123", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={"access_token": "cdp_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock initial query (returns first chunk)
    status_header = {
        "queryId": "q1",
        "completionStatus": "RESULTS_PRODUCED",
        "progress": 1.0,
        "rowCount": 5,  # Total 5 rows
        "chunkCount": 3,
    }
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/api/v3/query",
        json={
            "metadata": {
                "columns": [{"name": "data", "type": "varchar", "nullable": True}]
            },
            "data": [["Row1"], ["Row2"]],
            "returnedRows": 2,
        },
        headers={
            "x-hyperdb-status": json.dumps(status_header)
        },
        status=200,
    )

    # Mock second chunk
    responses.add(
        responses.GET,
        "https://myorg.my.salesforce.com/api/v3/query/q1/rows",
        json={"data": [["Row3"], ["Row4"]], "returnedRows": 2},
        status=200,
    )

    # Mock third chunk
    responses.add(
        responses.GET,
        "https://myorg.my.salesforce.com/api/v3/query/q1/rows",
        json={"data": [["Row5"]], "returnedRows": 1},
        status=200,
    )

    with sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="username_password",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
        output_format="json",
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT data FROM large_table")

        # Fetch all rows (should paginate automatically)
        rows = cursor.fetchall()
        assert len(rows) == 5
        assert rows[0] == ("Row1",)
        assert rows[4] == ("Row5",)


@responses.activate
def test_async_query_with_polling():
    """Test async query that requires polling."""
    # Mock OAuth
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "token123", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={"access_token": "cdp_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock initial query (async response)
    initial_status_header = {
        "queryId": "q1",
        "completionStatus": "RUNNING_OR_UNSPECIFIED",
        "progress": 0.5,
        "rowCount": 0,
        "chunkCount": 0,
    }
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/api/v3/query",
        json={
            "metadata": {
                "columns": [{"name": "result", "type": "varchar", "nullable": True}]
            },
            "data": [],
            "returnedRows": 0,
        },
        headers={
            "x-hyperdb-status": json.dumps(initial_status_header)
        },
        status=200,
    )

    # Mock status polling (complete). getQueryStatusV3 returns QueryStatus as
    # the JSON body directly; it never sets x-hyperdb-status (that header is
    # only set by the POST /v3/query path).
    responses.add(
        responses.GET,
        "https://myorg.my.salesforce.com/api/v3/query/q1",
        json={
            "queryId": "q1",
            "completionStatus": "FINISHED",
            "progress": 1.0,
            "rowCount": 1,
            "chunkCount": 1,
        },
        status=200,
    )

    # Mock fetching results
    responses.add(
        responses.GET,
        "https://myorg.my.salesforce.com/api/v3/query/q1/rows",
        json={"data": [["Success"]], "returnedRows": 1},
        status=200,
    )

    with sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="username_password",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
        output_format="json",
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM large_dataset")

        row = cursor.fetchone()
        assert row == ("Success",)


@responses.activate
def test_unsupported_dml_operations():
    """Test that DML operations raise NotSupportedError."""
    # Mock OAuth
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "token123", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={"access_token": "cdp_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    with sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="username_password",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
        output_format="json",
    ) as conn:
        cursor = conn.cursor()

        with pytest.raises(NotSupportedError):
            cursor.execute("INSERT INTO users VALUES (1, 'Alice')")

        with pytest.raises(NotSupportedError):
            cursor.execute("UPDATE users SET name = 'Bob'")

        with pytest.raises(NotSupportedError):
            cursor.execute("DELETE FROM users")


@responses.activate
def test_sql_syntax_error():
    """Test handling of SQL syntax errors."""
    # Mock OAuth
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "token123", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={"access_token": "cdp_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock SQL error
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/api/v3/query",
        json={"message": "SQL syntax error near 'INVALID'"},
        status=400,
    )

    with sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="username_password",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
        output_format="json",
    ) as conn:
        cursor = conn.cursor()

        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT * FROM INVALID SYNTAX")


@responses.activate
def test_cursor_iteration():
    """Test iterating over cursor results."""
    # Mock OAuth
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "token123", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={"access_token": "cdp_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock query
    status_header = {
        "queryId": "q1",
        "completionStatus": "RESULTS_PRODUCED",
        "progress": 1.0,
        "rowCount": 3,
        "chunkCount": 1,
    }
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/api/v3/query",
        json={
            "metadata": {
                "columns": [{"name": "name", "type": "varchar", "nullable": True}]
            },
            "data": [["Alice"], ["Bob"], ["Charlie"]],
            "returnedRows": 3,
        },
        headers={
            "x-hyperdb-status": json.dumps(status_header)
        },
        status=200,
    )

    with sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="username_password",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
        output_format="json",
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM users")

        names = [row[0] for row in cursor]
        assert names == ["Alice", "Bob", "Charlie"]


@responses.activate
def test_jwt_authentication():
    """Test JWT authentication flow."""
    # Mock JWT token request
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "jwt_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock query
    status_header = {
        "queryId": "q1",
        "completionStatus": "RESULTS_PRODUCED",
        "progress": 1.0,
        "rowCount": 1,
        "chunkCount": 1,
    }
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/api/v3/query",
        json={
            "metadata": {
                "columns": [{"name": "col", "type": "varchar", "nullable": True}]
            },
            "data": [["Test"]],
            "returnedRows": 1,
        },
        headers={
            "x-hyperdb-status": json.dumps(status_header)
        },
        status=200,
    )

    # Use mock JWT encoding
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            "https://test.salesforce.com/services/oauth2/token",
            json={"access_token": "jwt_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
            status=200,
        )
        rsps.add(
            responses.POST,
            "https://myorg.my.salesforce.com/services/a360/token",
            json={"access_token": "cdp_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
            status=200,
        )
        rsps.add(
            responses.POST,
            "https://myorg.my.salesforce.com/api/v3/query",
            json={
                "metadata": {
                    "columns": [{"name": "col", "type": "varchar", "nullable": True}]
                },
                "data": [["Test"]],
                "returnedRows": 1,
            },
            headers={
                "x-hyperdb-status": json.dumps(status_header)
            },
            status=200,
        )

        from unittest.mock import patch

        with patch("jwt.encode", return_value="mock_jwt"):
            conn = sfdc.connect(
                login_url="https://test.salesforce.com",
                auth_type="jwt",
                username="test@example.com",
                client_id="client_id",
                jwt_private_key="-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----",
                output_format="json",
            )

            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            row = cursor.fetchone()
            assert row == ("Test",)
            conn.close()


@responses.activate
def test_refresh_token_authentication():
    """Test refresh token authentication flow."""
    # Mock SFDC OAuth token response
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "sfdc_access_token",
            "expires_in": 7200,
            "token_type": "Bearer",
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={"access_token": "cdp_token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock query
    status_header = {
        "queryId": "q1",
        "completionStatus": "RESULTS_PRODUCED",
        "progress": 1.0,
        "rowCount": 1,
        "chunkCount": 1,
    }
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/api/v3/query",
        json={
            "metadata": {
                "columns": [{"name": "col", "type": "varchar", "nullable": True}]
            },
            "data": [["Test"]],
            "returnedRows": 1,
        },
        headers={
            "x-hyperdb-status": json.dumps(status_header)
        },
        status=200,
    )

    conn = sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="refresh_token",
        client_id="client_id",
        client_secret="client_secret",
        refresh_token="refresh_token_xyz",
        output_format="json",
    )

    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    row = cursor.fetchone()
    assert row == ("Test",)
    conn.close()
