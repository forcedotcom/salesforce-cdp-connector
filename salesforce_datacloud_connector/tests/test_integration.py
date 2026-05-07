"""
End-to-end integration tests for the driver.

These tests verify the complete flow: connect → execute → fetch → close
"""

import pytest
import responses

import salesforce_datacloud_connector as sfdc
from salesforce_datacloud_connector.exceptions import NotSupportedError, ProgrammingError


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

    # Mock query execution
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql",
        json={
            "data": [["Alice", 30], ["Bob", 25], ["Charlie", 35]],
            "metadata": [
                {"name": "name", "type": "Varchar", "nullable": True},
                {"name": "age", "type": "Numeric", "nullable": False, "scale": 0},
            ],
            "returnedRows": 3,
            "status": {
                "queryId": "q1",
                "completionStatus": "ResultsProduced",
                "progress": 1.0,
                "rowCount": 3,
                "chunkCount": 1,
            },
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
def test_end_to_end_with_context_managers():
    """Test complete flow using context managers."""
    # Mock OAuth
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "token123", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Mock query
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql",
        json={
            "data": [["Result"]],
            "metadata": [{"name": "col", "type": "Varchar", "nullable": True}],
            "returnedRows": 1,
            "status": {
                "queryId": "q1",
                "completionStatus": "ResultsProduced",
                "progress": 1.0,
                "rowCount": 1,
                "chunkCount": 1,
            },
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

    # Mock query with parameter validation
    def check_parameters(request):
        body = request.body.decode("utf-8")
        assert '"name": "status"' in body
        assert '"value": "Active"' in body
        return (
            200,
            {},
            """{
                "data": [["Alice"]],
                "metadata": [{"name": "name", "type": "Varchar", "nullable": true}],
                "returnedRows": 1,
                "status": {
                    "queryId": "q1",
                    "completionStatus": "ResultsProduced",
                    "progress": 1.0,
                    "rowCount": 1,
                    "chunkCount": 1
                }
            }""",
        )

    responses.add_callback(
        responses.POST,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql",
        callback=check_parameters,
    )

    with sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="username_password",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
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

    # Mock initial query (returns first chunk)
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql",
        json={
            "data": [["Row1"], ["Row2"]],
            "metadata": [{"name": "data", "type": "Varchar", "nullable": True}],
            "returnedRows": 2,
            "status": {
                "queryId": "q1",
                "completionStatus": "ResultsProduced",
                "progress": 1.0,
                "rowCount": 5,  # Total 5 rows
                "chunkCount": 3,
            },
        },
        status=200,
    )

    # Mock second chunk
    responses.add(
        responses.GET,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql/q1/rows",
        json={"data": [["Row3"], ["Row4"]], "returnedRows": 2},
        status=200,
    )

    # Mock third chunk
    responses.add(
        responses.GET,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql/q1/rows",
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

    # Mock initial query (async response)
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql",
        json={
            "data": [],
            "metadata": [{"name": "result", "type": "Varchar", "nullable": True}],
            "returnedRows": 0,
            "status": {
                "queryId": "q1",
                "completionStatus": "Running",
                "progress": 0.5,
                "rowCount": 0,
                "chunkCount": 0,
            },
        },
        status=200,
    )

    # Mock status polling (complete)
    responses.add(
        responses.GET,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql/q1",
        json={
            "status": {
                "queryId": "q1",
                "completionStatus": "Finished",
                "progress": 1.0,
                "rowCount": 1,
                "chunkCount": 1,
            }
        },
        status=200,
    )

    # Mock fetching results
    responses.add(
        responses.GET,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql/q1/rows",
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

    with sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="username_password",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
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

    # Mock SQL error
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql",
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

    # Mock query
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql",
        json={
            "data": [["Alice"], ["Bob"], ["Charlie"]],
            "metadata": [{"name": "name", "type": "Varchar", "nullable": True}],
            "returnedRows": 3,
            "status": {
                "queryId": "q1",
                "completionStatus": "ResultsProduced",
                "progress": 1.0,
                "rowCount": 3,
                "chunkCount": 1,
            },
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
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql",
        json={
            "data": [["Test"]],
            "metadata": [{"name": "col", "type": "Varchar", "nullable": True}],
            "returnedRows": 1,
            "status": {
                "queryId": "q1",
                "completionStatus": "ResultsProduced",
                "progress": 1.0,
                "rowCount": 1,
                "chunkCount": 1,
            },
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
            "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql",
            json={
                "data": [["Test"]],
                "metadata": [{"name": "col", "type": "Varchar", "nullable": True}],
                "returnedRows": 1,
                "status": {
                    "queryId": "q1",
                    "completionStatus": "ResultsProduced",
                    "progress": 1.0,
                    "rowCount": 1,
                    "chunkCount": 1,
                },
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

    # Mock query
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/data/v64.0/ssot/query-sql",
        json={
            "data": [["Test"]],
            "metadata": [{"name": "col", "type": "Varchar", "nullable": True}],
            "returnedRows": 1,
            "status": {
                "queryId": "q1",
                "completionStatus": "ResultsProduced",
                "progress": 1.0,
                "rowCount": 1,
                "chunkCount": 1,
            },
        },
        status=200,
    )

    conn = sfdc.connect(
        login_url="https://test.salesforce.com",
        auth_type="refresh_token",
        client_id="client_id",
        client_secret="client_secret",
        refresh_token="refresh_token_xyz",
    )

    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    row = cursor.fetchone()
    assert row == ("Test",)
    conn.close()
