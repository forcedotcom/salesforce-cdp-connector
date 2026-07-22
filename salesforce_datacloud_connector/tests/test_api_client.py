"""
Tests for Data Cloud Query API client.
"""

import json

import pytest
import responses

from salesforce_datacloud_connector.api.client import DataCloudQueryClient
from salesforce_datacloud_connector.exceptions import ProgrammingError


def mock_token_getter():
    """Mock token getter for tests."""
    return "mock_token_12345"


@responses.activate
def test_execute_query_sync_v3():
    """Test executing a query that returns synchronous results (v3 API)."""
    status_header = {
        "queryId": "MTAuMjQuMTIzLjE5NDo3NDg0_cb6991ab",
        "completionStatus": "RESULTS_PRODUCED",
        "chunkCount": 1,
        "rowCount": 2,
        "progress": 1.0,
        "expirationTime": "2025-09-26T10:55:07.438Z",
        "executionStats": {
            "wallClockTime": 1.122854338,
            "rowsProcessed": 2
        }
    }

    responses.add(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        json={
            "metadata": {
                "columns": [
                    {"name": "name", "type": "varchar", "nullable": True},
                    {"name": "age", "type": "numeric", "nullable": False, "precision": 10, "scale": 0},
                ]
            },
            "data": [["Alice", 30], ["Bob", 25]],
            "returnedRows": 2
        },
        headers={
            "x-hyperdb-status": json.dumps(status_header)
        },
        status=200,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    response = client.execute_query("SELECT name, age FROM users")

    assert len(response.data) == 2
    assert response.data[0] == ["Alice", 30]
    assert response.returned_rows == 2
    assert response.status.query_id == "MTAuMjQuMTIzLjE5NDo3NDg0_cb6991ab"
    assert response.status.is_complete()


@responses.activate
def test_execute_query_async_v3():
    """Test executing a query that returns async status (v3 API)."""
    status_header = {
        "queryId": "MTAuMjQuMTIzLjE5NDo3NDg0_cb6991ab-async",
        "completionStatus": "RUNNING_OR_UNSPECIFIED",
        "chunkCount": 0,
        "rowCount": 0,
        "progress": 0.5,
    }

    responses.add(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        json={
            "metadata": {
                "columns": [
                    {"name": "name", "type": "varchar", "nullable": True},
                ]
            },
            "data": None,
            "returnedRows": 0
        },
        headers={
            "x-hyperdb-status": json.dumps(status_header)
        },
        status=200,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    response = client.execute_query("SELECT name FROM large_table")

    assert response.status.is_running()
    assert not response.status.is_complete()
    assert response.status.query_id == "MTAuMjQuMTIzLjE5NDo3NDg0_cb6991ab-async"
    assert response.status.completion_status == "RUNNING"  # normalized
    assert response.data is None or response.data == []


@responses.activate
def test_execute_query_with_parameters_v3():
    """Test executing a parameterized query (v3 API)."""
    def check_request(request):
        import json
        body = json.loads(request.body.decode("utf-8"))
        assert "parameters" in body
        assert len(body["parameters"]) == 1
        assert body["parameters"][0]["type"] == "varchar"
        assert body["parameters"][0]["value"] == "Active"
        # V3 does not include parameter names
        assert "name" not in body["parameters"][0]

        status_header = {
            "queryId": "q1",
            "completionStatus": "RESULTS_PRODUCED",
            "progress": 1.0,
            "rowCount": 0,
            "chunkCount": 0
        }

        return (
            200,
            {"x-hyperdb-status": json.dumps(status_header)},
            json.dumps({
                "metadata": {"columns": []},
                "data": [],
                "returnedRows": 0
            })
        )

    responses.add_callback(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    response = client.execute_query(
        "SELECT * FROM users WHERE status = ?",
        parameters={"status": "Active"}
    )

    assert response.status.query_id == "q1"


@responses.activate
def test_get_query_status_v3():
    """Test getting query status (v3 API)."""
    status_header = {
        "queryId": "query123",
        "completionStatus": "FINISHED",
        "progress": 1.0,
        "rowCount": 1000,
        "chunkCount": 1,
    }

    responses.add(
        responses.GET,
        "https://test.c360a.salesforce.com/api/v3/query/query123",
        json={
            "metadata": {"columns": []},
            "data": None,
            "returnedRows": 0
        },
        headers={
            "x-hyperdb-status": json.dumps(status_header)
        },
        status=200,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    status = client.get_query_status("query123")

    assert status.query_id == "query123"
    assert status.is_complete()
    assert status.row_count == 1000


@responses.activate
def test_fetch_results_v3():
    """Test fetching query results (v3 API)."""
    def check_request(request):
        # Verify v3 query parameters
        assert "offset=0" in request.url
        assert "limit=1000000" in request.url
        assert "byteLimit=20971520" in request.url
        # v3 should not have rowLimit or omitSchema params
        assert "rowLimit" not in request.url
        assert "omitSchema" not in request.url
        # v3 should not have workload param in query string
        assert "workload=" not in request.url

        return (200, {}, json.dumps({
            "metadata": {
                "columns": [
                    {"name": "id", "type": "numeric", "nullable": False},
                    {"name": "value", "type": "varchar", "nullable": True}
                ]
            },
            "data": [[1, "first"], [2, "second"]],
            "returnedRows": 2
        }))

    responses.add_callback(
        responses.GET,
        "https://test.c360a.salesforce.com/api/v3/query/query123/rows",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    response = client.fetch_results(
        query_id="query123",
        offset=0,
        row_limit=1000000,
        omit_schema=False
    )

    assert len(response.data) == 2
    assert response.data[0] == [1, "first"]
    assert response.returned_rows == 2
    assert len(response.metadata) == 2


@responses.activate
def test_cancel_query_v3():
    """Test cancelling a running query (v3 API)."""
    responses.add(
        responses.DELETE,
        "https://test.c360a.salesforce.com/api/v3/query/query123",
        status=204,  # v3 returns 204 No Content on success
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    # Should not raise any exception
    client.cancel_query("query123")


@responses.activate
def test_error_response_v3():
    """Test v3 error response mapping."""
    responses.add(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        json={
            "timestamp": "2026-07-22T10:55:07.000+00:00",
            "error": "COMMON_ERROR_GENERIC",
            "message": "SQL syntax error: unexpected token",
            "path": "/api/v3/query",
            "tenantId": "tenant123",
            "internalErrorCode": "COMMON_ERROR_GENERIC",
            "details": {}
        },
        status=400,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    with pytest.raises(ProgrammingError) as exc_info:
        client.execute_query("SELECT * FROM invalid syntax")

    assert "SQL syntax error" in str(exc_info.value)
    assert exc_info.value.http_status == 400


@responses.activate
def test_get_query_status_with_long_polling():
    """Test query status with long-polling."""
    def check_request(request):
        assert "waitTimeMs" in request.url
        return (200, {}, '{"status": {"queryId": "q1", "completionStatus": "Running", "progress": 0.8, "rowCount": 0, "chunkCount": 0}}')

    responses.add_callback(
        responses.GET,
        "https://test.salesforce.com/services/data/v64.0/ssot/query-sql/q1",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        instance_url="https://test.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    status = client.get_query_status("q1", wait_time_ms=5000)
    assert status.is_running()


@responses.activate
def test_fetch_results():
    """Test fetching query results."""
    responses.add(
        responses.GET,
        "https://test.salesforce.com/services/data/v64.0/ssot/query-sql/query123/rows",
        json={
            "data": [["row1"], ["row2"]],
            "returnedRows": 2,
        },
        status=200,
    )

    client = DataCloudQueryClient(
        instance_url="https://test.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    response = client.fetch_results("query123", offset=0, row_limit=100)

    assert len(response.data) == 2
    assert response.returned_rows == 2


@responses.activate
def test_fetch_results_with_offset():
    """Test fetching results with pagination offset."""
    def check_request(request):
        assert "offset=100" in request.url
        assert "rowLimit=50" in request.url
        assert "omitSchema=true" in request.url
        return (200, {}, '{"data": [["row101"]], "returnedRows": 1}')

    responses.add_callback(
        responses.GET,
        "https://test.salesforce.com/services/data/v64.0/ssot/query-sql/query123/rows",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        instance_url="https://test.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    response = client.fetch_results("query123", offset=100, row_limit=50)
    assert response.returned_rows == 1


@responses.activate
def test_cancel_query():
    """Test canceling a query."""
    responses.add(
        responses.DELETE,
        "https://test.salesforce.com/services/data/v64.0/ssot/query-sql/query123",
        status=204,
    )

    client = DataCloudQueryClient(
        instance_url="https://test.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    # Should not raise
    client.cancel_query("query123")


@responses.activate
def test_retry_on_500():
    """Test automatic retry on 500 errors."""
    # First two calls fail with 500, third succeeds
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/data/v64.0/ssot/query-sql",
        json={"error": "Internal Server Error"},
        status=500,
    )
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/data/v64.0/ssot/query-sql",
        json={"error": "Internal Server Error"},
        status=500,
    )
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/data/v64.0/ssot/query-sql",
        json={
            "data": [],
            "metadata": [],
            "returnedRows": 0,
            "status": {
                "queryId": "q1",
                "completionStatus": "ResultsProduced",
                "progress": 1.0,
                "rowCount": 0,
                "chunkCount": 0,
            },
        },
        status=200,
    )

    client = DataCloudQueryClient(
        instance_url="https://test.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    # Should succeed after retries
    response = client.execute_query("SELECT 1")
    assert response.status.query_id == "q1"
    assert len(responses.calls) == 3


@responses.activate
def test_no_retry_on_400():
    """Test that 400 errors are not retried."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/data/v64.0/ssot/query-sql",
        json={"message": "SQL syntax error"},
        status=400,
    )

    client = DataCloudQueryClient(
        instance_url="https://test.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    with pytest.raises(ProgrammingError):
        client.execute_query("SELECT * FROM invalid syntax")

    # Should only make one call (no retries)
    assert len(responses.calls) == 1


@responses.activate
def test_workload_parameter():
    """Test that workload parameter is included in requests."""
    def check_request(request):
        assert "workload=my_app" in request.url
        return (200, {}, '{"data": [], "metadata": [], "returnedRows": 0, "status": {"queryId": "q1", "completionStatus": "ResultsProduced", "progress": 1.0, "rowCount": 0, "chunkCount": 0}}')

    responses.add_callback(
        responses.POST,
        "https://test.salesforce.com/services/data/v64.0/ssot/query-sql",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        instance_url="https://test.salesforce.com",
        auth_token_getter=mock_token_getter,
        workload="my_app",
    )

    client.execute_query("SELECT 1")


@responses.activate
def test_dataspace_header():
    """Test that dataspace is included as a request header."""
    def check_request(request):
        assert request.headers.get("dataspace") == "custom_space"
        assert "dataspace" not in request.url
        return (200, {}, '{"data": [], "metadata": [], "returnedRows": 0, "status": {"queryId": "q1", "completionStatus": "ResultsProduced", "progress": 1.0, "rowCount": 0, "chunkCount": 0}}')

    responses.add_callback(
        responses.POST,
        "https://test.salesforce.com/services/data/v64.0/ssot/query-sql",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        instance_url="https://test.salesforce.com",
        auth_token_getter=mock_token_getter,
        dataspace="custom_space",
    )

    client.execute_query("SELECT 1")


@responses.activate
def test_auth_token_included():
    """Test that auth token is included in request headers."""
    def check_request(request):
        assert "Authorization" in request.headers
        assert request.headers["Authorization"] == "Bearer mock_token_12345"
        return (200, {}, '{"data": [], "metadata": [], "returnedRows": 0, "status": {"queryId": "q1", "completionStatus": "ResultsProduced", "progress": 1.0, "rowCount": 0, "chunkCount": 0}}')

    responses.add_callback(
        responses.POST,
        "https://test.salesforce.com/services/data/v64.0/ssot/query-sql",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        instance_url="https://test.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    client.execute_query("SELECT 1")
