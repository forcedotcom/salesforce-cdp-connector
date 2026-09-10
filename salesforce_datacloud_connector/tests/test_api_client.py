"""
Tests for Data Cloud Query API client.
"""

import json
from unittest.mock import patch

import pytest
import responses

from salesforce_datacloud_connector.api.client import DataCloudQueryClient
from salesforce_datacloud_connector.api.models import QueryStatus
from salesforce_datacloud_connector.exceptions import OperationalError, ProgrammingError
from tests._concurrency_helpers import ConcurrencyGate


def mock_token_getter():
    """Mock token getter for tests."""
    return "mock_token_12345"


class _FakeResponse:
    """Minimal stand-in for requests.Response used by the concurrency tests.

    ``responses`` mutates a global registry and is not thread-safe, so the
    shared-client concurrency tests patch ``requests.request`` directly with a
    thread-safe fake instead.
    """

    def __init__(self, status_code, json_body, headers=None):
        self.status_code = status_code
        self._json = json_body
        self.headers = headers or {}
        self.text = ""

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        return self._json


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
def test_named_parameters_translated_to_qmark_v3():
    """The driver advertises paramstyle='named', but v3 accepts only positional
    (qmark) parameters. A :name placeholder in the SQL must be rewritten to ?
    and its value emitted positionally, or the server rejects the query with
    "conflicting parameter style 'named' ... set to 'qmark'"."""
    def check_request(request):
        body = json.loads(request.body.decode("utf-8"))
        assert body["sql"] == "SELECT ? AS msg"
        assert body["parameters"] == [{"type": "varchar", "value": "hi there"}]

        status_header = {
            "queryId": "q1", "completionStatus": "RESULTS_PRODUCED",
            "progress": 1.0, "rowCount": 0, "chunkCount": 0,
        }
        return (200, {"x-hyperdb-status": json.dumps(status_header)},
                json.dumps({"metadata": {"columns": []}, "data": [], "returnedRows": 0}))

    responses.add_callback(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )
    client.execute_query("SELECT :greeting AS msg", parameters={"greeting": "hi there"})


@responses.activate
def test_named_parameter_reuse_translated_positionally_v3():
    """A named parameter used more than once expands to one positional ? per
    occurrence, with the value repeated in the parameters array."""
    def check_request(request):
        body = json.loads(request.body.decode("utf-8"))
        assert body["sql"] == "SELECT ? AS a, ? + 1 AS b"
        assert body["parameters"] == [
            {"type": "numeric", "value": 7},
            {"type": "numeric", "value": 7},
        ]

        status_header = {
            "queryId": "q1", "completionStatus": "RESULTS_PRODUCED",
            "progress": 1.0, "rowCount": 0, "chunkCount": 0,
        }
        return (200, {"x-hyperdb-status": json.dumps(status_header)},
                json.dumps({"metadata": {"columns": []}, "data": [], "returnedRows": 0}))

    responses.add_callback(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )
    client.execute_query("SELECT :n AS a, :n + 1 AS b", parameters={"n": 7})


@responses.activate
def test_qmark_sql_passes_through_unchanged_v3():
    """SQL that already uses positional ? placeholders (e.g. internal catalog
    queries) has no :name tokens: the SQL is left untouched and the parameter
    array is built from the dict's insertion order."""
    def check_request(request):
        body = json.loads(request.body.decode("utf-8"))
        assert body["sql"] == "SELECT * FROM t WHERE a = ? AND b = ?"
        assert body["parameters"] == [
            {"type": "varchar", "value": "x"},
            {"type": "varchar", "value": "y"},
        ]

        status_header = {
            "queryId": "q1", "completionStatus": "RESULTS_PRODUCED",
            "progress": 1.0, "rowCount": 0, "chunkCount": 0,
        }
        return (200, {"x-hyperdb-status": json.dumps(status_header)},
                json.dumps({"metadata": {"columns": []}, "data": [], "returnedRows": 0}))

    responses.add_callback(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )
    # dict order (a then b) defines positional order for qmark SQL
    client.execute_query(
        "SELECT * FROM t WHERE a = ? AND b = ?",
        parameters={"a": "x", "b": "y"},
    )


@responses.activate
def test_named_translation_leaves_type_casts_alone_v3():
    """Postgres :: type casts must not be mistaken for :name placeholders when
    translating. Only the genuine :name (present in params) is rewritten."""
    def check_request(request):
        body = json.loads(request.body.decode("utf-8"))
        # ::regclass cast preserved; :kind rewritten to ?
        assert body["sql"] == "SELECT 'x'::regclass WHERE relkind = ?"
        assert body["parameters"] == [{"type": "varchar", "value": "r"}]

        status_header = {
            "queryId": "q1", "completionStatus": "RESULTS_PRODUCED",
            "progress": 1.0, "rowCount": 0, "chunkCount": 0,
        }
        return (200, {"x-hyperdb-status": json.dumps(status_header)},
                json.dumps({"metadata": {"columns": []}, "data": [], "returnedRows": 0}))

    responses.add_callback(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )
    client.execute_query(
        "SELECT 'x'::regclass WHERE relkind = :kind",
        parameters={"kind": "r"},
    )


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
def test_poll_until_complete_v3():
    """Test polling until query completes (v3 API)."""
    # First poll: still running
    responses.add(
        responses.GET,
        "https://test.c360a.salesforce.com/api/v3/query/query123",
        json={
            "metadata": {"columns": []},
            "data": None,
            "returnedRows": 0
        },
        headers={
            "x-hyperdb-status": json.dumps({
                "queryId": "query123",
                "completionStatus": "RUNNING_OR_UNSPECIFIED",
                "progress": 0.5,
                "rowCount": 0,
                "chunkCount": 0,
            })
        },
        status=200,
    )

    # Second poll: completed
    responses.add(
        responses.GET,
        "https://test.c360a.salesforce.com/api/v3/query/query123",
        json={
            "metadata": {"columns": []},
            "data": None,
            "returnedRows": 0
        },
        headers={
            "x-hyperdb-status": json.dumps({
                "queryId": "query123",
                "completionStatus": "FINISHED",
                "progress": 1.0,
                "rowCount": 100,
                "chunkCount": 1,
            })
        },
        status=200,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    status = client.poll_until_complete("query123", poll_interval_ms=100, timeout_seconds=10)

    assert status.is_complete()
    assert status.row_count == 100


@responses.activate
def test_get_query_status_with_long_polling():
    """Test query status with long-polling."""
    def check_request(request):
        assert "waitTimeMs" in request.url
        status_header = {
            "queryId": "q1",
            "completionStatus": "RUNNING_OR_UNSPECIFIED",
            "progress": 0.8,
            "rowCount": 0,
            "chunkCount": 0
        }
        return (200, {"x-hyperdb-status": json.dumps(status_header)}, '{"metadata":{"columns":[]},"data":null,"returnedRows":0}')

    responses.add_callback(
        responses.GET,
        "https://test.c360a.salesforce.com/api/v3/query/q1",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    status = client.get_query_status("q1", wait_time_ms=5000)
    assert status.is_running()


@responses.activate
def test_fetch_results_with_offset():
    """Test fetching results with pagination offset."""
    def check_request(request):
        assert "offset=100" in request.url
        assert "limit=50" in request.url
        assert "byteLimit=20971520" in request.url
        assert "rowLimit" not in request.url
        assert "omitSchema" not in request.url
        return (200, {}, '{"data": [["row101"]], "returnedRows": 1}')

    responses.add_callback(
        responses.GET,
        "https://test.c360a.salesforce.com/api/v3/query/query123/rows",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    response = client.fetch_results("query123", offset=100, row_limit=50)
    assert response.returned_rows == 1


@responses.activate
def test_retry_on_500():
    """Test automatic retry on 500 errors."""
    # First two calls fail with 500, third succeeds
    responses.add(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        json={"error": "Internal Server Error"},
        status=500,
    )
    responses.add(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        json={"error": "Internal Server Error"},
        status=500,
    )
    status_header = {
        "queryId": "q1",
        "completionStatus": "RESULTS_PRODUCED",
        "progress": 1.0,
        "rowCount": 0,
        "chunkCount": 0,
    }
    responses.add(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        json={
            "metadata": {"columns": []},
            "data": [],
            "returnedRows": 0,
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

    # Should succeed after retries
    response = client.execute_query("SELECT 1")
    assert response.status.query_id == "q1"
    assert len(responses.calls) == 3


@responses.activate
def test_no_retry_on_400():
    """Test that 400 errors are not retried."""
    responses.add(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        json={"message": "SQL syntax error"},
        status=400,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
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
        assert request.headers.get("x-hyperdb-workload") == "python-connector-v2_my_app"
        assert "workload=" not in request.url
        status_header = {
            "queryId": "q1",
            "completionStatus": "RESULTS_PRODUCED",
            "progress": 1.0,
            "rowCount": 0,
            "chunkCount": 0
        }
        return (200, {"x-hyperdb-status": json.dumps(status_header)}, '{"metadata":{"columns":[]},"data":[],"returnedRows":0}')

    responses.add_callback(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
        workload="my_app",
    )

    client.execute_query("SELECT 1")


@responses.activate
def test_dataspace_header():
    """Test that dataspace is included as a request header."""
    def check_request(request):
        assert request.headers.get("ctx-dataspace-ds_name") == "custom_space"
        assert "dataspace" not in request.url
        status_header = {
            "queryId": "q1",
            "completionStatus": "RESULTS_PRODUCED",
            "progress": 1.0,
            "rowCount": 0,
            "chunkCount": 0
        }
        return (200, {"x-hyperdb-status": json.dumps(status_header)}, '{"metadata":{"columns":[]},"data":[],"returnedRows":0}')

    responses.add_callback(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
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
        status_header = {
            "queryId": "q1",
            "completionStatus": "RESULTS_PRODUCED",
            "progress": 1.0,
            "rowCount": 0,
            "chunkCount": 0
        }
        return (200, {"x-hyperdb-status": json.dumps(status_header)}, '{"metadata":{"columns":[]},"data":[],"returnedRows":0}')

    responses.add_callback(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        callback=check_request,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    client.execute_query("SELECT 1")


@responses.activate
def test_execute_query_missing_status_header_raises_operational_error():
    """A 200 response with no x-hyperdb-status header must raise a clear
    OperationalError, not a downstream AttributeError (status lives only in
    the header in v3)."""
    responses.add(
        responses.POST,
        "https://test.c360a.salesforce.com/api/v3/query",
        json={
            "metadata": {"columns": [{"name": "name", "type": "varchar", "nullable": True}]},
            "data": [["Alice"]],
            "returnedRows": 1,
        },
        # NOTE: deliberately no x-hyperdb-status header
        status=200,
    )

    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )

    with pytest.raises(OperationalError, match="x-hyperdb-status"):
        client.execute_query("SELECT name FROM users")


def test_query_status_from_dict_real_payload():
    """Verbatim shape v3 emits in the x-hyperdb-status response header."""
    status = QueryStatus.from_dict(
        {
            "queryId": "q1",
            "completionStatus": "Finished",
            "progress": 1.0,
            "rowCount": 1288,
            "chunkCount": 1,
            "expirationTime": "2025-09-26T10:55:07.438Z",
        }
    )

    assert status.query_id == "q1"
    assert status.is_complete()
    assert status.row_count == 1288
    assert status.chunk_count == 1
    assert status.progress == 1.0
    assert status.expiration_time == "2025-09-26T10:55:07.438Z"


def test_query_status_from_empty_dict_does_not_throw():
    status = QueryStatus.from_dict({})
    assert status.query_id == ""
    assert status.completion_status == ""
    assert status.progress == 0.0
    assert status.row_count == 0
    assert status.chunk_count == 0
    assert status.expiration_time is None


def test_query_status_unknown_keys_ignored():
    """Forward-compat: extra fields the server may add later must not raise."""
    status = QueryStatus.from_dict(
        {
            "queryId": "q1",
            "completionStatus": "Finished",
            "progress": 1.0,
            "rowCount": 0,
            "chunkCount": 0,
            "newServerSideField": {"foo": "bar"},
        }
    )
    assert status.is_complete()


# ---------------------------------------------------------------------------
# Concurrency: a single DataCloudQueryClient shared across threads.
#
# threadsafety=2 requires that one connection (hence its one shared query
# client) be usable from multiple threads at once. The client holds no
# per-request mutable state — each call builds its own request body, headers,
# and parses its own response — so concurrent callers must not see each other's
# query IDs or rows bleed across. These tests pin that invariant so a future
# change that introduces shared mutable request state fails loudly.
# ---------------------------------------------------------------------------


def _status_header(query_id):
    return {
        "x-hyperdb-status": json.dumps(
            {
                "queryId": query_id,
                "completionStatus": "RESULTS_PRODUCED",
                "progress": 1.0,
                "rowCount": 1,
                "chunkCount": 1,
            }
        )
    }


def test_shared_client_concurrent_execute_query_keeps_responses_distinct():
    """N threads issuing distinct queries through ONE shared client each get
    back their own query id and rows — no cross-thread bleed of request or
    response state."""
    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )
    parties = 12
    gate = ConcurrencyGate()

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        # The per-thread query id is carried in the SQL so the fake can echo it
        # back in both the body and the status header. Hold every caller inside
        # the request so their responses are genuinely built concurrently.
        sql = json["sql"]
        gate.enter()
        marker = sql.split("'")[1]
        return _FakeResponse(
            200,
            {"metadata": {"columns": [{"name": "m", "type": "varchar", "nullable": True}]},
             "data": [[marker]], "returnedRows": 1},
            headers=_status_header(f"query_{marker}"),
        )

    def worker(i):
        return client.execute_query(f"SELECT '{i}' AS m")

    # Patch once in the main thread: patch() setattrs requests.request globally,
    # so the fake is visible to every worker thread. All threads start and join
    # inside this context, so the patch outlives them. (Patching per-thread
    # would race — concurrent setattr on the same module attribute.)
    with patch("requests.request", side_effect=fake_request):
        results, errors = gate.run(worker, parties)

    assert errors == []
    assert gate.entered == parties
    assert gate.max_concurrent == parties  # all genuinely in-flight together
    # Each worker sees exactly its own marker echoed back — no cross-talk.
    for i, resp in enumerate(results):
        assert resp.status.query_id == f"query_{i}"
        assert resp.data == [[str(i)]]


def test_shared_client_concurrent_fetch_results_keeps_rows_distinct():
    """Concurrent fetch_results() calls on one shared client return each
    caller's own rows, proving fetch has no shared per-call buffer."""
    client = DataCloudQueryClient(
        tenant_endpoint="https://test.c360a.salesforce.com",
        auth_token_getter=mock_token_getter,
    )
    parties = 12
    gate = ConcurrencyGate()

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        # URL is .../query/{query_id}/rows — echo the id back as the row value.
        query_id = url.split("/query/")[1].split("/rows")[0]
        gate.enter()
        return _FakeResponse(
            200,
            {"metadata": [], "data": [[query_id]], "returnedRows": 1},
        )

    def worker(i):
        return client.fetch_results(query_id=f"q{i}", offset=0)

    with patch("requests.request", side_effect=fake_request):
        results, errors = gate.run(worker, parties)

    assert errors == []
    assert gate.entered == parties
    for i, resp in enumerate(results):
        assert resp.data == [[f"q{i}"]]
