"""
Tests for DB-API 2.0 Connection.
"""

import json
from unittest.mock import Mock, patch

import pytest

from salesforce_datacloud_connector.connection import Connection
from salesforce_datacloud_connector.cursor import Cursor
from salesforce_datacloud_connector.exceptions import InterfaceError
from tests._concurrency_helpers import run_concurrently


def create_mock_token_provider():
    """Create a mock token provider for testing."""
    provider = Mock()
    provider.get_tenant_endpoint.return_value = "https://test.c360a.salesforce.com"
    provider.get_cdp_token.return_value = "mock_cdp_token"
    provider.get_cdp_token_and_tenant_endpoint.return_value = (
        "mock_cdp_token",
        "https://test.c360a.salesforce.com",
    )
    return provider


def test_connection_initialization():
    """Test connection initialization."""
    auth = create_mock_token_provider()
    conn = Connection(auth, dataspace="test_space", workload="test_workload")

    assert not conn.closed
    assert conn.dataspace == "test_space"
    assert conn.workload == "test_workload"


def test_create_cursor():
    """Test creating a cursor from connection."""
    auth = create_mock_token_provider()
    conn = Connection(auth)

    cursor = conn.cursor()

    assert isinstance(cursor, Cursor)
    assert cursor._client is not None


def test_create_multiple_cursors():
    """Test creating multiple cursors from same connection."""
    auth = create_mock_token_provider()
    conn = Connection(auth)

    cursor1 = conn.cursor()
    cursor2 = conn.cursor()

    assert cursor1 is not cursor2
    assert isinstance(cursor1, Cursor)
    assert isinstance(cursor2, Cursor)


def test_close_connection():
    """Test closing a connection."""
    auth = create_mock_token_provider()
    conn = Connection(auth)

    assert not conn.closed

    conn.close()

    assert conn.closed


def test_operations_after_close():
    """Test that operations fail after connection is closed."""
    auth = create_mock_token_provider()
    conn = Connection(auth)

    conn.close()

    with pytest.raises(InterfaceError):
        conn.cursor()

    with pytest.raises(InterfaceError):
        conn.commit()

    with pytest.raises(InterfaceError):
        conn.rollback()


def test_commit_noop():
    """Test that commit() is a no-op for read-only driver."""
    auth = create_mock_token_provider()
    conn = Connection(auth)

    # Should not raise
    conn.commit()


def test_rollback_noop():
    """Test that rollback() is a no-op for read-only driver."""
    auth = create_mock_token_provider()
    conn = Connection(auth)

    # Should not raise
    conn.rollback()


def test_context_manager():
    """Test connection as context manager."""
    auth = create_mock_token_provider()

    with Connection(auth) as conn:
        assert not conn.closed
        cursor = conn.cursor()
        assert isinstance(cursor, Cursor)

    # Connection should be closed after exiting context
    assert conn.closed


def test_context_manager_with_exception():
    """Test that connection is closed even if exception occurs."""
    auth = create_mock_token_provider()

    try:
        with Connection(auth) as conn:
            assert not conn.closed
            raise ValueError("Test exception")
    except ValueError:
        pass

    assert conn.closed


def test_multiple_close_calls():
    """Test that multiple close() calls are safe."""
    auth = create_mock_token_provider()
    conn = Connection(auth)

    conn.close()
    assert conn.closed

    # Second close should be safe
    conn.close()
    assert conn.closed


def test_dataspace_property():
    """Test dataspace property."""
    auth = create_mock_token_provider()
    conn = Connection(auth, dataspace="custom_space")

    assert conn.dataspace == "custom_space"


def test_workload_property():
    """Test workload property."""
    auth = create_mock_token_provider()
    conn = Connection(auth, workload="my_app")

    assert conn.workload == "my_app"


def test_default_dataspace():
    """Test default dataspace is None (server applies the org default)."""
    auth = create_mock_token_provider()
    conn = Connection(auth)

    # When no dataspace is supplied the connector forwards None to the API
    # client, which lets the server apply the org's default dataspace.
    assert conn.dataspace is None


def test_no_workload_by_default():
    """Test that workload is None by default."""
    auth = create_mock_token_provider()
    conn = Connection(auth)

    assert conn.workload is None


def test_connect_wires_token_exchanger():
    """Test that connect() creates DataCloudTokenExchanger and passes it to Connection."""
    from unittest.mock import patch
    import salesforce_datacloud_connector as sfdc
    from salesforce_datacloud_connector.auth.token_exchanger import DataCloudTokenExchanger
    from salesforce_datacloud_connector.auth.oauth import JWTAuthenticator

    # Mock the JWT authenticator's token fetch and the exchange.
    with patch.object(JWTAuthenticator, '_fetch_new_token', return_value=("core_token", 7200, "https://test.salesforce.com")), \
         patch.object(DataCloudTokenExchanger, '_exchange_token', return_value=("cdp_token", 7200, "https://tenant.c360a.salesforce.com")):
            # Create connection via connect()
            conn = sfdc.connect(
                login_url="https://login.salesforce.com",
                auth_type="jwt",
                username="test@example.com",
                client_id="test_client_id",
                jwt_private_key="-----BEGIN PRIVATE KEY-----\ntest_key\n-----END PRIVATE KEY-----",
                dataspace="test_ds",
                workload="test_workload"
            )

            # Verify connection is created
            assert conn is not None
            assert isinstance(conn, sfdc.Connection)

            # Verify the client has the correct tenant endpoint (from CDP exchange)
            assert conn._client.tenant_endpoint == "https://tenant.c360a.salesforce.com"

            # Verify token provider is DataCloudTokenExchanger
            assert isinstance(conn._token_provider, DataCloudTokenExchanger)

            conn.close()


def test_connect_wires_client_credentials():
    """Test that connect(auth_type='client_credentials') composes the client-credentials
    authenticator → DataCloudTokenExchanger → Connection (GA go-forward path)."""
    from unittest.mock import patch
    import salesforce_datacloud_connector as sfdc
    from salesforce_datacloud_connector.auth.token_exchanger import DataCloudTokenExchanger
    from salesforce_datacloud_connector.auth.oauth import ClientCredentialsAuthenticator

    # Mock the client-credentials authenticator's token fetch (no user/JWT needed).
    with patch.object(ClientCredentialsAuthenticator, '_fetch_new_token', return_value=("core_token", 7200, "https://test.salesforce.com")), \
         patch.object(DataCloudTokenExchanger, '_exchange_token', return_value=("cdp_token", 7200, "https://tenant.c360a.salesforce.com")):
            conn = sfdc.connect(
                login_url="https://login.salesforce.com",
                auth_type="client_credentials",
                client_id="test_client_id",
                client_secret="test_client_secret",
                dataspace="test_ds",
                workload="test_workload",
            )

            assert conn is not None
            assert isinstance(conn, sfdc.Connection)
            # Composed authenticator is the client-credentials flow
            assert isinstance(conn._token_provider._core_authenticator, ClientCredentialsAuthenticator)
            # Tenant endpoint flows through from the CDP exchange
            assert conn._client.tenant_endpoint == "https://tenant.c360a.salesforce.com"
            assert isinstance(conn._token_provider, DataCloudTokenExchanger)

            conn.close()


def test_connect_client_credentials_requires_secret():
    """connect(auth_type='client_credentials') must reject missing client_secret."""
    import pytest
    import salesforce_datacloud_connector as sfdc

    with pytest.raises(ValueError, match="client_credentials auth requires"):
        sfdc.connect(
            login_url="https://login.salesforce.com",
            auth_type="client_credentials",
            client_id="test_client_id",
            # client_secret intentionally omitted
        )


def test_connect_positional_args_backward_compatible():
    """connect()'s pre-target_org positional parameter order (…, dataspace,
    workload) must still bind correctly — target_org must not shift dataspace
    and workload out of their original positions for existing positional
    callers."""
    from unittest.mock import patch
    import salesforce_datacloud_connector as sfdc
    from salesforce_datacloud_connector.auth.token_exchanger import DataCloudTokenExchanger
    from salesforce_datacloud_connector.auth.oauth import ClientCredentialsAuthenticator

    with patch.object(ClientCredentialsAuthenticator, '_fetch_new_token', return_value=("core_token", 7200, "https://test.salesforce.com")), \
         patch.object(DataCloudTokenExchanger, '_exchange_token', return_value=("cdp_token", 7200, "https://tenant.c360a.salesforce.com")):
            conn = sfdc.connect(
                "https://login.salesforce.com",  # login_url
                "client_credentials",  # auth_type
                None,  # username
                None,  # password
                "test_client_id",  # client_id
                "test_client_secret",  # client_secret
                None,  # jwt_private_key
                None,  # refresh_token
                "test_ds",  # dataspace
                "test_workload",  # workload
            )

            assert conn.dataspace == "test_ds"
            assert conn.workload == "test_workload"

            conn.close()


def test_connect_wires_sf_cli():
    """connect(auth_type='sf_cli') composes SfCliAuthenticator → DataCloudTokenExchanger
    → Connection, with no connected-app credentials required (local/dev flow)."""
    from unittest.mock import patch
    import salesforce_datacloud_connector as sfdc
    from salesforce_datacloud_connector.auth.token_exchanger import DataCloudTokenExchanger
    from salesforce_datacloud_connector.auth.oauth import SfCliAuthenticator

    with patch.object(SfCliAuthenticator, '_fetch_new_token', return_value=("core_token", 7200, "https://test.salesforce.com")), \
         patch.object(DataCloudTokenExchanger, '_exchange_token', return_value=("cdp_token", 7200, "https://tenant.c360a.salesforce.com")):
            conn = sfdc.connect(
                auth_type="sf_cli",
                target_org="my-alias",
                dataspace="test_ds",
                workload="test_workload",
            )

            assert conn is not None
            assert isinstance(conn, sfdc.Connection)
            authenticator = conn._token_provider._core_authenticator
            assert isinstance(authenticator, SfCliAuthenticator)
            assert authenticator.target_org == "my-alias"
            assert conn._client.tenant_endpoint == "https://tenant.c360a.salesforce.com"

            conn.close()


# ---------------------------------------------------------------------------
# Concurrency: one Connection shared across threads (threadsafety=2).
#
# Cursors are thread-CONFINED, but creating them from a shared Connection must
# be safe: cursor() must hand every thread its own distinct Cursor bound to the
# one shared client, and the benign close()/cursor() race must never corrupt
# state. These characterize the connection's existing safety so the
# threadsafety=2 promise has a regression guard.
# ---------------------------------------------------------------------------


def test_concurrent_cursor_creation_returns_distinct_cursors():
    """N threads calling cursor() on one shared Connection each get a distinct
    Cursor object, all bound to the connection's single shared client."""
    conn = Connection(create_mock_token_provider())
    parties = 16

    def worker(_i):
        return conn.cursor()

    results, errors = run_concurrently(worker, parties)

    assert errors == []
    assert all(isinstance(c, Cursor) for c in results)
    # Every cursor is a distinct object...
    assert len({id(c) for c in results}) == parties
    # ...yet all share the connection's one client instance.
    assert all(c._client is conn._client for c in results)


def test_concurrent_close_and_cursor_race_is_benign():
    """Threads racing cursor() against close() never raise anything other than
    the documented InterfaceError, and the connection ends up closed."""
    conn = Connection(create_mock_token_provider())
    parties = 16

    def worker(i):
        if i % 4 == 0:
            conn.close()
            return "closed"
        try:
            return conn.cursor()
        except InterfaceError:
            # Legal outcome: connection was closed by a racing thread.
            return "interface_error"

    results, errors = run_concurrently(worker, parties)

    # No unexpected exception type escaped — only the documented InterfaceError,
    # which workers convert to a sentinel. Any other raise would appear here.
    assert errors == []
    assert conn.closed
    # Every result is one of the three legal outcomes.
    assert all(
        r == "closed" or r == "interface_error" or isinstance(r, Cursor)
        for r in results
    )


# ---------------------------------------------------------------------------
# Regression: token + tenant-endpoint must travel together per request.
#
# A Connection wraps a DataCloudTokenExchanger, whose token and tenant
# endpoint are published together as one atomic snapshot per exchange (see
# auth/token_exchanger.py). But Connection.__init__ used to capture the
# tenant endpoint ONCE (at construction time) while wiring a fresh-token-per-
# request callback for the token. After a re-exchange that returns a
# DIFFERENT tenant endpoint (e.g. multi-tenant failover after the CDP token
# is invalidated/expires), outgoing requests would keep hitting the
# ORIGINALLY-captured endpoint with the NEW token — a torn pair the exchanger
# itself was designed to prevent.
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Minimal stand-in for requests.Response, matching test_threadsafety.py's
    fake so query-status headers can be supplied without hitting the network."""

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


def _status_header(query_id):
    return {
        "x-hyperdb-status": json.dumps(
            {
                "queryId": query_id,
                "completionStatus": "RESULTS_PRODUCED",
                "progress": 1.0,
                "rowCount": 0,
                "chunkCount": 0,
            }
        )
    }


def test_query_after_endpoint_change_targets_new_endpoint_with_new_token():
    """Regression: once the token provider's (token, tenant_endpoint) pair
    changes — simulating a re-exchange after expiry/invalidation that landed
    on a different tenant — a subsequent query MUST be sent to the NEW
    endpoint using the NEW token. It must never send the new token to the
    stale, originally-captured endpoint."""
    provider = Mock()
    # Endpoint A / token A at construction time...
    provider.get_tenant_endpoint.return_value = "https://tenantA.c360a.salesforce.com"
    provider.get_cdp_token.return_value = "token_A"
    provider.get_cdp_token_and_tenant_endpoint.return_value = (
        "token_A",
        "https://tenantA.c360a.salesforce.com",
    )

    conn = Connection(provider)

    # ...then the exchanger performs a re-exchange (expiry/invalidation) that
    # returns a DIFFERENT tenant endpoint paired with a new token.
    provider.get_tenant_endpoint.return_value = "https://tenantB.c360a.salesforce.com"
    provider.get_cdp_token.return_value = "token_B"
    provider.get_cdp_token_and_tenant_endpoint.return_value = (
        "token_B",
        "https://tenantB.c360a.salesforce.com",
    )

    captured = {}

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        captured["url"] = url
        captured["authorization"] = headers.get("Authorization") if headers else None
        return _FakeResponse(200, {"metadata": {"columns": []}, "data": [], "returnedRows": 0},
                              headers=_status_header("q1"))

    with patch("requests.request", side_effect=fake_request):
        cursor = conn.cursor()
        cursor.execute("SELECT 1")

    # The request must have gone to the NEW endpoint...
    assert captured["url"].startswith("https://tenantB.c360a.salesforce.com")
    # ...carrying the NEW token...
    assert captured["authorization"] == "Bearer token_B"
    # ...never the new token paired with the stale endpoint A.
    assert "tenantA" not in captured["url"]
