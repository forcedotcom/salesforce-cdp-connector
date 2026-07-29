"""
Tests for DB-API 2.0 Connection.
"""

from unittest.mock import Mock

import pytest

from salesforce_datacloud_connector.connection import Connection
from salesforce_datacloud_connector.cursor import Cursor
from salesforce_datacloud_connector.exceptions import InterfaceError


def create_mock_token_provider():
    """Create a mock token provider for testing."""
    provider = Mock()
    provider.get_tenant_endpoint.return_value = "https://test.c360a.salesforce.com"
    provider.get_cdp_token.return_value = "mock_cdp_token"
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

    # Mock the JWT authenticator's token fetch, the exchange, and the revoke
    # (patch _revoke_core_token so no live HTTP call escapes during connect()).
    with patch.object(JWTAuthenticator, '_fetch_new_token', return_value=("core_token", 7200, "https://test.salesforce.com")), \
         patch.object(DataCloudTokenExchanger, '_exchange_token', return_value=("cdp_token", 7200, "https://tenant.c360a.salesforce.com")), \
         patch.object(DataCloudTokenExchanger, '_revoke_core_token', return_value=None):
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
    # Patch _revoke_core_token too so no live HTTP call escapes during connect().
    with patch.object(ClientCredentialsAuthenticator, '_fetch_new_token', return_value=("core_token", 7200, "https://test.salesforce.com")), \
         patch.object(DataCloudTokenExchanger, '_exchange_token', return_value=("cdp_token", 7200, "https://tenant.c360a.salesforce.com")), \
         patch.object(DataCloudTokenExchanger, '_revoke_core_token', return_value=None):
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
