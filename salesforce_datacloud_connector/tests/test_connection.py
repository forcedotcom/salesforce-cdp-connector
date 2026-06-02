"""
Tests for DB-API 2.0 Connection.
"""

from unittest.mock import Mock

import pytest

from salesforce_datacloud_connector.auth.oauth import OAuthAuthenticator
from salesforce_datacloud_connector.connection import Connection
from salesforce_datacloud_connector.cursor import Cursor
from salesforce_datacloud_connector.exceptions import InterfaceError


def create_mock_authenticator():
    """Create a mock authenticator for testing."""
    auth = Mock(spec=OAuthAuthenticator)
    auth.get_instance_url.return_value = "https://test.salesforce.com"
    auth.get_oauth_token.return_value = "mock_token"
    return auth


def test_connection_initialization():
    """Test connection initialization."""
    auth = create_mock_authenticator()
    conn = Connection(auth, dataspace="test_space", workload="test_workload")

    assert not conn.closed
    assert conn.dataspace == "test_space"
    assert conn.workload == "test_workload"


def test_create_cursor():
    """Test creating a cursor from connection."""
    auth = create_mock_authenticator()
    conn = Connection(auth)

    cursor = conn.cursor()

    assert isinstance(cursor, Cursor)
    assert cursor._client is not None


def test_create_multiple_cursors():
    """Test creating multiple cursors from same connection."""
    auth = create_mock_authenticator()
    conn = Connection(auth)

    cursor1 = conn.cursor()
    cursor2 = conn.cursor()

    assert cursor1 is not cursor2
    assert isinstance(cursor1, Cursor)
    assert isinstance(cursor2, Cursor)


def test_close_connection():
    """Test closing a connection."""
    auth = create_mock_authenticator()
    conn = Connection(auth)

    assert not conn.closed

    conn.close()

    assert conn.closed


def test_operations_after_close():
    """Test that operations fail after connection is closed."""
    auth = create_mock_authenticator()
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
    auth = create_mock_authenticator()
    conn = Connection(auth)

    # Should not raise
    conn.commit()


def test_rollback_noop():
    """Test that rollback() is a no-op for read-only driver."""
    auth = create_mock_authenticator()
    conn = Connection(auth)

    # Should not raise
    conn.rollback()


def test_context_manager():
    """Test connection as context manager."""
    auth = create_mock_authenticator()

    with Connection(auth) as conn:
        assert not conn.closed
        cursor = conn.cursor()
        assert isinstance(cursor, Cursor)

    # Connection should be closed after exiting context
    assert conn.closed


def test_context_manager_with_exception():
    """Test that connection is closed even if exception occurs."""
    auth = create_mock_authenticator()

    try:
        with Connection(auth) as conn:
            assert not conn.closed
            raise ValueError("Test exception")
    except ValueError:
        pass

    assert conn.closed


def test_multiple_close_calls():
    """Test that multiple close() calls are safe."""
    auth = create_mock_authenticator()
    conn = Connection(auth)

    conn.close()
    assert conn.closed

    # Second close should be safe
    conn.close()
    assert conn.closed


def test_dataspace_property():
    """Test dataspace property."""
    auth = create_mock_authenticator()
    conn = Connection(auth, dataspace="custom_space")

    assert conn.dataspace == "custom_space"


def test_workload_property():
    """Test workload property."""
    auth = create_mock_authenticator()
    conn = Connection(auth, workload="my_app")

    assert conn.workload == "my_app"


def test_default_dataspace():
    """Test default dataspace is None (server applies the org default)."""
    auth = create_mock_authenticator()
    conn = Connection(auth)

    # When no dataspace is supplied the connector forwards None to the API
    # client, which lets the server apply the org's default dataspace.
    assert conn.dataspace is None


def test_no_workload_by_default():
    """Test that workload is None by default."""
    auth = create_mock_authenticator()
    conn = Connection(auth)

    assert conn.workload is None
