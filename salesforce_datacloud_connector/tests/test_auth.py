"""
Tests for OAuth authentication.
"""

from unittest.mock import patch

import pytest
import responses

from salesforce_datacloud_connector.auth.oauth import (
    JWTAuthenticator,
    RefreshTokenAuthenticator,
    UsernamePasswordAuthenticator,
)
from salesforce_datacloud_connector.exceptions import OperationalError


@responses.activate
def test_username_password_auth_success():
    """Test successful username/password authentication."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "test_token_12345",
            "expires_in": 7200,
            "token_type": "Bearer",
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    auth = UsernamePasswordAuthenticator(
        login_url="https://test.salesforce.com",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
    )

    token = auth.get_oauth_token()
    assert token == "test_token_12345"


@responses.activate
def test_username_password_auth_failure():
    """Test failed username/password authentication."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"error": "invalid_grant"},
        status=400,
    )

    auth = UsernamePasswordAuthenticator(
        login_url="https://test.salesforce.com",
        username="test@example.com",
        password="wrong_password",
        client_id="client_id",
        client_secret="client_secret",
    )

    with pytest.raises(OperationalError):
        auth.get_oauth_token()


@responses.activate
def test_token_not_cached_fetches_each_call():
    """Core tokens are never cached (matches JDBC's getOAuthToken()): each
    get_oauth_token() call fetches fresh, even back-to-back."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "token1",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "token2",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    auth = UsernamePasswordAuthenticator(
        login_url="https://test.salesforce.com",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
    )

    # First call - hits the API
    token1 = auth.get_oauth_token()
    assert token1 == "token1"
    assert len(responses.calls) == 1

    # Second call - fetches again rather than reusing a cache
    token2 = auth.get_oauth_token()
    assert token2 == "token2"
    assert len(responses.calls) == 2


@responses.activate
def test_jwt_auth_success():
    """Test successful JWT authentication."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "jwt_token_12345",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    # Mock private key (not a real key)
    mock_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF6F9qoUPCCMXScQXXMRDdqmqPkCN
-----END RSA PRIVATE KEY-----"""

    with patch("jwt.encode", return_value="mock_jwt_token"):
        auth = JWTAuthenticator(
            login_url="https://test.salesforce.com",
            client_id="client_id",
            username="test@example.com",
            jwt_private_key=mock_private_key,
        )

        token = auth.get_oauth_token()
        assert token == "jwt_token_12345"


@responses.activate
def test_refresh_token_auth_success():
    """Test successful refresh token authentication."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "refresh_access_token",
            "expires_in": 7200,
            "token_type": "Bearer",
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    auth = RefreshTokenAuthenticator(
        login_url="https://test.salesforce.com",
        client_id="client_id",
        client_secret="client_secret",
        refresh_token="refresh_token_xyz",
    )

    token = auth.get_oauth_token()
    assert token == "refresh_access_token"


@responses.activate
def test_login_url_trailing_slash():
    """Test that login URL trailing slashes are handled correctly."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"access_token": "token", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
        status=200,
    )

    # Test with trailing slash
    auth = UsernamePasswordAuthenticator(
        login_url="https://test.salesforce.com/",  # Note trailing slash
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
    )

    token = auth.get_oauth_token()
    assert token == "token"


@responses.activate
def test_get_instance_url():
    """Test that instance URL is extracted from OAuth response."""
    responses.add(
        responses.POST,
        "https://login.salesforce.com/services/oauth2/token",
        json={
            "access_token": "test_token",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    auth = UsernamePasswordAuthenticator(
        login_url="https://login.salesforce.com",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
    )

    # Get instance URL should trigger an OAuth fetch since none has happened yet
    instance_url = auth.get_instance_url()
    assert instance_url == "https://myorg.my.salesforce.com"
    assert len(responses.calls) == 1

    # get_oauth_token() fetches fresh again (no core-token caching)
    token = auth.get_oauth_token()
    assert token == "test_token"
    assert len(responses.calls) == 2


@responses.activate
def test_missing_instance_url_in_response():
    """Test that missing instance_url in OAuth response raises error."""
    responses.add(
        responses.POST,
        "https://login.salesforce.com/services/oauth2/token",
        json={
            "access_token": "test_token",
            "expires_in": 7200,
            # Missing instance_url
        },
        status=200,
    )

    auth = UsernamePasswordAuthenticator(
        login_url="https://login.salesforce.com",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
    )

    with pytest.raises(OperationalError, match="No instance_url in response"):
        auth.get_oauth_token()


@responses.activate
def test_client_credentials_auth_success():
    """Test successful client credentials authentication."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "client_creds_token_12345",
            "expires_in": 7200,
            "token_type": "Bearer",
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    from salesforce_datacloud_connector.auth.oauth import ClientCredentialsAuthenticator

    auth = ClientCredentialsAuthenticator(
        login_url="https://test.salesforce.com",
        client_id="client_id",
        client_secret="client_secret",
    )

    token = auth.get_oauth_token()
    assert token == "client_creds_token_12345"

    # Verify request parameters
    assert len(responses.calls) == 1
    request_body = responses.calls[0].request.body
    assert "grant_type=client_credentials" in request_body
    assert "client_id=client_id" in request_body
    assert "client_secret=client_secret" in request_body


@responses.activate
def test_client_credentials_auth_failure():
    """Test failed client credentials authentication."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={"error": "invalid_client"},
        status=401,
    )

    from salesforce_datacloud_connector.auth.oauth import ClientCredentialsAuthenticator

    auth = ClientCredentialsAuthenticator(
        login_url="https://test.salesforce.com",
        client_id="invalid_client",
        client_secret="wrong_secret",
    )

    with pytest.raises(OperationalError):
        auth.get_oauth_token()
