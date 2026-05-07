"""
Tests for OAuth authentication.
"""

import time
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
def test_token_caching():
    """Test that tokens are cached and reused."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "cached_token",
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

    # First call - should hit the API
    token1 = auth.get_oauth_token()
    assert len(responses.calls) == 1

    # Second call - should use cached token
    token2 = auth.get_oauth_token()
    assert len(responses.calls) == 1  # No additional API call
    assert token1 == token2


@responses.activate
def test_token_refresh_before_expiry():
    """Test that tokens are refreshed before they expire (60s buffer)."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "token1",
            "expires_in": 100,  # 100 seconds
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

    # Get first token
    token1 = auth.get_oauth_token()
    assert token1 == "token1"

    # Simulate time passing (50 seconds - within 60s buffer of expiry)
    with patch("time.time", return_value=time.time() + 50):
        token2 = auth.get_oauth_token()
        # Should get new token because we're within 60s of expiry
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


def test_token_invalidation():
    """Test that token invalidation forces a new fetch."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            "https://test.salesforce.com/services/oauth2/token",
            json={"access_token": "token1", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
            status=200,
        )
        rsps.add(
            responses.POST,
            "https://test.salesforce.com/services/oauth2/token",
            json={"access_token": "token2", "expires_in": 7200, "instance_url": "https://myorg.my.salesforce.com"},
            status=200,
        )

        auth = UsernamePasswordAuthenticator(
            login_url="https://test.salesforce.com",
            username="test@example.com",
            password="password123",
            client_id="client_id",
            client_secret="client_secret",
        )

        # Get first token
        token1 = auth.get_oauth_token()
        assert token1 == "token1"

        # Invalidate and get new token
        auth.invalidate_token()
        token2 = auth.get_oauth_token()
        assert token2 == "token2"
        assert len(rsps.calls) == 2


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

    # Get instance URL should trigger OAuth fetch if not cached
    instance_url = auth.get_instance_url()
    assert instance_url == "https://myorg.my.salesforce.com"

    # Verify token was also cached
    token = auth.get_oauth_token()
    assert token == "test_token"
    assert len(responses.calls) == 1  # Only one API call


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
