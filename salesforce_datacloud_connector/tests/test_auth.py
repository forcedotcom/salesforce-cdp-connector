"""
Tests for OAuth authentication.
"""

import threading
from unittest.mock import patch

import pytest
import responses

from salesforce_datacloud_connector.auth.oauth import (
    JWTAuthenticator,
    RefreshTokenAuthenticator,
    UsernamePasswordAuthenticator,
)
from salesforce_datacloud_connector.exceptions import OperationalError
from tests._concurrency_helpers import ConcurrencyGate


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


# ---------------------------------------------------------------------------
# Concurrency (threadsafety=2): a single authenticator shared across threads.
# ---------------------------------------------------------------------------


def _make_username_authenticator():
    return UsernamePasswordAuthenticator(
        login_url="https://test.salesforce.com",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
    )


def test_concurrent_get_instance_url_fetches_exactly_once():
    """N threads calling get_instance_url() on a cold authenticator must trigger
    exactly ONE token fetch. instance_url is stable org metadata; the unlocked
    check-then-fetch admits every caller (thundering herd), so this fails until
    get_instance_url() dedups the cold miss under a lock (double-checked)."""
    auth = _make_username_authenticator()
    parties = 8
    gate = ConcurrencyGate()

    def fake_fetch():
        gate.enter()  # counts every caller that reaches the fetch
        return ("token", 7200, "https://myorg.my.salesforce.com")

    def worker(_i):
        return auth.get_instance_url()

    with patch.object(auth, "_fetch_new_token", side_effect=fake_fetch):
        results, errors = gate.run(worker, parties)

    assert errors == []
    assert all(r == "https://myorg.my.salesforce.com" for r in results)
    assert gate.entered == 1  # deduplicated: only one thread fetched
    assert gate.max_concurrent == 1


def test_get_oauth_token_and_instance_url_returns_matched_pair_under_concurrency():
    """get_oauth_token_and_instance_url() must return a (token, instance_url)
    pair drawn from the SAME fetch. Composing get_oauth_token() +
    get_instance_url() as two separate calls could pair one thread's token with
    another thread's instance_url; the atomic pair method must never mismatch.

    Each fetch here returns a distinct token whose org number is embedded in
    both the token and its instance_url, so any cross-fetch pairing is caught."""
    auth = _make_username_authenticator()
    parties = 16
    gate = ConcurrencyGate()
    counter = {"n": 0}
    counter_lock = threading.Lock()

    def fake_fetch():
        with counter_lock:
            counter["n"] += 1
            n = counter["n"]
        gate.enter()  # hold all callers together so fetches genuinely overlap
        return (f"token{n}", 7200, f"https://org{n}.my.salesforce.com")

    def worker(_i):
        return auth.get_oauth_token_and_instance_url()

    with patch.object(auth, "_fetch_new_token", side_effect=fake_fetch):
        results, errors = gate.run(worker, parties)

    assert errors == []
    # Core tokens are never cached: every caller performs its own fresh fetch.
    assert gate.entered == parties
    assert gate.max_concurrent == parties
    # Every returned pair is internally consistent — url matches its own token.
    for token, url in results:
        n = token[len("token"):]
        assert url == f"https://org{n}.my.salesforce.com"
