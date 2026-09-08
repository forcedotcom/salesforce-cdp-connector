"""
Tests for OAuth authentication.
"""

import subprocess
import threading
from unittest.mock import patch

import pytest
import responses

from salesforce_datacloud_connector.auth.oauth import (
    JWTAuthenticator,
    RefreshTokenAuthenticator,
    SfCliAuthenticator,
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


def _completed_process(stdout: str) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")


_ORG_DISPLAY_OK = '{"status": 0, "result": {"instanceUrl": "https://myorg.my.salesforce.com"}}'
_SHOW_TOKEN_OK = '{"status": 0, "result": {"accessToken": "sf_cli_token_12345"}}'


@patch("subprocess.run")
def test_sf_cli_auth_forces_no_color(mock_run):
    """`sf ... --json` can still emit ANSI color codes when FORCE_COLOR is set
    in the environment, which breaks JSON parsing. NO_COLOR=1 must be passed
    to the subprocess env so --json output stays plain regardless of the
    caller's shell settings."""
    mock_run.side_effect = [
        _completed_process(_ORG_DISPLAY_OK),
        _completed_process(_SHOW_TOKEN_OK),
    ]

    SfCliAuthenticator().get_oauth_token()

    for call in mock_run.call_args_list:
        assert call.kwargs["env"]["NO_COLOR"] == "1"


@patch("subprocess.run")
def test_sf_cli_auth_success(mock_run):
    """SfCliAuthenticator combines `org display` (instanceUrl) and
    `org auth show-access-token` (accessToken) into one core token."""
    mock_run.side_effect = [
        _completed_process(_ORG_DISPLAY_OK),
        _completed_process(_SHOW_TOKEN_OK),
    ]

    auth = SfCliAuthenticator(target_org="my-alias")
    token = auth.get_oauth_token()

    assert token == "sf_cli_token_12345"
    assert auth.get_instance_url() == "https://myorg.my.salesforce.com"

    first_call_args = mock_run.call_args_list[0].args[0]
    second_call_args = mock_run.call_args_list[1].args[0]
    assert first_call_args == ["sf", "org", "display", "--json", "--target-org", "my-alias"]
    assert second_call_args == [
        "sf", "org", "auth", "show-access-token", "--json", "--target-org", "my-alias"
    ]


@patch("subprocess.run")
def test_sf_cli_auth_default_org_no_target_flag(mock_run):
    """Without target_org, neither CLI call includes --target-org — the CLI's
    own default-org resolution applies."""
    mock_run.side_effect = [
        _completed_process(_ORG_DISPLAY_OK),
        _completed_process(_SHOW_TOKEN_OK),
    ]

    auth = SfCliAuthenticator()
    auth.get_oauth_token()

    first_call_args = mock_run.call_args_list[0].args[0]
    second_call_args = mock_run.call_args_list[1].args[0]
    assert "--target-org" not in first_call_args
    assert "--target-org" not in second_call_args


@patch("subprocess.run")
def test_sf_cli_auth_cli_not_found(mock_run):
    """A missing `sf` binary should raise a clear OperationalError, not a
    raw FileNotFoundError."""
    mock_run.side_effect = FileNotFoundError()

    auth = SfCliAuthenticator()
    with pytest.raises(OperationalError, match="Salesforce CLI"):
        auth.get_oauth_token()


@patch("subprocess.run")
def test_sf_cli_auth_org_display_failure(mock_run):
    """A non-zero status from `org display` (e.g. no authenticated org)
    surfaces the CLI's own message and skips the token-fetch call."""
    mock_run.side_effect = [
        _completed_process(
            '{"status": 1, "message": "No authorization information found for my-alias."}'
        ),
    ]

    auth = SfCliAuthenticator(target_org="my-alias")
    with pytest.raises(OperationalError, match="No authorization information found"):
        auth.get_oauth_token()

    assert mock_run.call_count == 1


@patch("subprocess.run")
def test_sf_cli_auth_show_access_token_failure(mock_run):
    """A non-zero status from `org auth show-access-token` surfaces the
    CLI's own message."""
    mock_run.side_effect = [
        _completed_process(_ORG_DISPLAY_OK),
        _completed_process('{"status": 1, "message": "No auth found."}'),
    ]

    auth = SfCliAuthenticator(target_org="my-alias")
    with pytest.raises(OperationalError, match="No auth found"):
        auth.get_oauth_token()


@patch("subprocess.run")
def test_sf_cli_auth_malformed_json(mock_run):
    """Non-JSON stdout raises OperationalError instead of propagating the
    raw JSONDecodeError."""
    mock_run.side_effect = [_completed_process("not json")]

    auth = SfCliAuthenticator()
    with pytest.raises(OperationalError):
        auth.get_oauth_token()


@patch("subprocess.run")
def test_sf_cli_auth_timeout(mock_run):
    """A CLI call that hangs past the timeout raises OperationalError."""
    mock_run.side_effect = subprocess.TimeoutExpired(cmd="sf", timeout=30)

    auth = SfCliAuthenticator()
    with pytest.raises(OperationalError):
        auth.get_oauth_token()


@patch("subprocess.run")
def test_sf_cli_auth_fetches_fresh_each_call(mock_run):
    """Matches the no-cache model: each get_oauth_token() call re-invokes the
    CLI rather than reusing a previous result."""
    mock_run.side_effect = [
        _completed_process(_ORG_DISPLAY_OK),
        _completed_process('{"status": 0, "result": {"accessToken": "token1"}}'),
        _completed_process(_ORG_DISPLAY_OK),
        _completed_process('{"status": 0, "result": {"accessToken": "token2"}}'),
    ]

    auth = SfCliAuthenticator()
    assert auth.get_oauth_token() == "token1"
    assert auth.get_oauth_token() == "token2"
    assert mock_run.call_count == 4


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
