"""
Tests for OAuth/token-exchange error bodies, retry/backoff, and the
client's one-shot refresh on 401.
"""

from unittest.mock import Mock, patch

import pytest
import requests
import responses

from salesforce_datacloud_connector.api.client import DataCloudQueryClient
from salesforce_datacloud_connector.auth._http import RetryConfig
from salesforce_datacloud_connector.auth.oauth import (
    ClientCredentialsAuthenticator,
    UsernamePasswordAuthenticator,
)
from salesforce_datacloud_connector.auth.token_exchanger import DataCloudTokenExchanger
from salesforce_datacloud_connector.exceptions import OperationalError

TOKEN_URL = "https://login.salesforce.com/services/oauth2/token"
EXCHANGE_URL = "https://org.my.salesforce.com/services/a360/token"
OK_BODY = {
    "access_token": "tok",
    "instance_url": "https://org.my.salesforce.com",
    "expires_in": 7200,
}


@pytest.fixture
def sleeps():
    with patch("salesforce_datacloud_connector.auth._http.time.sleep") as sleep:
        yield sleep


def _password_auth(**kwargs):
    return UsernamePasswordAuthenticator(
        username="u", password="p", client_id="id", client_secret="s", **kwargs
    )


@responses.activate
def test_oauth_error_body_is_surfaced(sleeps):
    responses.post(
        TOKEN_URL,
        status=400,
        json={"error": "invalid_grant", "error_description": "authentication failure"},
    )
    with pytest.raises(OperationalError) as exc:
        _password_auth().get_oauth_token()
    assert "invalid_grant: authentication failure" in str(exc.value)
    assert exc.value.http_status == 400
    assert len(responses.calls) == 1  # 400 is never retried
    sleeps.assert_not_called()


@responses.activate
def test_non_json_error_body_falls_back_to_text(sleeps):
    responses.post(TOKEN_URL, status=403, body="Forbidden by proxy")
    with pytest.raises(OperationalError, match="HTTP 403 - Forbidden by proxy"):
        _password_auth().get_oauth_token()
    assert len(responses.calls) == 1


@pytest.mark.parametrize("status", [401, 403])
@responses.activate
def test_auth_failures_not_retried(sleeps, status):
    responses.post(TOKEN_URL, status=status, json={"error": "denied"})
    with pytest.raises(OperationalError):
        _password_auth().get_oauth_token()
    assert len(responses.calls) == 1


@pytest.mark.parametrize("status", [429, 500, 503])
@responses.activate
def test_transient_status_retried_with_exponential_backoff(sleeps, status):
    responses.post(TOKEN_URL, status=status)
    responses.post(TOKEN_URL, status=status)
    responses.post(TOKEN_URL, json=OK_BODY)
    token = _password_auth().get_oauth_token()
    assert token == "tok"
    assert len(responses.calls) == 3
    assert [c.args[0] for c in sleeps.call_args_list] == [1.0, 2.0]


@responses.activate
def test_retries_exhausted_raises_last_error(sleeps):
    responses.post(TOKEN_URL, status=503, json={"error": "unavailable"})
    with pytest.raises(OperationalError, match="HTTP 503 - unavailable"):
        _password_auth().get_oauth_token()
    assert len(responses.calls) == 4  # 1 attempt + 3 retries
    assert [c.args[0] for c in sleeps.call_args_list] == [1.0, 2.0, 4.0]


@responses.activate
def test_backoff_is_capped(sleeps):
    responses.post(TOKEN_URL, status=500)
    retry = RetryConfig(max_retries=4, initial_backoff_seconds=10, max_backoff_seconds=30)
    with pytest.raises(OperationalError):
        _password_auth(retry=retry).get_oauth_token()
    assert [c.args[0] for c in sleeps.call_args_list] == [10, 20, 30, 30]


@responses.activate
def test_connection_errors_retried(sleeps):
    responses.post(TOKEN_URL, body=requests.exceptions.ConnectionError("boom"))
    responses.post(TOKEN_URL, json=OK_BODY)
    assert _password_auth().get_oauth_token() == "tok"
    assert len(responses.calls) == 2


@responses.activate
def test_retry_can_be_disabled(sleeps):
    responses.post(TOKEN_URL, status=500)
    with pytest.raises(OperationalError):
        ClientCredentialsAuthenticator(
            client_id="id", client_secret="s", retry=RetryConfig(max_retries=0)
        ).get_oauth_token()
    assert len(responses.calls) == 1
    sleeps.assert_not_called()


def test_retry_config_validation():
    with pytest.raises(ValueError):
        RetryConfig(max_retries=-1)
    with pytest.raises(ValueError):
        RetryConfig(initial_backoff_seconds=-1)


@responses.activate
def test_exchanger_retries_transient_failures(sleeps):
    auth = Mock()
    auth.get_oauth_token.return_value = "core"
    auth.get_instance_url.return_value = "https://org.my.salesforce.com"

    responses.post(EXCHANGE_URL, status=502)
    responses.post(
        EXCHANGE_URL,
        json={
            "access_token": "cdp",
            "expires_in": 3600,
            "instance_url": "tenant.c360a.salesforce.com",
        },
    )
    exchanger = DataCloudTokenExchanger(auth)
    assert exchanger.get_cdp_token() == "cdp"
    assert exchanger.get_tenant_endpoint() == "https://tenant.c360a.salesforce.com"
    assert len(responses.calls) == 2


@responses.activate
def test_exchanger_surfaces_error_body(sleeps):
    auth = Mock()
    auth.get_oauth_token.return_value = "core"
    auth.get_instance_url.return_value = "https://org.my.salesforce.com"
    responses.post(
        EXCHANGE_URL,
        status=400,
        json={"error": "invalid_request", "error_description": "bad subject_token"},
    )
    with pytest.raises(OperationalError, match="invalid_request: bad subject_token"):
        DataCloudTokenExchanger(auth).get_cdp_token()
    assert len(responses.calls) == 1


# --- DataCloudQueryClient 401 refresh -------------------------------------

QUERY_URL = "https://tenant.c360a.salesforce.com/api/v3/query"


def _client(on_unauthorized, tokens):
    return DataCloudQueryClient(
        tenant_endpoint="https://tenant.c360a.salesforce.com",
        auth_token_getter=Mock(side_effect=tokens),
        on_unauthorized=on_unauthorized,
    )


@responses.activate
def test_401_invalidates_token_and_retries_once():
    responses.get(QUERY_URL, status=401, json={"message": "expired"})
    responses.get(QUERY_URL, json={"ok": True})
    on_unauthorized = Mock()
    client = _client(on_unauthorized, ["old", "new"])

    response = client._make_request("GET", QUERY_URL)

    assert response.json() == {"ok": True}
    on_unauthorized.assert_called_once()
    assert responses.calls[0].request.headers["Authorization"] == "Bearer old"
    assert responses.calls[1].request.headers["Authorization"] == "Bearer new"


@responses.activate
def test_second_401_is_raised_without_looping():
    responses.get(QUERY_URL, status=401, json={"message": "still bad"})
    on_unauthorized = Mock()
    client = _client(on_unauthorized, ["a", "b", "c"])

    with pytest.raises(OperationalError, match="still bad"):
        client._make_request("GET", QUERY_URL)

    on_unauthorized.assert_called_once()
    assert len(responses.calls) == 2


@responses.activate
def test_401_without_callback_raises_immediately():
    responses.get(QUERY_URL, status=401, json={"message": "expired"})
    client = _client(None, ["a"])
    with pytest.raises(OperationalError):
        client._make_request("GET", QUERY_URL)
    assert len(responses.calls) == 1
