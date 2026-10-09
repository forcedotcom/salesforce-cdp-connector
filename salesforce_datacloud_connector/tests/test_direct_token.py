"""Tests for cdp_token (pre-minted Data Cloud token) authentication."""

import base64
import json
import time

import pytest

import salesforce_datacloud_connector as sdc
from salesforce_datacloud_connector.auth.direct_token import DirectCdpTokenProvider
from salesforce_datacloud_connector.exceptions import OperationalError


def make_jwt(payload):
    def b64(data):
        return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()

    return f"{b64({'alg': 'none'})}.{b64(payload)}.sig"


def test_valid_token_and_endpoint_normalization():
    token = make_jwt({"exp": time.time() + 3600})
    provider = DirectCdpTokenProvider(token, "tenant.c360a.salesforce.com")
    assert provider.get_cdp_token() == token
    assert provider.get_tenant_endpoint() == "https://tenant.c360a.salesforce.com"
    provider.invalidate_token()  # no-op
    assert provider.get_cdp_token() == token


def test_endpoint_with_scheme_is_kept():
    token = make_jwt({"exp": time.time() + 3600})
    provider = DirectCdpTokenProvider(token, "https://tenant.c360a.salesforce.com")
    assert provider.get_tenant_endpoint() == "https://tenant.c360a.salesforce.com"


def test_expired_token_fails_fast():
    with pytest.raises(OperationalError, match="already expired"):
        DirectCdpTokenProvider(make_jwt({"exp": time.time() - 10}), "t.example.com")


@pytest.mark.parametrize(
    "token, message",
    [
        ("not-a-jwt", "valid JWT"),
        ("a.!!!notbase64.c", "Failed to parse"),
        (make_jwt({"sub": "x"}), "missing a numeric 'exp'"),
        (make_jwt({"exp": "soon"}), "missing a numeric 'exp'"),
    ],
)
def test_malformed_token_rejected(token, message):
    with pytest.raises(OperationalError, match=message):
        DirectCdpTokenProvider(token, "t.example.com")


def test_token_expiring_later_cannot_be_refreshed(monkeypatch):
    provider = DirectCdpTokenProvider(
        make_jwt({"exp": time.time() + 5}), "t.example.com"
    )
    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + 60)
    with pytest.raises(OperationalError, match="cannot be refreshed"):
        provider.get_cdp_token()


def test_connect_requires_token_and_endpoint():
    with pytest.raises(ValueError, match="cdp_token, tenant_endpoint"):
        sdc.connect(auth_type="cdp_token", cdp_token="x")
    with pytest.raises(ValueError, match="cdp_token, tenant_endpoint"):
        sdc.connect(auth_type="cdp_token", tenant_endpoint="t.example.com")


def test_connect_builds_connection_without_network():
    token = make_jwt({"exp": time.time() + 3600})
    conn = sdc.connect(
        auth_type="cdp_token",
        cdp_token=token,
        tenant_endpoint="tenant.c360a.salesforce.com",
        dataspace="ds1",
    )
    assert conn.dataspace == "ds1"
    assert conn._client.tenant_endpoint == "https://tenant.c360a.salesforce.com"
    assert conn._client.auth_token_getter() == token
