"""Tests for JWT claims/key loading and shared HTTP options."""

import time

import jwt
import pytest
import requests
import responses
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

import salesforce_datacloud_connector as sdc
from salesforce_datacloud_connector.auth.oauth import JWTAuthenticator
from salesforce_datacloud_connector.http_options import HttpOptions
from tests._keys import TEST_RSA_PRIVATE_KEY_PEM, TEST_RSA_PUBLIC_KEY


def _jwt_auth(**kwargs):
    defaults = dict(
        login_url="https://login.salesforce.com",
        client_id="cid",
        username="u@example.com",
        jwt_private_key=TEST_RSA_PRIVATE_KEY_PEM,
    )
    defaults.update(kwargs)
    return JWTAuthenticator(**defaults)


def _decode(token, audience):
    return jwt.decode(
        token, TEST_RSA_PUBLIC_KEY, algorithms=["RS256"], audience=audience
    )


# --- JWT claims ------------------------------------------------------------


def test_jwt_claims_aud_iat_exp():
    before = int(time.time())
    claims = _decode(_jwt_auth()._create_jwt(), "https://login.salesforce.com")
    assert claims["iss"] == "cid"
    assert claims["sub"] == "u@example.com"
    assert claims["aud"] == "https://login.salesforce.com"
    assert before <= claims["iat"] <= int(time.time())
    assert claims["exp"] - claims["iat"] == 120


def test_jwt_audience_is_scheme_and_host_only():
    auth = _jwt_auth(login_url="https://myorg.my.salesforce.com/some/path/")
    claims = _decode(auth._create_jwt(), "https://myorg.my.salesforce.com")
    assert claims["aud"] == "https://myorg.my.salesforce.com"


def test_jwt_expiry_is_configurable():
    claims = _decode(
        _jwt_auth(jwt_expiry_seconds=60)._create_jwt(), "https://login.salesforce.com"
    )
    assert claims["exp"] - claims["iat"] == 60


# --- eager key validation and key sources ----------------------------------


def test_invalid_key_rejected_at_construction():
    with pytest.raises(ValueError, match="neither PEM text nor a readable file"):
        _jwt_auth(jwt_private_key="definitely-not-a-key")
    with pytest.raises(ValueError, match="Failed to parse"):
        _jwt_auth(jwt_private_key="-----BEGIN PRIVATE KEY-----\nbogus\n-----END PRIVATE KEY-----")


def test_non_rsa_key_rejected():
    ec_pem = (
        ec.generate_private_key(ec.SECP256R1())
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        .decode()
    )
    with pytest.raises(ValueError, match="must be an RSA"):
        _jwt_auth(jwt_private_key=ec_pem)


def test_key_accepts_bytes_and_paths(tmp_path):
    key_file = tmp_path / "server.key"
    key_file.write_text(TEST_RSA_PRIVATE_KEY_PEM)

    for source in (
        TEST_RSA_PRIVATE_KEY_PEM.encode(),
        key_file,
        str(key_file),
    ):
        claims = _decode(
            _jwt_auth(jwt_private_key=source)._create_jwt(),
            "https://login.salesforce.com",
        )
        assert claims["iss"] == "cid"


def test_connect_validates_key_before_network():
    with pytest.raises(ValueError, match="jwt_private_key"):
        sdc.connect(
            auth_type="jwt",
            username="u",
            client_id="cid",
            jwt_private_key="garbage",
        )


def test_connect_rejects_client_secret_with_jwt():
    with pytest.raises(ValueError, match="client_secret is not allowed for jwt"):
        sdc.connect(
            auth_type="jwt",
            username="u",
            client_id="cid",
            client_secret="s",
            jwt_private_key=TEST_RSA_PRIVATE_KEY_PEM,
        )


# --- HttpOptions -----------------------------------------------------------


def test_http_options_builds_session_from_verify_and_proxies():
    http = HttpOptions(
        timeout=5, verify="/etc/ca.pem", proxies={"https": "http://proxy:3128"}
    )
    assert http.timeout == 5
    assert http.session.verify == "/etc/ca.pem"
    assert http.session.proxies["https"] == "http://proxy:3128"


def test_http_options_validation():
    with pytest.raises(ValueError, match="timeout"):
        HttpOptions(timeout=0)
    with pytest.raises(ValueError, match="not both"):
        HttpOptions(session=requests.Session(), verify=False)


@responses.activate
def test_connect_shares_one_http_session_across_auth_and_query():
    responses.post(
        "https://login.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core",
            "instance_url": "https://org.my.salesforce.com",
            "expires_in": 7200,
        },
    )
    responses.post(
        "https://org.my.salesforce.com/services/a360/token",
        json={
            "access_token": "cdp",
            "expires_in": 3600,
            "instance_url": "tenant.c360a.salesforce.com",
        },
    )

    conn = sdc.connect(
        auth_type="client_credentials", client_id="cid", client_secret="s"
    )

    exchanger = conn._token_provider
    http = conn._client._http
    assert exchanger._http is http
    assert exchanger._core_authenticator.http is http
    assert http.timeout == HttpOptions.DEFAULT_TIMEOUT_SECONDS


def test_connect_does_not_expose_http_or_retry_knobs():
    for name in ("auth_retry", "timeout", "verify", "proxies", "session"):
        with pytest.raises(TypeError):
            sdc.connect(auth_type="sf_cli", **{name: object()})
