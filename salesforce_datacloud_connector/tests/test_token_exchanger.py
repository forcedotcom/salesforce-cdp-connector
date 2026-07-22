"""
Tests for DataCloudTokenExchanger (core token → CDP token exchange).
"""

import time
from unittest.mock import Mock, patch

import pytest
import responses

from salesforce_datacloud_connector.auth.oauth import JWTAuthenticator
from salesforce_datacloud_connector.auth.token_exchanger import DataCloudTokenExchanger
from salesforce_datacloud_connector.exceptions import OperationalError


@responses.activate
def test_jwt_token_exchange_success():
    """Test successful core token → CDP token exchange using JWT authenticator."""
    # Mock core token fetch (JWT flow)
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core_token_12345",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={
            "access_token": "cdp_token_67890",
            "expires_in": 3600,
            "instance_url": "https://tenant123.c360a.salesforce.com",
        },
        status=200,
    )

    # Mock core token revocation
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/oauth2/revoke",
        status=200,
    )

    # Create JWT authenticator
    with patch("jwt.encode", return_value="mock_jwt"):
        jwt_auth = JWTAuthenticator(
            login_url="https://test.salesforce.com",
            client_id="test_client_id",
            username="test@example.com",
            jwt_private_key="-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
        )

        # Create exchanger
        exchanger = DataCloudTokenExchanger(
            core_authenticator=jwt_auth,
            dataspace="default",
        )

        # Get CDP token
        cdp_token = exchanger.get_cdp_token()
        assert cdp_token == "cdp_token_67890"

        # Verify tenant endpoint
        tenant_endpoint = exchanger.get_tenant_endpoint()
        assert tenant_endpoint == "https://tenant123.c360a.salesforce.com"

        # Verify request sequence: JWT auth → exchange → revoke
        assert len(responses.calls) == 3
        assert "/services/oauth2/token" in responses.calls[0].request.url  # Core token
        assert "/services/a360/token" in responses.calls[1].request.url  # Exchange
        assert "/services/oauth2/revoke" in responses.calls[2].request.url  # Revoke


@responses.activate
def test_cdp_token_caching_with_60s_buffer():
    """Test that CDP tokens are cached and reused with 60s buffer before expiry."""
    # Mock core token fetch
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core_token_1",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={
            "access_token": "cdp_token_1",
            "expires_in": 150,  # 150 seconds
            "instance_url": "https://tenant123.c360a.salesforce.com",
        },
        status=200,
    )

    # Mock core token revocation
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/oauth2/revoke",
        status=200,
    )

    with patch("jwt.encode", return_value="mock_jwt"):
        jwt_auth = JWTAuthenticator(
            login_url="https://test.salesforce.com",
            client_id="test_client_id",
            username="test@example.com",
            jwt_private_key="-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
        )

        exchanger = DataCloudTokenExchanger(
            core_authenticator=jwt_auth,
            dataspace="default",
        )

        # First call - should exchange
        cdp_token_1 = exchanger.get_cdp_token()
        assert cdp_token_1 == "cdp_token_1"
        first_call_count = len(responses.calls)

        # Second call immediately - should use cache
        cdp_token_2 = exchanger.get_cdp_token()
        assert cdp_token_2 == "cdp_token_1"
        assert len(responses.calls) == first_call_count  # No additional calls

        # Simulate time passing (100s: past the 60s buffer threshold of 90s = expiry 150s - 60s)
        with patch("time.time", return_value=time.time() + 100):
            # Should trigger re-exchange because current_time >= (expiry - 60)
            # Add mock for second exchange
            responses.add(
                responses.POST,
                "https://test.salesforce.com/services/oauth2/token",
                json={
                    "access_token": "core_token_2",
                    "expires_in": 7200,
                    "instance_url": "https://myorg.my.salesforce.com",
                },
                status=200,
            )
            responses.add(
                responses.POST,
                "https://myorg.my.salesforce.com/services/a360/token",
                json={
                    "access_token": "cdp_token_2",
                    "expires_in": 3600,
                    "instance_url": "https://tenant123.c360a.salesforce.com",
                },
                status=200,
            )
            responses.add(
                responses.POST,
                "https://myorg.my.salesforce.com/services/oauth2/revoke",
                status=200,
            )

            cdp_token_3 = exchanger.get_cdp_token()
            assert cdp_token_3 == "cdp_token_2"  # New token
            assert len(responses.calls) > first_call_count  # Additional calls made


@responses.activate
def test_cdp_token_invalidation():
    """Test that invalidate_token forces a new exchange on next call."""
    # Mock first exchange
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core_token_1",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={
            "access_token": "cdp_token_1",
            "expires_in": 3600,
            "instance_url": "https://tenant123.c360a.salesforce.com",
        },
        status=200,
    )
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/oauth2/revoke",
        status=200,
    )

    # Mock second exchange
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core_token_2",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={
            "access_token": "cdp_token_2",
            "expires_in": 3600,
            "instance_url": "https://tenant123.c360a.salesforce.com",
        },
        status=200,
    )
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/oauth2/revoke",
        status=200,
    )

    with patch("jwt.encode", return_value="mock_jwt"):
        jwt_auth = JWTAuthenticator(
            login_url="https://test.salesforce.com",
            client_id="test_client_id",
            username="test@example.com",
            jwt_private_key="-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
        )

        exchanger = DataCloudTokenExchanger(
            core_authenticator=jwt_auth,
            dataspace="default",
        )

        # Get first token
        cdp_token_1 = exchanger.get_cdp_token()
        assert cdp_token_1 == "cdp_token_1"
        first_call_count = len(responses.calls)

        # Invalidate token
        exchanger.invalidate_token()

        # Get token again - should trigger a new CDP exchange.
        # The core authenticator's own token is still cached (expires_in=7200, no time
        # advance), so it is reused without a second /oauth2/token call. Only the CDP
        # exchange + core-token revoke fire: exactly 2 additional calls.
        cdp_token_2 = exchanger.get_cdp_token()
        assert cdp_token_2 == "cdp_token_2"
        assert len(responses.calls) == first_call_count + 2  # exchange + revoke


@responses.activate
def test_get_tenant_endpoint_triggers_exchange():
    """Test that get_tenant_endpoint triggers exchange if not cached."""
    # Mock exchange
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core_token",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={
            "access_token": "cdp_token",
            "expires_in": 3600,
            "instance_url": "https://tenant123.c360a.salesforce.com",
        },
        status=200,
    )
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/oauth2/revoke",
        status=200,
    )

    with patch("jwt.encode", return_value="mock_jwt"):
        jwt_auth = JWTAuthenticator(
            login_url="https://test.salesforce.com",
            client_id="test_client_id",
            username="test@example.com",
            jwt_private_key="-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
        )

        exchanger = DataCloudTokenExchanger(
            core_authenticator=jwt_auth,
            dataspace="default",
        )

        # Get tenant endpoint first (before getting token)
        tenant_endpoint = exchanger.get_tenant_endpoint()
        assert tenant_endpoint == "https://tenant123.c360a.salesforce.com"

        # Verify token was also cached
        cdp_token = exchanger.get_cdp_token()
        assert cdp_token == "cdp_token"
        assert len(responses.calls) == 3  # Only one exchange happened


@responses.activate
def test_cdp_token_exchange_failure():
    """Test that exchange failure raises OperationalError."""
    # Mock core token fetch success
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core_token",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    # Mock CDP token exchange failure (401 Unauthorized)
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={"error": "invalid_token", "error_description": "Token is invalid"},
        status=401,
    )

    with patch("jwt.encode", return_value="mock_jwt"):
        jwt_auth = JWTAuthenticator(
            login_url="https://test.salesforce.com",
            client_id="test_client_id",
            username="test@example.com",
            jwt_private_key="-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
        )

        exchanger = DataCloudTokenExchanger(
            core_authenticator=jwt_auth,
            dataspace="default",
        )

        # Attempt to get CDP token - should raise OperationalError
        with pytest.raises(OperationalError, match="CDP token exchange failed"):
            exchanger.get_cdp_token()


@responses.activate
def test_missing_access_token_in_exchange_response():
    """Test that missing access_token in response raises OperationalError."""
    # Mock core token fetch
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core_token",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    # Mock CDP token exchange with missing access_token
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={
            "expires_in": 3600,
            "instance_url": "https://tenant123.c360a.salesforce.com",
            # Missing access_token
        },
        status=200,
    )

    with patch("jwt.encode", return_value="mock_jwt"):
        jwt_auth = JWTAuthenticator(
            login_url="https://test.salesforce.com",
            client_id="test_client_id",
            username="test@example.com",
            jwt_private_key="-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
        )

        exchanger = DataCloudTokenExchanger(
            core_authenticator=jwt_auth,
            dataspace="default",
        )

        with pytest.raises(OperationalError, match="No access_token in CDP token exchange response"):
            exchanger.get_cdp_token()


@responses.activate
def test_dataspace_included_in_exchange_request():
    """Test that dataspace is passed to the CDP token exchange endpoint if set."""
    # Mock core token fetch
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core_token",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={
            "access_token": "cdp_token",
            "expires_in": 3600,
            "instance_url": "https://tenant123.c360a.salesforce.com",
        },
        status=200,
    )
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/oauth2/revoke",
        status=200,
    )

    with patch("jwt.encode", return_value="mock_jwt"):
        jwt_auth = JWTAuthenticator(
            login_url="https://test.salesforce.com",
            client_id="test_client_id",
            username="test@example.com",
            jwt_private_key="-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
        )

        exchanger = DataCloudTokenExchanger(
            core_authenticator=jwt_auth,
            dataspace="my_custom_dataspace",
        )

        cdp_token = exchanger.get_cdp_token()
        assert cdp_token == "cdp_token"

        # Verify dataspace was included in exchange request
        exchange_call = [c for c in responses.calls if "/services/a360/token" in c.request.url][0]
        assert "dataspace=my_custom_dataspace" in exchange_call.request.url


@responses.activate
def test_dataspace_omitted_if_none():
    """Test that dataspace is NOT included in exchange request if None."""
    # Mock core token fetch
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core_token",
            "expires_in": 7200,
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )

    # Mock CDP token exchange
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={
            "access_token": "cdp_token",
            "expires_in": 3600,
            "instance_url": "https://tenant123.c360a.salesforce.com",
        },
        status=200,
    )
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/oauth2/revoke",
        status=200,
    )

    with patch("jwt.encode", return_value="mock_jwt"):
        jwt_auth = JWTAuthenticator(
            login_url="https://test.salesforce.com",
            client_id="test_client_id",
            username="test@example.com",
            jwt_private_key="-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
        )

        exchanger = DataCloudTokenExchanger(
            core_authenticator=jwt_auth,
            dataspace=None,  # No dataspace
        )

        cdp_token = exchanger.get_cdp_token()
        assert cdp_token == "cdp_token"

        # Verify dataspace was NOT included in exchange request
        exchange_call = [c for c in responses.calls if "/services/a360/token" in c.request.url][0]
        assert "dataspace=" not in exchange_call.request.url


def test_import_from_auth_module():
    """Test that DataCloudTokenExchanger can be imported from auth module."""
    from salesforce_datacloud_connector.auth import DataCloudTokenExchanger
    assert DataCloudTokenExchanger is not None
