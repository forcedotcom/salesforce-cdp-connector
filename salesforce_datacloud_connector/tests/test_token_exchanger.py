"""
Tests for DataCloudTokenExchanger (core token → CDP token exchange).
"""

import threading
import time
from unittest.mock import patch

import pytest
import responses

from salesforce_datacloud_connector.auth.oauth import JWTAuthenticator
from salesforce_datacloud_connector.auth.token_exchanger import DataCloudTokenExchanger
from salesforce_datacloud_connector.exceptions import OperationalError
from tests._concurrency_helpers import ConcurrencyGate


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

        # Verify request sequence: JWT auth → exchange (no revoke, matches JDBC)
        assert len(responses.calls) == 2
        assert "/services/oauth2/token" in responses.calls[0].request.url  # Core token
        assert "/services/a360/token" in responses.calls[1].request.url  # Exchange


@responses.activate
def test_cdp_token_caching_with_no_buffer():
    """CDP tokens are cached until exact expiry, with no refresh buffer
    (matches JDBC's DataCloudToken.isAlive(): now <= expiry)."""
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

        base_time = time.time()

        # 100s in: still well within expiry (150s) — no 60s buffer means this
        # must still be a cache hit, unlike the old buffered behavior.
        with patch("time.time", return_value=base_time + 100):
            cdp_token_3 = exchanger.get_cdp_token()
            assert cdp_token_3 == "cdp_token_1"
            assert len(responses.calls) == first_call_count  # Still no additional calls

        # 151s in: past the exact expiry — must re-exchange.
        with patch("time.time", return_value=base_time + 151):
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

            cdp_token_4 = exchanger.get_cdp_token()
            assert cdp_token_4 == "cdp_token_2"  # New token
            assert len(responses.calls) == first_call_count + 2  # core fetch + exchange


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

        # Get token again - should trigger a new CDP exchange: core-token fetch
        # (the core authenticator never caches) + CDP exchange = 2 additional calls.
        cdp_token_2 = exchanger.get_cdp_token()
        assert cdp_token_2 == "cdp_token_2"
        assert len(responses.calls) == first_call_count + 2  # fetch + exchange


@responses.activate
def test_core_token_refetched_on_natural_cdp_expiry():
    """The core authenticator never caches (matches JDBC's getOAuthToken()),
    so every CDP re-exchange — including a natural expiry-driven one — uses a
    freshly fetched core token rather than reusing an old one."""
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core_token_1",
            "expires_in": 7200,  # ~2h
            "instance_url": "https://myorg.my.salesforce.com",
        },
        status=200,
    )
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={
            "access_token": "cdp_token_1",
            "expires_in": 3600,  # ~1h
            "instance_url": "https://tenant123.c360a.salesforce.com",
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

        base_time = time.time()
        cdp_token_1 = exchanger.get_cdp_token()
        assert cdp_token_1 == "cdp_token_1"

        # Past the CDP token's exact expiry (3600s, no buffer).
        with patch("time.time", return_value=base_time + 3601):
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

            cdp_token_2 = exchanger.get_cdp_token()
            assert cdp_token_2 == "cdp_token_2"

            exchange_calls = [c for c in responses.calls if "/services/a360/token" in c.request.url]
            assert len(exchange_calls) == 2
            # The second exchange must carry the freshly fetched core token, not
            # the one used for the first exchange.
            assert "core_token_2" in exchange_calls[1].request.url
            assert "core_token_1" not in exchange_calls[1].request.url


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
        assert len(responses.calls) == 2  # Only one exchange happened


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


def test_normalize_endpoint_prefixes_and_is_idempotent():
    """Unit-level: bare host gets https://, existing scheme is preserved."""
    norm = DataCloudTokenExchanger._normalize_endpoint
    assert norm("tenant.c360a.salesforce.com") == "https://tenant.c360a.salesforce.com"
    assert norm("https://tenant.c360a.salesforce.com") == "https://tenant.c360a.salesforce.com"
    assert norm("http://tenant.c360a.salesforce.com") == "http://tenant.c360a.salesforce.com"


@responses.activate
def test_tenant_endpoint_schemeless_response_gets_https_prefix():
    """Real orgs return the a360 instance_url as a bare host (no scheme). The
    exchanger must normalize it to an https:// URL so the off-core query client
    can build valid request URLs — mirrors on-core v1, which prepends https://
    at request time. Regression for the live-run 'No scheme supplied' failure."""
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
    # a360 returns a SCHEMELESS host, exactly as the live pc-rnd org does.
    responses.add(
        responses.POST,
        "https://myorg.my.salesforce.com/services/a360/token",
        json={
            "access_token": "cdp_token",
            "expires_in": 3600,
            "instance_url": "tenant789.pc-rnd.c360a.salesforce.com",
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
        exchanger = DataCloudTokenExchanger(core_authenticator=jwt_auth, dataspace="default")

        assert exchanger.get_tenant_endpoint() == "https://tenant789.pc-rnd.c360a.salesforce.com"


def test_import_from_auth_module():
    """Test that DataCloudTokenExchanger can be imported from auth module."""
    from salesforce_datacloud_connector.auth import DataCloudTokenExchanger
    assert DataCloudTokenExchanger is not None


@responses.activate
def test_client_credentials_token_exchange_success():
    """Test successful core token → CDP token exchange using client credentials authenticator."""
    # Mock core token fetch (client credentials flow)
    responses.add(
        responses.POST,
        "https://test.salesforce.com/services/oauth2/token",
        json={
            "access_token": "core_token_client_creds",
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
            "access_token": "cdp_token_from_client_creds",
            "expires_in": 3600,
            "instance_url": "https://tenant456.c360a.salesforce.com",
        },
        status=200,
    )

    from salesforce_datacloud_connector.auth.oauth import ClientCredentialsAuthenticator

    # Create client credentials authenticator
    client_creds_auth = ClientCredentialsAuthenticator(
        login_url="https://test.salesforce.com",
        client_id="test_client_id",
        client_secret="test_client_secret",
    )

    # Create exchanger
    exchanger = DataCloudTokenExchanger(
        core_authenticator=client_creds_auth,
        dataspace="default",
    )

    # Get CDP token
    cdp_token = exchanger.get_cdp_token()
    assert cdp_token == "cdp_token_from_client_creds"

    # Verify tenant endpoint
    tenant_endpoint = exchanger.get_tenant_endpoint()
    assert tenant_endpoint == "https://tenant456.c360a.salesforce.com"

    # Verify request sequence: client creds auth → exchange (no revoke, matches JDBC)
    assert len(responses.calls) == 2
    assert "/services/oauth2/token" in responses.calls[0].request.url  # Core token
    assert "grant_type=client_credentials" in responses.calls[0].request.body
    assert "/services/a360/token" in responses.calls[1].request.url  # Exchange


# ---------------------------------------------------------------------------
# Concurrency (threadsafety=2): one exchanger shared across threads.
#
# The CDP token + tenant endpoint are org-scoped, cacheable state. On a cold
# miss, N racing get_cdp_token()/get_tenant_endpoint() callers must dedup to a
# single exchange rather than each firing their own (thundering herd), and the
# token and its tenant endpoint must always come from the SAME exchange so a
# reader never pairs one exchange's token with another's endpoint.
# ---------------------------------------------------------------------------


def _make_cold_exchanger(dataspace="default"):
    """A DataCloudTokenExchanger over a JWT authenticator with no network wired.
    Callers patch _fetch_new_token / _exchange_token to drive it deterministically."""
    with patch("jwt.encode", return_value="mock_jwt"):
        jwt_auth = JWTAuthenticator(
            login_url="https://test.salesforce.com",
            client_id="test_client_id",
            username="test@example.com",
            jwt_private_key="-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
        )
    return DataCloudTokenExchanger(core_authenticator=jwt_auth, dataspace=dataspace)


def test_concurrent_get_cdp_token_exchanges_exactly_once():
    """N threads calling get_cdp_token() on a cold exchanger must trigger exactly
    ONE token exchange. The CDP token is cacheable org-scoped state; today's
    unlocked check-then-exchange admits every caller (thundering herd), so this
    fails until get_cdp_token() single-flights the cold miss under a lock
    (double-checked locking)."""
    exchanger = _make_cold_exchanger()
    parties = 8
    gate = ConcurrencyGate()

    def fake_fetch():
        # Core token fetch is cheap and un-gated; only the exchange is counted.
        return ("core_token", 7200, "https://myorg.my.salesforce.com")

    def fake_exchange(instance_url, core_token):
        gate.enter()  # counts every caller that reaches the exchange
        return ("cdp_token", 3600, "https://tenant123.c360a.salesforce.com")

    def worker(_i):
        return exchanger.get_cdp_token()

    with patch.object(
        exchanger._core_authenticator, "_fetch_new_token", side_effect=fake_fetch
    ), patch.object(exchanger, "_exchange_token", side_effect=fake_exchange):
        results, errors = gate.run(worker, parties)

    assert errors == []
    assert all(r == "cdp_token" for r in results)
    assert gate.entered == 1  # deduplicated: only one thread exchanged
    assert gate.max_concurrent == 1


def test_concurrent_cdp_token_and_tenant_endpoint_derive_from_one_exchange():
    """get_cdp_token() and get_tenant_endpoint() racing on a cold exchanger must
    still perform exactly ONE exchange, and every returned (token, endpoint) must
    come from that same exchange — never a torn pair drawn from two exchanges.

    Each exchange embeds a distinct number in both its token and its endpoint, so
    any cross-exchange pairing (or a second exchange) is caught."""
    exchanger = _make_cold_exchanger()
    parties = 16
    gate = ConcurrencyGate()
    counter = {"n": 0}
    counter_lock = threading.Lock()

    def fake_fetch():
        return ("core_token", 7200, "https://myorg.my.salesforce.com")

    def fake_exchange(instance_url, core_token):
        with counter_lock:
            counter["n"] += 1
            n = counter["n"]
        gate.enter()  # hold callers together so overlapping exchanges are exposed
        return (f"cdp_token{n}", 3600, f"https://tenant{n}.c360a.salesforce.com")

    def worker(i):
        # Half the threads read the token, half read the endpoint — both must be
        # served from the same single exchange.
        if i % 2 == 0:
            return ("token", exchanger.get_cdp_token())
        return ("endpoint", exchanger.get_tenant_endpoint())

    with patch.object(
        exchanger._core_authenticator, "_fetch_new_token", side_effect=fake_fetch
    ), patch.object(exchanger, "_exchange_token", side_effect=fake_exchange):
        results, errors = gate.run(worker, parties)

    assert errors == []
    # Exactly one exchange happened despite the cold-miss stampede.
    assert gate.entered == 1
    assert gate.max_concurrent == 1
    # The single exchange was #1, so every reader sees that exchange's values.
    tokens = {v for kind, v in results if kind == "token"}
    endpoints = {v for kind, v in results if kind == "endpoint"}
    assert tokens == {"cdp_token1"}
    assert endpoints == {"https://tenant1.c360a.salesforce.com"}
