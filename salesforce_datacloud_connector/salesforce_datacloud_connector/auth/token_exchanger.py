"""
Data Cloud token exchanger (core token → CDP token).

Wraps any OAuthAuthenticator and exchanges Salesforce core tokens for
Data Cloud (CDP) tokens via the /services/a360/token endpoint.
"""

from __future__ import annotations

import threading
import time
from collections import namedtuple
from typing import Optional

import requests

from ..exceptions import OperationalError
from .oauth import OAuthAuthenticator

# Immutable snapshot of the three values a single CDP exchange produces. Bundling
# them into one namedtuple lets us publish them with a single atomic reference
# assignment (self._cache = _CdpTokenCache(...)), so a reader can never observe a
# token from one exchange paired with a tenant endpoint from another (torn read).
_CdpTokenCache = namedtuple("_CdpTokenCache", ["token", "expiry", "tenant_endpoint"])


class DataCloudTokenExchanger:
    """
    Exchanges Salesforce core tokens for Data Cloud CDP tokens.

    Wraps any OAuthAuthenticator instance and handles the token exchange flow:
    1. Fetch a fresh core token from authenticator (no core-token caching)
    2. POST to /services/a360/token with core token
    3. Cache CDP token until it expires (no refresh buffer, matches JDBC's
       DataCloudToken.isAlive())

    The CDP token exchange returns the Data Cloud tenant endpoint, which is
    different from the Salesforce instance URL.
    """

    # Token exchange grant type (from v1 precedent)
    CDP_GRANT_TYPE = "urn:salesforce:grant-type:external:cdp"
    CDP_SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"

    def __init__(
        self,
        core_authenticator: OAuthAuthenticator,
        dataspace: Optional[str] = None,
    ):
        """
        Initialize the CDP token exchanger.

        Args:
            core_authenticator: An OAuthAuthenticator instance that provides core tokens
            dataspace: Data space name to pass to the exchange endpoint (optional)
        """
        self._core_authenticator = core_authenticator
        self._dataspace = dataspace
        # Single atomic reference to the current cache snapshot (None = cold).
        # Publishing a fresh _CdpTokenCache in one assignment keeps token, expiry
        # and tenant_endpoint mutually consistent for every reader.
        self._cache: Optional[_CdpTokenCache] = None
        # Guards the cold-miss / expiry re-exchange so a shared exchanger is safe
        # across threads (threadsafety=2): N racing misses do ONE exchange, not a
        # thundering herd.
        self._lock = threading.Lock()

    def _ensure_cache(self) -> _CdpTokenCache:
        """
        Return a live CDP cache snapshot, exchanging a new one if needed.

        Uses double-checked locking: the common hit takes the unlocked fast path;
        only an apparent miss acquires the lock, then re-checks so a single thread
        performs the exchange while the others reuse its freshly published snapshot
        (rather than each firing their own — the pre-lock thundering herd).

        Liveness mirrors JDBC's DataCloudToken.isAlive(): a snapshot is alive while
        current_time <= expiry, with no refresh buffer. The (potentially slow) core
        fetch + exchange run under the lock so concurrent cold callers dedup to one
        network round-trip.

        Returns:
            A live _CdpTokenCache snapshot

        Raises:
            OperationalError: If token exchange fails
        """
        # Fast path: a live snapshot needs no lock.
        snapshot = self._cache
        if snapshot is not None and time.time() <= snapshot.expiry:
            return snapshot

        with self._lock:
            # Re-check under the lock — another thread may have exchanged while we
            # waited, in which case we reuse its snapshot instead of exchanging.
            snapshot = self._cache
            if snapshot is not None and time.time() <= snapshot.expiry:
                return snapshot

            # Fetch a fresh core token paired with its instance URL from ONE fetch
            # (the core authenticator never caches), then exchange for a CDP token.
            core_token, instance_url = (
                self._core_authenticator.get_oauth_token_and_instance_url()
            )
            cdp_token, expires_in, tenant_endpoint = self._exchange_token(
                instance_url, core_token
            )

            # Publish the whole snapshot atomically in one reference assignment.
            snapshot = _CdpTokenCache(
                token=cdp_token,
                expiry=time.time() + expires_in,
                tenant_endpoint=tenant_endpoint,
            )
            self._cache = snapshot
            return snapshot

    def get_cdp_token(self) -> str:
        """
        Get a valid CDP token, using cache or exchanging a new one if needed.

        Mirrors JDBC's DataCloudToken.isAlive(): the cached token is used while
        current_time <= expiry, with no refresh buffer. On a miss, a fresh core
        token is fetched (the core authenticator never caches) and exchanged.

        Returns:
            Valid CDP access token

        Raises:
            OperationalError: If token exchange fails
        """
        return self._ensure_cache().token

    def get_tenant_endpoint(self) -> str:
        """
        Get the Data Cloud tenant endpoint URL.

        The tenant endpoint is returned by the CDP token exchange and is used
        as the base URL for all off-core v3 API calls. It is drawn from the SAME
        snapshot as the CDP token, so the two can never come from different
        exchanges.

        Returns:
            Data Cloud tenant endpoint (e.g., "https://{tenant}.c360a.salesforce.com")

        Raises:
            OperationalError: If no token exchange has occurred yet
        """
        tenant_endpoint = self._ensure_cache().tenant_endpoint
        if tenant_endpoint is None:
            raise OperationalError("Tenant endpoint not available from CDP token exchange")
        return tenant_endpoint

    def invalidate_token(self):
        """
        Invalidate the cached CDP token, forcing a re-exchange on next request.

        Clears the whole snapshot atomically (token, expiry AND tenant_endpoint),
        so a subsequent get_tenant_endpoint() re-exchanges rather than returning a
        stale endpoint.

        Note: This does NOT invalidate the core authenticator's token.
        """
        with self._lock:
            self._cache = None

    def _exchange_token(
        self, instance_url: str, core_token: str
    ) -> tuple[str, int, str]:
        """
        Exchange core token for CDP token via /services/a360/token.

        Args:
            instance_url: Salesforce instance URL (from core OAuth response)
            core_token: Core access token

        Returns:
            Tuple of (cdp_token, expires_in_seconds, tenant_endpoint)

        Raises:
            OperationalError: If exchange fails
        """
        exchange_url = f"{instance_url}/services/a360/token"

        params = {
            "grant_type": self.CDP_GRANT_TYPE,
            "subject_token_type": self.CDP_SUBJECT_TOKEN_TYPE,
            "subject_token": core_token,
        }

        # Add dataspace if configured
        if self._dataspace:
            params["dataspace"] = self._dataspace

        try:
            response = requests.post(exchange_url, params=params, timeout=30)
            response.raise_for_status()
            token_data = response.json()

            access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in")
            tenant_endpoint = token_data.get("instance_url")

            if not access_token:
                raise OperationalError("No access_token in CDP token exchange response")

            if not expires_in:
                raise OperationalError("No expires_in in CDP token exchange response")

            if not tenant_endpoint:
                raise OperationalError("No instance_url in CDP token exchange response")

            # The a360 exchange returns the tenant endpoint as a bare host
            # (no scheme). On-core v1 prepends https:// at request time; we
            # normalize once here so the off-core client can build valid URLs.
            tenant_endpoint = self._normalize_endpoint(tenant_endpoint)

            return access_token, expires_in, tenant_endpoint

        except requests.exceptions.RequestException as e:
            raise OperationalError(f"CDP token exchange failed: {e}") from e

    @staticmethod
    def _normalize_endpoint(endpoint: str) -> str:
        """
        Ensure the tenant endpoint carries an explicit scheme.

        The /services/a360/token response returns instance_url as a bare host
        (e.g. "tenant.c360a.salesforce.com"). The off-core query client builds
        request URLs by string concatenation, so a missing scheme makes requests
        raise "No scheme supplied". Prepend https:// when absent; leave an
        existing http:// or https:// untouched (idempotent).

        Args:
            endpoint: Tenant endpoint as returned by the exchange (with or without scheme)

        Returns:
            Endpoint guaranteed to start with a scheme
        """
        if endpoint.startswith(("https://", "http://")):
            return endpoint
        return f"https://{endpoint}"
