"""
Data Cloud token exchanger (core token → CDP token).

Wraps any OAuthAuthenticator and exchanges Salesforce core tokens for
Data Cloud (CDP) tokens via the /services/a360/token endpoint.
"""

from __future__ import annotations

import time
from typing import Optional

import requests

from ..exceptions import OperationalError
from .oauth import OAuthAuthenticator


class DataCloudTokenExchanger:
    """
    Exchanges Salesforce core tokens for Data Cloud CDP tokens.

    Wraps any OAuthAuthenticator instance and handles the token exchange flow:
    1. Fetch core token from authenticator
    2. POST to /services/a360/token with core token
    3. Cache CDP token with 60s expiry buffer
    4. Revoke core token for security

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
        self._cached_cdp_token: Optional[str] = None
        self._token_expiry: Optional[float] = None
        self._tenant_endpoint: Optional[str] = None

    def get_cdp_token(self) -> str:
        """
        Get a valid CDP token, using cache or exchanging a new one if needed.

        Uses the same 60s-buffer caching pattern as OAuthAuthenticator:
        - If cached token exists and current_time < (expiry - 60), return cached token
        - Otherwise, fetch a fresh core token and exchange it for a CDP token

        Returns:
            Valid CDP access token

        Raises:
            OperationalError: If token exchange fails
        """
        current_time = time.time()

        # Check cache with 60s buffer
        if (
            self._cached_cdp_token is not None
            and self._token_expiry is not None
            and current_time < (self._token_expiry - 60)
        ):
            return self._cached_cdp_token

        # Exchange core token for CDP token
        core_token = self._core_authenticator.get_oauth_token()
        instance_url = self._core_authenticator.get_instance_url()

        cdp_token, expires_in, tenant_endpoint = self._exchange_token(
            instance_url, core_token
        )

        # Cache CDP token
        self._cached_cdp_token = cdp_token
        self._token_expiry = current_time + expires_in
        self._tenant_endpoint = tenant_endpoint

        # Revoke core token for security (match v1 behavior)
        self._revoke_core_token(instance_url, core_token)

        return cdp_token

    def get_tenant_endpoint(self) -> str:
        """
        Get the Data Cloud tenant endpoint URL.

        The tenant endpoint is returned by the CDP token exchange and is used
        as the base URL for all off-core v3 API calls.

        Returns:
            Data Cloud tenant endpoint (e.g., "https://{tenant}.c360a.salesforce.com")

        Raises:
            OperationalError: If no token exchange has occurred yet
        """
        if self._tenant_endpoint is None:
            # Trigger exchange to get tenant endpoint
            self.get_cdp_token()

        if self._tenant_endpoint is None:
            raise OperationalError("Tenant endpoint not available from CDP token exchange")

        return self._tenant_endpoint

    def invalidate_token(self):
        """
        Invalidate the cached CDP token, forcing a re-exchange on next request.

        Note: This does NOT invalidate the core authenticator's token.
        """
        self._cached_cdp_token = None
        self._token_expiry = None

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

    def _revoke_core_token(self, instance_url: str, core_token: str):
        """
        Revoke the core token after successful CDP token exchange.

        This matches v1 behavior and improves security by limiting core token lifetime.
        Also invalidates the core authenticator's own cache: that cache's ~2h TTL
        outlives the revoked token, so without this a later re-exchange (the CDP
        token lives ~1h) would pull the same now-revoked token back out of cache
        and fail the exchange with a 401.

        Args:
            instance_url: Salesforce instance URL
            core_token: Core access token to revoke
        """
        revoke_url = f"{instance_url}/services/oauth2/revoke"
        params = {"token": core_token}

        try:
            requests.post(revoke_url, params=params, timeout=10)
            # Revocation failures are non-fatal (token will expire naturally)
        except requests.exceptions.RequestException:
            pass  # Silently ignore revocation failures
        finally:
            self._core_authenticator.invalidate_token()
