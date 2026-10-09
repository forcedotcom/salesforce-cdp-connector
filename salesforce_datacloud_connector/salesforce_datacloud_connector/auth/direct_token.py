"""
Direct Data Cloud (CDP) token provider.

For callers that already hold a CDP access token and the tenant endpoint (e.g.
minted by another service, or handed to a notebook/Spark job). Skips the core
OAuth login and the /services/a360/token exchange entirely, matching the JDBC
driver's DirectCdpTokenProcessor (``cdpToken`` + ``tenantUrl`` properties).

The token cannot be refreshed: once it expires, queries fail and the caller
must create a new connection with a fresh token.
"""

from __future__ import annotations

import base64
import json
import time

from ..exceptions import OperationalError
from .token_exchanger import DataCloudTokenExchanger


def _jwt_expiry(token: str) -> float:
    """Return the ``exp`` claim (epoch seconds) of a JWT without verifying it."""
    segments = token.split(".")
    if len(segments) < 2:
        raise OperationalError("CDP token is not a valid JWT (expected at least 2 segments)")
    payload = segments[1]
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    except (ValueError, UnicodeDecodeError) as e:
        raise OperationalError(f"Failed to parse CDP token JWT: {e}") from e

    exp = claims.get("exp") if isinstance(claims, dict) else None
    if isinstance(exp, bool) or not isinstance(exp, (int, float)):
        raise OperationalError("CDP token JWT is missing a numeric 'exp' claim")
    return float(exp)


class DirectCdpTokenProvider:
    """
    Token provider for a pre-minted CDP token.

    Implements the same interface the Connection uses from
    DataCloudTokenExchanger (get_cdp_token, get_tenant_endpoint,
    invalidate_token).
    """

    def __init__(self, cdp_token: str, tenant_endpoint: str):
        """
        Args:
            cdp_token: Data Cloud access token (a JWT carrying an ``exp`` claim)
            tenant_endpoint: Data Cloud tenant endpoint, with or without scheme
                (e.g. "tenant.c360a.salesforce.com")

        Raises:
            OperationalError: If the token is not a JWT with an ``exp`` claim,
                or has already expired
        """
        self._token = cdp_token
        self._expiry = _jwt_expiry(cdp_token)
        if self._expiry <= time.time():
            raise OperationalError("CDP token has already expired")
        self._tenant_endpoint = DataCloudTokenExchanger._normalize_endpoint(
            tenant_endpoint.strip()
        )

    def get_cdp_token(self) -> str:
        """Return the token, or raise if it has expired (it cannot be refreshed)."""
        if time.time() > self._expiry:
            raise OperationalError(
                "CDP token has expired and cannot be refreshed; "
                "create a new connection with a fresh token"
            )
        return self._token

    def get_tenant_endpoint(self) -> str:
        """Return the tenant endpoint (always with a scheme)."""
        return self._tenant_endpoint

    def invalidate_token(self):
        """No-op: a direct token has no source to re-exchange from."""
