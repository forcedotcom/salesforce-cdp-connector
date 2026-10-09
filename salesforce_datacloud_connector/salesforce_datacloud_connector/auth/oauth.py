"""
OAuth authentication for Salesforce Data Cloud.

This module provides OAuth authenticators for different flows:
- Username/Password (OAuth 2.0 Password Grant)
- JWT Bearer Token (OAuth 2.0 JWT Bearer Flow)
- Refresh Token (OAuth 2.0 Refresh Token Flow)

All authenticators fetch a fresh token on every call (no caching), matching
the JDBC driver's DataCloudTokenProvider.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from abc import ABC, abstractmethod
from typing import Optional, Union
from urllib.parse import urlparse

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from ..exceptions import OperationalError
from ..http_options import HttpOptions
from ._http import RetryConfig, post_with_retry


class OAuthAuthenticator(ABC):
    """
    Abstract base class for OAuth authenticators.

    All authenticators must implement _fetch_new_token(). get_oauth_token()
    always fetches a fresh token — no core-token caching (matches JDBC).
    """

    def __init__(
        self,
        login_url: str = "https://login.salesforce.com",
        retry: Optional[RetryConfig] = None,
        http: Optional[HttpOptions] = None,
    ):
        """
        Initialize the authenticator.

        Args:
            login_url: Salesforce login URL (e.g., "https://login.salesforce.com" or
                      "https://test.salesforce.com" for sandboxes)
            retry: Retry policy for transient (5xx/429/network) failures of the
                   token endpoint. Defaults to 3 retries with 1-30s backoff.
            http: Transport options (session, timeout, TLS verification, proxies)
        """
        self.login_url = login_url.rstrip("/")
        self.retry = retry
        self.http = http or HttpOptions()
        self._instance_url: Optional[str] = None

    def _request_token(
        self, token_url: str, data: dict, description: str
    ) -> tuple[str, int, str]:
        """POST a grant to the OAuth token endpoint and validate the response."""
        token_data = post_with_retry(
            token_url,
            description=description,
            retry=self.retry,
            data=data,
            http=self.http,
        )

        access_token = token_data.get("access_token")
        # Salesforce typically returns tokens valid for 2 hours (7200s)
        expires_in = token_data.get("expires_in", 7200)
        instance_url = token_data.get("instance_url")

        if not access_token:
            raise OperationalError("No access token in response")

        if not instance_url:
            raise OperationalError("No instance_url in response")

        return access_token, expires_in, instance_url

    @abstractmethod
    def _fetch_new_token(self) -> tuple[str, int, str]:
        """
        Fetch a new OAuth token.

        Returns:
            Tuple of (access_token, expires_in_seconds, instance_url)

        Raises:
            OperationalError: If authentication fails
        """
        pass

    def get_oauth_token(self) -> str:
        """
        Fetch a fresh OAuth token.

        Matches the JDBC driver's DataCloudTokenProvider.getOAuthToken(), which
        always fetches a new core token rather than caching one.

        Returns:
            Valid OAuth access token

        Raises:
            OperationalError: If authentication fails
        """
        access_token, _expires_in, instance_url = self._fetch_new_token()
        self._instance_url = instance_url

        return access_token

    def get_instance_url(self) -> str:
        """
        Get the Salesforce instance URL returned by OAuth.

        This URL is extracted from the OAuth response and should be used for all API calls.

        Returns:
            Salesforce instance URL (e.g., "https://myorg.my.salesforce.com")

        Raises:
            OperationalError: If no token has been fetched yet
        """
        if self._instance_url is None:
            # Trigger token fetch to get instance URL
            self.get_oauth_token()

        if self._instance_url is None:
            raise OperationalError("Instance URL not available from OAuth response")

        return self._instance_url


class UsernamePasswordAuthenticator(OAuthAuthenticator):
    """
    OAuth 2.0 Username-Password (Resource Owner Password Credentials) flow.

    This flow is suitable for trusted applications where the user provides
    their username and password directly.
    """

    def __init__(
        self,
        login_url: str = "https://login.salesforce.com",
        username: str = None,
        password: str = None,
        client_id: str = None,
        client_secret: str = None,
        retry: Optional[RetryConfig] = None,
        http: Optional[HttpOptions] = None,
    ):
        """
        Initialize username/password authenticator.

        Args:
            login_url: Salesforce login URL (default: "https://login.salesforce.com")
            username: Salesforce username
            password: Salesforce password
            client_id: Connected app client ID
            client_secret: Connected app client secret
            retry: Retry policy for transient token-endpoint failures
            http: Transport options (session, timeout, TLS verification, proxies)
        """
        super().__init__(login_url, retry, http)
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret

    def _fetch_new_token(self) -> tuple[str, int, str]:
        """Fetch OAuth token using username/password flow."""
        token_url = f"{self.login_url}/services/oauth2/token"

        data = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password,
        }

        return self._request_token(
            token_url, data, "Authentication failed with username/password"
        )


def _load_rsa_private_key(key: Union[str, bytes, os.PathLike, None]):
    """
    Load an RSA private key from PEM text/bytes or a PEM file path.

    Raises:
        ValueError: If the key is missing, unreadable, not PEM, or not RSA.
    """
    if not key:
        raise ValueError("jwt_private_key is required")

    is_inline_pem = (
        isinstance(key, str) and key.lstrip().startswith("-----BEGIN")
    ) or (isinstance(key, bytes) and key.lstrip().startswith(b"-----BEGIN"))
    if not is_inline_pem:
        try:
            with open(key, "rb") as f:
                key = f.read()
        except OSError as e:
            raise ValueError(
                f"jwt_private_key is neither PEM text nor a readable file path: {e}"
            ) from e

    pem = key.encode() if isinstance(key, str) else key
    try:
        private_key = serialization.load_pem_private_key(pem.strip(), password=None)
    except (ValueError, TypeError) as e:
        raise ValueError(
            f"Failed to parse jwt_private_key as an unencrypted PEM key: {e}"
        ) from e

    if not isinstance(private_key, rsa.RSAPrivateKey):
        raise ValueError("jwt_private_key must be an RSA private key")
    return private_key


class JWTAuthenticator(OAuthAuthenticator):
    """
    OAuth 2.0 JWT Bearer Token flow.

    This flow uses a JWT signed with a private key to authenticate.
    It's suitable for server-to-server integrations.
    """

    def __init__(
        self,
        login_url: str = "https://login.salesforce.com",
        client_id: str = None,
        username: str = None,
        jwt_private_key: Union[str, bytes, os.PathLike] = None,
        jwt_expiry_seconds: int = 120,
        retry: Optional[RetryConfig] = None,
        http: Optional[HttpOptions] = None,
    ):
        """
        Initialize JWT authenticator.

        Args:
            login_url: Salesforce login URL (default: "https://login.salesforce.com")
            client_id: Connected app client ID
            username: Salesforce username
            jwt_private_key: RSA private key: PEM text or bytes, or a path to a
                             PEM file. Validated immediately.
            jwt_expiry_seconds: JWT expiration time in seconds (default: 2 minutes)
            retry: Retry policy for transient token-endpoint failures
            http: Transport options (session, timeout, TLS verification, proxies)
        """
        super().__init__(login_url, retry, http)
        self.client_id = client_id
        self.username = username
        self.jwt_private_key = _load_rsa_private_key(jwt_private_key)
        self.jwt_expiry_seconds = jwt_expiry_seconds

    def _audience(self) -> str:
        """Scheme and host of the login URL, without any path (as JDBC does)."""
        parsed = urlparse(self.login_url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _create_jwt(self) -> str:
        """
        Create a JWT for the bearer token flow.

        Returns:
            Signed JWT string
        """
        current_time = int(time.time())

        payload = {
            "iss": self.client_id,  # Issuer (client ID)
            "sub": self.username,  # Subject (username)
            "aud": self._audience(),  # Audience (scheme + host of the login URL)
            "iat": current_time,  # Issued at
            "exp": current_time + self.jwt_expiry_seconds,  # Expiration
        }

        try:
            token = jwt.encode(payload, self.jwt_private_key, algorithm="RS256")
            return token
        except Exception as e:
            raise OperationalError(f"Failed to create JWT: {e}") from e

    def _fetch_new_token(self) -> tuple[str, int, str]:
        """Fetch OAuth token using JWT bearer flow."""
        token_url = f"{self.login_url}/services/oauth2/token"

        jwt_token = self._create_jwt()

        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt_token,
        }

        return self._request_token(
            token_url, data, "Authentication failed with JWT"
        )


class RefreshTokenAuthenticator(OAuthAuthenticator):
    """
    OAuth 2.0 Refresh Token flow.

    This flow uses a refresh token to obtain new access tokens.
    It's suitable when you already have a refresh token from a previous
    authorization flow.
    """

    def __init__(
        self,
        login_url: str = "https://login.salesforce.com",
        client_id: str = None,
        client_secret: str = None,
        refresh_token: str = None,
        retry: Optional[RetryConfig] = None,
        http: Optional[HttpOptions] = None,
    ):
        """
        Initialize refresh token authenticator.

        Args:
            login_url: Salesforce login URL (default: "https://login.salesforce.com")
            client_id: Connected app client ID
            client_secret: Connected app client secret
            refresh_token: OAuth refresh token
            retry: Retry policy for transient token-endpoint failures
            http: Transport options (session, timeout, TLS verification, proxies)
        """
        super().__init__(login_url, retry, http)
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token

    def _fetch_new_token(self) -> tuple[str, int, str]:
        """Fetch OAuth token using refresh token flow."""
        token_url = f"{self.login_url}/services/oauth2/token"

        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }

        return self._request_token(
            token_url, data, "Authentication failed with refresh token"
        )


class ClientCredentialsAuthenticator(OAuthAuthenticator):
    """
    OAuth 2.0 Client Credentials flow.

    This flow uses only client_id and client_secret to authenticate.
    It's suitable for server-to-server integrations where no user context
    is required (e.g., Data Cloud connectors with appropriate permissions).
    """

    def __init__(
        self,
        login_url: str = "https://login.salesforce.com",
        client_id: str = None,
        client_secret: str = None,
        retry: Optional[RetryConfig] = None,
        http: Optional[HttpOptions] = None,
    ):
        """
        Initialize client credentials authenticator.

        Args:
            login_url: Salesforce login URL (default: "https://login.salesforce.com")
            client_id: Connected app client ID
            client_secret: Connected app client secret
            retry: Retry policy for transient token-endpoint failures
            http: Transport options (session, timeout, TLS verification, proxies)
        """
        super().__init__(login_url, retry, http)
        self.client_id = client_id
        self.client_secret = client_secret

    def _fetch_new_token(self) -> tuple[str, int, str]:
        """Fetch OAuth token using client credentials flow."""
        token_url = f"{self.login_url}/services/oauth2/token"

        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        return self._request_token(
            token_url, data, "Authentication failed with client credentials"
        )


class SfCliAuthenticator(OAuthAuthenticator):
    """
    Authenticates using the locally installed Salesforce CLI (`sf`).

    Intended for local development: reuses whatever org the Salesforce CLI is
    already authenticated against (e.g. via `sf org login web`) instead of
    requiring a connected app's client_id/secret or a JWT key. Every call
    re-invokes the CLI, which transparently refreshes its own stored token if
    needed — this matches the no-cache model of the other authenticators.

    Recent CLI versions redact `accessToken` from `sf org display` output, so
    the token and instance URL are fetched from two separate subcommands.
    """

    def __init__(self, target_org: Optional[str] = None, cli_path: str = "sf"):
        """
        Initialize the Salesforce CLI authenticator.

        Args:
            target_org: Org alias or username to pass as `--target-org` to
                        the CLI. If omitted, the CLI's own default org applies.
            cli_path: Path to the Salesforce CLI executable (default: "sf")
        """
        super().__init__()
        self.target_org = target_org
        self.cli_path = cli_path

    def _run_sf(self, *args: str) -> dict:
        """Run an `sf` subcommand with --json and return its parsed payload."""
        command = [self.cli_path, *args, "--json"]
        if self.target_org:
            command += ["--target-org", self.target_org]

        # --json output must stay plain: if the caller's shell forces color
        # (e.g. FORCE_COLOR set), the CLI emits ANSI codes even with --json,
        # which breaks JSON parsing. NO_COLOR overrides that.
        env = {**os.environ, "NO_COLOR": "1"}

        try:
            result = subprocess.run(
                command, capture_output=True, text=True, timeout=30, env=env
            )
        except FileNotFoundError as e:
            raise OperationalError(
                f"Salesforce CLI ('{self.cli_path}') not found. Install it from "
                "https://developer.salesforce.com/tools/salesforcecli and authenticate "
                "with `sf org login web` before using SfCliAuthenticator."
            ) from e
        except subprocess.TimeoutExpired as e:
            raise OperationalError(f"Salesforce CLI command timed out: {' '.join(command)}") from e

        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            raise OperationalError(
                f"Could not parse output of `{' '.join(command)}`: {e}"
            ) from e

        if payload.get("status") != 0:
            message = payload.get("message", "Unknown error from Salesforce CLI")
            raise OperationalError(f"Salesforce CLI authentication failed: {message}")

        return payload.get("result", {})

    def _fetch_new_token(self) -> tuple[str, int, str]:
        """Fetch a core token and instance URL via the Salesforce CLI."""
        org_info = self._run_sf("org", "display")
        instance_url = org_info.get("instanceUrl")
        if not instance_url:
            raise OperationalError("No instanceUrl in `sf org display` output")

        token_info = self._run_sf("org", "auth", "show-access-token")
        access_token = token_info.get("accessToken")
        if not access_token:
            raise OperationalError("No accessToken in `sf org auth show-access-token` output")

        return access_token, 7200, instance_url
