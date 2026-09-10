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

import threading
import time
from abc import ABC, abstractmethod
from typing import Optional, Tuple

import jwt
import requests

from ..exceptions import OperationalError


class OAuthAuthenticator(ABC):
    """
    Abstract base class for OAuth authenticators.

    All authenticators must implement _fetch_new_token(). get_oauth_token()
    always fetches a fresh token — no core-token caching (matches JDBC).
    """

    def __init__(self, login_url: str = "https://login.salesforce.com"):
        """
        Initialize the authenticator.

        Args:
            login_url: Salesforce login URL (e.g., "https://login.salesforce.com" or
                      "https://test.salesforce.com" for sandboxes)
        """
        self.login_url = login_url.rstrip("/")
        self._instance_url: Optional[str] = None
        # Guards publication of self._instance_url and single-flights the
        # cold-miss fetch in get_instance_url() so one shared authenticator is
        # safe to use from multiple threads (threadsafety=2).
        self._lock = threading.Lock()

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

    def get_oauth_token_and_instance_url(self) -> Tuple[str, str]:
        """
        Fetch a fresh core token and return it paired with its instance URL.

        Both values come from the SAME fetch, so a caller never pairs one
        fetch's token with another fetch's instance URL — the mismatch that a
        shared authenticator would otherwise expose when get_oauth_token() and
        get_instance_url() are called separately by racing threads.

        The (potentially slow) HTTP fetch runs OUTSIDE the lock so concurrent
        callers do not serialize on the network round-trip; only the tiny
        publication of self._instance_url is guarded, keeping get_instance_url()
        readers consistent.

        Matches the JDBC driver's DataCloudTokenProvider.getOAuthToken(): the
        core token is always fetched fresh (never cached).

        Returns:
            (access_token, instance_url) from one fetch

        Raises:
            OperationalError: If authentication fails
        """
        access_token, _expires_in, instance_url = self._fetch_new_token()
        with self._lock:
            self._instance_url = instance_url
        return access_token, instance_url

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
        access_token, _instance_url = self.get_oauth_token_and_instance_url()
        return access_token

    def get_instance_url(self) -> str:
        """
        Get the Salesforce instance URL returned by OAuth.

        This URL is extracted from the OAuth response and should be used for all
        API calls. Unlike the core token, the instance URL is stable org
        metadata, so it is cached after the first fetch: a cold miss is
        single-flighted under the lock (double-checked locking) so N concurrent
        first-callers trigger exactly ONE fetch, not a thundering herd.

        Returns:
            Salesforce instance URL (e.g., "https://myorg.my.salesforce.com")

        Raises:
            OperationalError: If no token has been fetched yet
        """
        # Fast path: already published, no lock needed.
        if self._instance_url is not None:
            return self._instance_url

        with self._lock:
            # Re-check under the lock — another thread may have published while
            # we waited. Fetch directly (not via get_oauth_token, which would
            # re-acquire this non-reentrant lock) so the cold miss fetches once.
            if self._instance_url is None:
                _access_token, _expires_in, instance_url = self._fetch_new_token()
                self._instance_url = instance_url

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
    ):
        """
        Initialize username/password authenticator.

        Args:
            login_url: Salesforce login URL (default: "https://login.salesforce.com")
            username: Salesforce username
            password: Salesforce password
            client_id: Connected app client ID
            client_secret: Connected app client secret
        """
        super().__init__(login_url)
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

        try:
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()

            access_token = token_data.get("access_token")
            # Salesforce typically returns tokens valid for 2 hours (7200s)
            # If not specified, default to 2 hours
            expires_in = token_data.get("expires_in", 7200)
            instance_url = token_data.get("instance_url")

            if not access_token:
                raise OperationalError("No access token in response")

            if not instance_url:
                raise OperationalError("No instance_url in response")

            return access_token, expires_in, instance_url

        except requests.exceptions.RequestException as e:
            raise OperationalError(
                f"Authentication failed with username/password: {e}"
            ) from e


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
        jwt_private_key: str = None,
        jwt_expiry_seconds: int = 300,
    ):
        """
        Initialize JWT authenticator.

        Args:
            login_url: Salesforce login URL (default: "https://login.salesforce.com")
            client_id: Connected app client ID
            username: Salesforce username
            jwt_private_key: Private key in PEM format (RSA)
            jwt_expiry_seconds: JWT expiration time in seconds (default: 5 minutes)
        """
        super().__init__(login_url)
        self.client_id = client_id
        self.username = username
        self.jwt_private_key = jwt_private_key
        self.jwt_expiry_seconds = jwt_expiry_seconds

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
            "aud": self.login_url,  # Audience (Salesforce login URL)
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

        try:
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()

            access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 7200)
            instance_url = token_data.get("instance_url")

            if not access_token:
                raise OperationalError("No access token in response")

            if not instance_url:
                raise OperationalError("No instance_url in response")

            return access_token, expires_in, instance_url

        except requests.exceptions.RequestException as e:
            raise OperationalError(f"Authentication failed with JWT: {e}") from e


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
    ):
        """
        Initialize refresh token authenticator.

        Args:
            login_url: Salesforce login URL (default: "https://login.salesforce.com")
            client_id: Connected app client ID
            client_secret: Connected app client secret
            refresh_token: OAuth refresh token
        """
        super().__init__(login_url)
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

        try:
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()

            access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 7200)
            instance_url = token_data.get("instance_url")

            if not access_token:
                raise OperationalError("No access token in response")

            if not instance_url:
                raise OperationalError("No instance_url in response")

            return access_token, expires_in, instance_url

        except requests.exceptions.RequestException as e:
            raise OperationalError(
                f"Authentication failed with refresh token: {e}"
            ) from e


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
    ):
        """
        Initialize client credentials authenticator.

        Args:
            login_url: Salesforce login URL (default: "https://login.salesforce.com")
            client_id: Connected app client ID
            client_secret: Connected app client secret
        """
        super().__init__(login_url)
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

        try:
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()

            access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 7200)
            instance_url = token_data.get("instance_url")

            if not access_token:
                raise OperationalError("No access token in response")

            if not instance_url:
                raise OperationalError("No instance_url in response")

            return access_token, expires_in, instance_url

        except requests.exceptions.RequestException as e:
            raise OperationalError(
                f"Authentication failed with client credentials: {e}"
            ) from e
