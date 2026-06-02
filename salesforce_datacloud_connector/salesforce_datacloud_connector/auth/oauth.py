"""
OAuth authentication for Salesforce Data Cloud.

This module provides OAuth authenticators for different flows:
- Username/Password (OAuth 2.0 Password Grant)
- JWT Bearer Token (OAuth 2.0 JWT Bearer Flow)
- Refresh Token (OAuth 2.0 Refresh Token Flow)

All authenticators implement token caching with automatic refresh.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Optional

import jwt
import requests

from ..exceptions import OperationalError


class OAuthAuthenticator(ABC):
    """
    Abstract base class for OAuth authenticators.

    All authenticators must implement get_oauth_token() and handle token caching
    with automatic refresh before expiration (60s buffer).
    """

    def __init__(self, login_url: str = "https://login.salesforce.com"):
        """
        Initialize the authenticator.

        Args:
            login_url: Salesforce login URL (e.g., "https://login.salesforce.com" or
                      "https://test.salesforce.com" for sandboxes)
        """
        self.login_url = login_url.rstrip("/")
        self._cached_token: Optional[str] = None
        self._token_expiry: Optional[float] = None
        self._instance_url: Optional[str] = None

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
        Get a valid OAuth token, using cache or fetching a new one if needed.

        Automatically refreshes the token if it's within 60 seconds of expiry.

        Returns:
            Valid OAuth access token

        Raises:
            OperationalError: If authentication fails
        """
        current_time = time.time()

        # Check if we have a cached token that's still valid (with 60s buffer)
        if (
            self._cached_token is not None
            and self._token_expiry is not None
            and current_time < (self._token_expiry - 60)
        ):
            return self._cached_token

        # Fetch new token and instance URL
        access_token, expires_in, instance_url = self._fetch_new_token()
        self._cached_token = access_token
        self._token_expiry = current_time + expires_in
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

    def invalidate_token(self):
        """Invalidate the cached token, forcing a refresh on next request."""
        self._cached_token = None
        self._token_expiry = None


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
