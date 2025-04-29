import requests
from requests.exceptions import HTTPError, RequestException
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from loguru import logger

from .exceptions import AuthenticationError
from .constants import DEFAULT_TOKEN_TIMEOUT_SECONDS
from .utils import clean_login_url

class AuthHandler(ABC):
    @abstractmethod
    def authenticate(self) -> None:
        """Perform initial authentication and store credentials/tokens."""
        pass

    @abstractmethod
    def get_headers(self) -> dict:
        """Return headers required for authenticated API calls."""
        pass

    @abstractmethod
    def get_instance_url(self) -> str:
        """Return the base URL for API calls."""
        pass

    @abstractmethod
    def ensure_valid_token(self) -> None:
        """Check token validity and refresh if necessary."""
        pass


class PasswordGrantAuth(AuthHandler):
    def __init__(self, *, username: str, password: str, client_id: str, client_secret: str, domain: str, token_timeout: int = DEFAULT_TOKEN_TIMEOUT_SECONDS):
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self.domain = clean_login_url(domain)

        self._token_timeout = timedelta(seconds=token_timeout)
        self._token = None
        self._instance_url = None
        self._token_created_at = None
        self._session = requests.Session() # Use session for potential keep-alive

    def authenticate(self) -> None:
        logger.info(f"Attempting authentication for domain: {self.domain}, user: {self.username}")
        auth_url = f"https://{self.domain}/services/oauth2/token"
        data = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }
        try:
            response = self._session.post(url=auth_url, data=data, headers=headers)
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            auth_data = response.json()
            self._token = auth_data["access_token"]
            self._instance_url = auth_data["instance_url"]

            # Ensure instance URL doesn't have trailing slash for consistency
            if self._instance_url.endswith('/'):
                self._instance_url = self._instance_url[:-1]
            self._token_created_at = datetime.now()
            logger.info(f"Authentication successful. Instance URL: {self._instance_url}")

        except HTTPError as e:
            logger.error(f"Authentication HTTP error: {e}")
            if e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            raise AuthenticationError(f"Authentication failed: {e}", http_error=e) from e
        except (RequestException, ConnectionError) as e:
            logger.error(f"Authentication network error: {e}")
            raise AuthenticationError(f"Network error during authentication: {e}") from e
        except (KeyError, ValueError) as e: # Handle JSON parsing errors or missing keys
             logger.error(f"Authentication response parsing error: {e}")
             raise AuthenticationError(f"Could not parse authentication response: {e}") from e


    def _is_token_valid(self) -> bool:
        if not self._token or not self._token_created_at:
            return False
        return (datetime.now() - self._token_created_at) < self._token_timeout

    def ensure_valid_token(self) -> None:
        """Checks token validity and re-authenticates if expired."""
        if not self._is_token_valid():
            logger.info("Auth token expired or invalid, re-authenticating.")
            self.authenticate() # Re-authenticate to refresh

    def get_headers(self) -> dict:
        self.ensure_valid_token() # Make sure token is valid before returning headers
        if not self._token:
             raise AuthenticationError("Cannot get headers, authentication token is missing.")
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json" # Common requirement for APIs
        }

    def get_instance_url(self) -> str:
        if not self._instance_url:
             # Try to authenticate if URL not set yet
             self.ensure_valid_token()
             if not self._instance_url:
                 raise AuthenticationError("Cannot get instance URL, authentication failed or not performed.")
        return self._instance_url