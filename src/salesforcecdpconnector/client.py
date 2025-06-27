import requests
import json
from loguru import logger
from typing import Optional, Dict, Any, List, Tuple, Union, Sequence

from .auth import AuthHandler
from .exceptions import ApiError, QueryError, AuthenticationError
from .constants import DEFAULT_API_VERSION

class SalesforceCDPClient:
    """Handles low-level communication with the Salesforce Datacloud."""

    def __init__(self, auth_handler: AuthHandler, api_version: str = DEFAULT_API_VERSION):
        self.auth = auth_handler
        self.api_version = api_version
        # Use a session provided by auth or create a new one
        self.session = getattr(auth_handler, '_session', requests.Session())
        self._base_api_path = None
        logger.debug(f"SalesforceCDPClient initialized with API version: {self.api_version}")

    def _get_base_api_path(self) -> str:
        """Constructs the base path for data service API calls."""
        if self._base_api_path is None:
             instance_url = self.auth.get_instance_url() # Ensures auth is done
             self._base_api_path = f"{instance_url}/services/data/{self.api_version}/ssot"
        return self._base_api_path

    def _request(self, method: str, path: str, **kwargs) -> dict:
        """Makes an authenticated HTTP request to the CDP API."""
        self.auth.ensure_valid_token() # Ensure token is valid before making call

        base_path = self._get_base_api_path()
        full_url = f"{base_path}{path}"

        headers = self.auth.get_headers()
        # Allow overriding headers if needed
        headers.update(kwargs.pop('headers', {}))

        # Ensure data is JSON encoded for POST/PATCH etc. if it's a dict
        if 'data' in kwargs and isinstance(kwargs['data'], (dict, list)):
             kwargs['data'] = json.dumps(kwargs['data'])
             if 'Content-Type' not in headers:
                 headers['Content-Type'] = 'application/json'
        elif 'json' in kwargs and 'Content-Type' not in headers:
             headers['Content-Type'] = 'application/json'


        logger.debug(f"Request: {method} {full_url}")
        logger.trace(f" Headers: {headers}")
        if 'params' in kwargs: logger.trace(f" Params: {kwargs['params']}")
        if 'data' in kwargs: logger.trace(f" Data: {kwargs['data']}")
        if 'json' in kwargs: logger.trace(f" JSON: {kwargs['json']}")

        try:
            response = self.session.request(method, full_url, headers=headers, **kwargs)
            response.raise_for_status() # Raise HTTPError for 4xx/5xx

            # Handle cases where response might be empty
            if response.status_code == 204 or not response.content:
                return {}

            # Assume JSON response otherwise
            return response.json()

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else 'N/A'
            content = e.response.text if e.response is not None else 'No Content'
            logger.error(f"API HTTP error: {method} {full_url} -> Status {status_code}")
            logger.error(f"Response content: {content}")

            # Check for auth token expiry specifically if possible (e.g., 401/403)
            if status_code in (401, 403):
                 # Could potentially try one refresh here, but safer to raise
                 raise AuthenticationError(f"API request failed, possibly expired token ({status_code}): {content}", http_error=e) from e
            
            try:
                error_details = json.loads(content) if content else {}
                if isinstance(error_details, list) and error_details:
                    message = error_details[0].get('message', str(e))
                    error_code = error_details[0].get('errorCode', 'UNKNOWN')
                    # Check if it looks like a query syntax error
                    if "QUERY_PARSER_ERROR" in error_code or "INVALID_QUERY" in error_code:
                         raise QueryError(f"Query failed [{error_code}]: {message}", http_error=e) from e
                else:
                    message = str(e)
            except (json.JSONDecodeError, TypeError):
                message = content # Use raw content if not JSON
            raise ApiError(f"API request failed ({status_code}): {message}", http_error=e) from e

        except (requests.exceptions.RequestException, ConnectionError) as e:
            logger.error(f"API network error: {method} {full_url} -> {e}")
            raise ApiError(f"Network error communicating with API: {e}") from e

    def submit_query(self, sql: str, params: Optional[Union[Sequence, Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Submits a SQL query to the CDP API."""
        logger.info(f"Submitting query: {sql[:100]}{'...' if len(sql) > 100 else ''}")
        payload = {"sql": sql}
        # The API seems to expect 'sqlParameters' - adjust if your API uses a different key
        if params:
            payload["sqlParameters"] = params

        # Note: The example code used /query-sql directly. Adjust path if needed.
        return self._request("POST", "/query-sql", json=payload)

    def get_query_status(self, query_id: str) -> Dict[str, Any]:
        """Gets the status of a previously submitted query."""
        logger.debug(f"Getting status for query ID: {query_id}")
        return self._request("GET", f"/query-sql/{query_id}")

    def get_query_results(self, query_id: str, offset: int, limit: int) -> Dict[str, Any]:
        """Fetches a page of results for a query."""
        logger.debug(f"Fetching results for query ID: {query_id}, offset: {offset}, limit: {limit}")
        params = {"offset": offset, "rowLimit": limit}
        return self._request("GET", f"/query-sql/{query_id}/rows", params=params)