"""
HTTP client for Salesforce Data Cloud Query API.

This module handles all REST API calls to the Query API endpoints:
- Execute queries (createSqlQuery)
- Check query status (getSqlQuery)
- Fetch results (getSqlQueryRows)
- Cancel queries (cancelSqlQuery)
- Fetch table metadata (getTableMetadata)
"""

import time
from typing import Any, Dict, List, Optional

import requests

from ..exceptions import OperationalError, map_http_error_to_exception
from ..types import infer_sql_parameter_type
from .models import QueryResponse, QueryStatus, SqlParameter


class DataCloudQueryClient:
    """
    Client for interacting with Salesforce Data Cloud Query API.

    Handles query execution, status polling, result fetching, and cancellation.
    Includes automatic retry logic for transient failures.
    """

    # API version to use (matches reference Java implementation)
    API_VERSION = "v64.0"

    # Retry configuration
    MAX_RETRIES = 3
    RETRY_WAIT_SECONDS = 5

    # Long polling configuration (max wait time for status checks)
    MAX_WAIT_TIME_MS = 10000  # 10 seconds

    def __init__(
        self,
        instance_url: str,
        auth_token_getter: callable,
        dataspace: Optional[str] = None,
        workload: Optional[str] = None,
    ):
        """
        Initialize the API client.

        Args:
            instance_url: Salesforce instance URL
            auth_token_getter: Callable that returns a valid OAuth token
            dataspace: Data space name (default: "default")
            workload: Optional workload name for logging/debugging
        """
        self.instance_url = instance_url.rstrip("/")
        self.auth_token_getter = auth_token_getter
        self.dataspace = dataspace
        self.workload = workload
        self._base_url = f"{self.instance_url}/services/data/{self.API_VERSION}/ssot/query-sql"

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests, including auth token and dataspace."""
        token = self.auth_token_getter()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        if self.dataspace:
            headers["dataspace"] = self.dataspace
        return headers

    def _make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        retry_count: int = 0,
    ) -> requests.Response:
        """
        Make an HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, DELETE)
            url: Request URL
            params: Query parameters
            json_data: JSON request body
            retry_count: Current retry attempt

        Returns:
            Response object

        Raises:
            Exception: If request fails after all retries
        """
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self._get_headers(),
                params=params,
                json=json_data,
                timeout=30,
            )
            # Check for errors
            if not response.ok:
                # Don't retry client errors (4xx except 429)
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    self._raise_api_error(response)

                # Retry on 5xx and 429 (rate limiting)
                if retry_count < self.MAX_RETRIES:
                    time.sleep(self.RETRY_WAIT_SECONDS)

                    return self._make_request(
                        method, url, params, json_data, retry_count + 1
                    )

                # All retries exhausted
                self._raise_api_error(response)

            return response

        except requests.exceptions.RequestException as e:
            # Network errors - retry if possible
            if retry_count < self.MAX_RETRIES:
                time.sleep(self.RETRY_WAIT_SECONDS)
                return self._make_request(
                    method, url, params, json_data, retry_count + 1
                )
            raise OperationalError(f"Request failed: {e}") from e

    def _raise_api_error(self, response: requests.Response):
        """Raise appropriate exception for API error response."""
        try:
            error_data = response.json()
            error_message = error_data.get("message", response.text)
        except Exception:
            error_message = response.text

        exception = map_http_error_to_exception(
            response.status_code, error_message, "API request failed"
        )
        raise exception

    def _convert_parameters(self, params: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert Python dict parameters to sqlParameters array format.

        Args:
            params: Dictionary of named parameters

        Returns:
            List of parameter dictionaries in API format
        """
        if not params:
            return []

        sql_params = []
        for name, value in params.items():
            param_type = infer_sql_parameter_type(value)
            sql_params.append(
                SqlParameter(name=name, value=value, type=param_type).to_dict()
            )

        return sql_params

    def execute_query(
        self,
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
        row_limit: int = 1000000,
    ) -> QueryResponse:
        """
        Execute a SQL query (createSqlQuery endpoint).

        Args:
            sql: SQL query string (may contain :param placeholders)
            parameters: Named parameters dict (e.g., {"param": "value"})
            row_limit: Maximum rows to return in initial response (server may limit)

        Returns:
            QueryResponse with initial results and status

        Raises:
            ProgrammingError: For SQL syntax errors
            OperationalError: For auth/network failures
        """
        params = {}
        if self.workload:
            params["workload"] = self.workload

        request_body = {
            "sql": sql,
            "rowLimit": row_limit,
        }

        # Add parameters if provided
        if parameters:
            request_body["sqlParameters"] = self._convert_parameters(parameters)

        response = self._make_request("POST", self._base_url, params=params, json_data=request_body)
        return QueryResponse.from_dict(response.json())

    def get_query_status(
        self, query_id: str, wait_time_ms: Optional[int] = None
    ) -> QueryStatus:
        """
        Get query status (getSqlQuery endpoint).

        Args:
            query_id: Query ID from execute_query
            wait_time_ms: Milliseconds to wait before returning (long-polling, max 10000)

        Returns:
            QueryStatus object

        Raises:
            OperationalError: For network failures
        """
        url = f"{self._base_url}/{query_id}"
        params = {}

        # Use long-polling to reduce API calls
        if wait_time_ms is not None:
            params["waitTimeMs"] = min(wait_time_ms, self.MAX_WAIT_TIME_MS)

        response = self._make_request("GET", url, params=params)
        data = response.json()
        return QueryStatus.from_dict(data.get("status", {}))

    def fetch_results(
        self,
        query_id: str,
        offset: int = 0,
        row_limit: int = 1000000,
        omit_schema: bool = True,
    ) -> QueryResponse:
        """
        Fetch query results (getSqlQueryRows endpoint).

        Args:
            query_id: Query ID from execute_query
            offset: Starting row number (0-based)
            row_limit: Maximum rows to return (server determines actual chunk size ~2MB)
            omit_schema: If True, don't return metadata (reduces response size)

        Returns:
            QueryResponse with rows

        Raises:
            ProgrammingError: If offset is out of range
            OperationalError: For network failures
        """
        url = f"{self._base_url}/{query_id}/rows"
        params = {
            "offset": offset,
            "rowLimit": row_limit,
            "omitSchema": "true" if omit_schema else "false",
        }

        if self.workload:
            params["workload"] = self.workload

        try:
            response = self._make_request("GET", url, params=params)
            return QueryResponse.from_dict(response.json())
        except Exception as e:
            # Handle "Request out of range" gracefully
            if "out of range" in str(e).lower():
                # Return empty response
                return QueryResponse(data=[], metadata=[], returned_rows=0)
            raise

    def cancel_query(self, query_id: str):
        """
        Cancel a running query (cancelSqlQuery endpoint).

        Args:
            query_id: Query ID to cancel

        Raises:
            OperationalError: For network failures
        """
        url = f"{self._base_url}/{query_id}"
        self._make_request("DELETE", url)

    def poll_until_complete(
        self, query_id: str, poll_interval_ms: int = 5000, timeout_seconds: int = 300
    ) -> QueryStatus:
        """
        Poll query status until it completes or times out.

        Uses long-polling with waitTimeMs to reduce API calls.

        Args:
            query_id: Query ID to poll
            poll_interval_ms: Milliseconds to wait between polls (used as waitTimeMs)
            timeout_seconds: Maximum time to wait (default: 5 minutes)

        Returns:
            Final QueryStatus

        Raises:
            OperationalError: If query times out or fails
        """
        start_time = time.time()
        wait_time_ms = min(poll_interval_ms, self.MAX_WAIT_TIME_MS)

        while True:
            status = self.get_query_status(query_id, wait_time_ms=wait_time_ms)

            if status.is_complete():
                return status

            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise OperationalError(
                    f"Query timed out after {timeout_seconds} seconds"
                )

            # Long-polling with waitTimeMs means we don't need additional sleep
            # The server will wait up to waitTimeMs before responding
