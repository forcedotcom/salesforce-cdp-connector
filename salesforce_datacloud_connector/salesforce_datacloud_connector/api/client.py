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
from .models import QueryResponse, QueryStatus


class DataCloudQueryClient:
    """
    Client for interacting with Salesforce Data Cloud Query API.

    Handles query execution, status polling, result fetching, and cancellation.
    Includes automatic retry logic for transient failures.
    """

    # Retry configuration
    MAX_RETRIES = 3
    RETRY_WAIT_SECONDS = 5

    # Long polling configuration (max wait time for status checks)
    MAX_WAIT_TIME_MS = 10000  # 10 seconds

    def __init__(
        self,
        tenant_endpoint: str,
        auth_token_getter: callable,
        dataspace: Optional[str] = None,
        workload: Optional[str] = None,
    ):
        """
        Initialize the API client for off-core Query v3.

        Args:
            tenant_endpoint: Data Cloud tenant endpoint (e.g., https://{tenant}.c360a.salesforce.com)
            auth_token_getter: Callable that returns a valid CDP token
            dataspace: Data space name (default: "default")
            workload: Optional workload name for observability
        """
        self.tenant_endpoint = tenant_endpoint.rstrip("/")
        self.auth_token_getter = auth_token_getter
        self.dataspace = dataspace or "default"
        self.workload = workload
        self._base_url = f"{self.tenant_endpoint}/api/v3/query"

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for v3 API requests."""
        token = self.auth_token_getter()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "ctx-dataspace-ds_name": self.dataspace,
        }

        # Add workload header if specified
        if self.workload:
            headers["x-hyperdb-workload"] = f"python-connector-v2_{self.workload}"
        else:
            headers["x-hyperdb-workload"] = "python-connector-v2"

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
        Convert Python dict parameters to v3 parameters array format.

        V3 uses positional parameters (question-mark style), so only type and value are sent.

        Args:
            params: Dictionary of named parameters (names ignored in v3)

        Returns:
            List of parameter dictionaries in v3 API format: [{"type": "varchar", "value": "..."}]
        """
        if not params:
            return []

        sql_params = []
        for name, value in params.items():
            param_type = infer_sql_parameter_type(value)
            # V3 parameter format: {"type": "varchar", "value": "..."} (lowercase type)
            sql_params.append({
                "type": param_type.lower(),
                "value": value,
            })

        return sql_params

    def execute_query(
        self,
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
        row_limit: int = 1000000,
    ) -> QueryResponse:
        """
        Execute a SQL query via POST /api/v3/query.

        Args:
            sql: SQL query string
            parameters: Named parameters dict (e.g., {"status": "Active"})
            row_limit: Maximum rows to return (passed as queryRowLimit)

        Returns:
            QueryResponse with initial results and status

        Raises:
            ProgrammingError: For SQL syntax errors (400)
            OperationalError: For auth/network failures (401, 403, 500+)
        """
        request_body = {
            "sql": sql,
            "transferMode": "ADAPTIVE",
            "queryRowLimit": row_limit,
        }

        # Add parameters if provided (v3 uses positional parameters)
        if parameters:
            request_body["parameters"] = self._convert_parameters(parameters)

        response = self._make_request("POST", self._base_url, json_data=request_body)

        # Parse response body
        body = response.json()
        query_response = QueryResponse.from_dict(body)

        # Parse status from x-hyperdb-status header (v3)
        status_header = response.headers.get("x-hyperdb-status")
        if status_header:
            import json
            status_data = json.loads(status_header)
            query_response.status = QueryStatus.from_dict(status_data)

        return query_response

    def get_query_status(
        self, query_id: str, wait_time_ms: Optional[int] = None
    ) -> QueryStatus:
        """
        Get query status via GET /api/v3/query/{queryId}.

        Args:
            query_id: Query ID from execute_query
            wait_time_ms: Milliseconds to wait before returning (long-polling, max 10000)

        Returns:
            QueryStatus object (parsed from x-hyperdb-status header)

        Raises:
            OperationalError: For network failures
        """
        url = f"{self._base_url}/{query_id}"
        params = {}

        # Use long-polling to reduce API calls
        if wait_time_ms is not None:
            params["waitTimeMs"] = min(wait_time_ms, self.MAX_WAIT_TIME_MS)

        response = self._make_request("GET", url, params=params)

        # Parse status from x-hyperdb-status header (v3)
        status_header = response.headers.get("x-hyperdb-status")
        if not status_header:
            raise OperationalError("Missing x-hyperdb-status header in response")

        import json
        status_data = json.loads(status_header)
        return QueryStatus.from_dict(status_data)

    def fetch_results(
        self,
        query_id: str,
        offset: int = 0,
        row_limit: int = 1000000,
        omit_schema: bool = True,
    ) -> QueryResponse:
        """
        Fetch query results via GET /api/v3/query/{queryId}/rows.

        Args:
            query_id: Query ID
            offset: Starting row number (0-based, required in v3)
            row_limit: Maximum rows to return (default: 1000000, server may limit)
            omit_schema: If True, omit metadata in response (not used in v3, kept for compatibility)

        Returns:
            QueryResponse with rows

        Raises:
            ProgrammingError: If offset is out of range (400)
            OperationalError: For network failures
        """
        url = f"{self._base_url}/{query_id}/rows"
        params = {
            "offset": offset,
            "limit": row_limit,
            "byteLimit": 20971520,  # 20MB default
        }

        try:
            response = self._make_request("GET", url, params=params)
            return QueryResponse.from_dict(response.json())
        except Exception as e:
            # Handle "Request out of range" gracefully (400 error)
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
