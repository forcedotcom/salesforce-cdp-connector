"""
HTTP client for Salesforce Data Cloud off-core Query v3 REST API.

This module implements the v3 REST transport layer for the Query API:
- execute_query: POST /api/v3/query
- get_query_status: GET /api/v3/query/{id}
- fetch_results: GET /api/v3/query/{id}/rows
- cancel_query: DELETE /api/v3/query/{id}
- poll_until_complete: blocking poll until a query completes
"""

import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from ..exceptions import OperationalError, ProgrammingError, map_http_error_to_exception
from ..types import infer_sql_parameter_type
from .models import QueryResponse, QueryStatus

# Matches a :name placeholder while ignoring PostgreSQL ::type casts. The
# negative lookbehind (?<![:\w]) rejects the second colon of "::" and any
# colon glued to an identifier, so "value::regclass" is left untouched but
# "= :kind" is captured.
_NAMED_PARAM_RE = re.compile(r"(?<![:\w]):(\w+)")


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
        """
        Raise appropriate exception for API error response.

        V3 error body: {"timestamp", "error", "message", "path", "tenantId", "internalErrorCode", "details"}
        """
        try:
            error_data = response.json()
            error_message = error_data.get("message", response.text)
        except Exception:
            error_message = response.text

        exception = map_http_error_to_exception(
            response.status_code, error_message, "API request failed"
        )
        raise exception

    @staticmethod
    def _param_entry(value: Any) -> Dict[str, Any]:
        """Build one v3 parameter entry: {"type": <lowercase>, "value": ...}."""
        return {"type": infer_sql_parameter_type(value).lower(), "value": value}

    def _bind_parameters(
        self, sql: str, params: Optional[Dict[str, Any]]
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Prepare SQL and parameters for v3, which accepts only positional
        (question-mark) parameters.

        The driver advertises paramstyle="named", so callers write :name
        placeholders with a values dict. v3 rejects named placeholders
        ("conflicting parameter style 'named' ... set to 'qmark'"), so each
        :name is rewritten to ? and its value emitted positionally in the order
        the placeholders appear in the SQL. A name used N times expands to N
        question marks and N repeated values.

        SQL that already uses ? placeholders (e.g. internal catalog queries)
        contains no :name tokens: it is passed through unchanged and the value
        array is built from the dict's insertion order.

        Args:
            sql: Query text with :name and/or ? placeholders
            params: Values dict (names match the :name placeholders)

        Returns:
            (sql_with_qmarks, positional_parameter_array)

        Raises:
            ProgrammingError: If the SQL references a :name absent from params
        """
        if not params:
            return sql, []

        # No :name placeholders → qmark-style SQL: positional array from dict order.
        if not _NAMED_PARAM_RE.search(sql):
            return sql, [self._param_entry(v) for v in params.values()]

        sql_params: List[Dict[str, Any]] = []

        def _replace(match: "re.Match") -> str:
            name = match.group(1)
            if name not in params:
                raise ProgrammingError(
                    f"No value supplied for named parameter ':{name}'"
                )
            sql_params.append(self._param_entry(params[name]))
            return "?"

        # re.sub invokes _replace left-to-right, so sql_params ends up in SQL order.
        new_sql = _NAMED_PARAM_RE.sub(_replace, sql)
        return new_sql, sql_params

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
        # v3 accepts only positional (qmark) parameters; rewrite any :name
        # placeholders and build the positional array in SQL order.
        sql, sql_params = self._bind_parameters(sql, parameters)

        request_body = {
            "sql": sql,
            "transferMode": "ADAPTIVE",
            "queryRowLimit": row_limit,
        }

        if sql_params:
            request_body["parameters"] = sql_params

        response = self._make_request("POST", self._base_url, json_data=request_body)

        # Parse response body
        body = response.json()
        query_response = QueryResponse.from_dict(body)

        # Parse status from x-hyperdb-status header (v3). The header carries the
        # queryId/rowCount/completionStatus the cursor relies on; the body never
        # contains status in v3, so a missing header is a hard error, not None.
        status_header = response.headers.get("x-hyperdb-status")
        if not status_header:
            raise OperationalError("Missing x-hyperdb-status header in query response")

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
        Cancel a running query via DELETE /api/v3/query/{queryId}.

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
