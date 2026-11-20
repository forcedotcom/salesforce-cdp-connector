"""
HTTP client for Salesforce Data Cloud off-core Query v3 REST API.

This module implements the v3 REST transport layer for the Query API:
- execute_query: POST /api/v3/query
- get_query_status: GET /api/v3/query/{id}
- fetch_results: GET /api/v3/query/{id}/rows
- cancel_query: DELETE /api/v3/query/{id}
- poll_until_complete: blocking poll until a query completes

Output format: execute_query and fetch_results request the Arrow IPC stream
via the Accept header. get_query_status has no Arrow variant per the v3 spec
and always requests JSON.
"""

import json
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from ..exceptions import OperationalError, ProgrammingError, map_http_error_to_exception
from ..http_options import HttpOptions
from ..types import infer_sql_parameter_type
from .models import QueryResponse, QueryStatus

# Matches a :name placeholder while ignoring PostgreSQL ::type casts. The
# negative lookbehind (?<![:\w]) rejects the second colon of "::" and any
# colon glued to an identifier, so "value::regclass" is left untouched but
# "= :kind" is captured.
_NAMED_PARAM_RE = re.compile(r"(?<![:\w]):(\w+)")

_JSON_MEDIA_TYPE = "application/json"
_ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"


# SQLSTATE for failures the server did not describe (the JDBC driver uses the same code).
_GENERIC_SQLSTATE = "HY000"


class DataCloudQueryClient:
    """
    Client for interacting with Salesforce Data Cloud Query API.

    Handles query execution, status polling, result fetching, and cancellation.
    Includes automatic retry logic for transient failures of idempotent calls.
    The query-creating POST is never retried: each attempt would start a new
    query on the server.
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
        user_agent: Optional[str] = None,
        query_settings: Optional[Dict[str, str]] = None,
        on_unauthorized: Optional[Callable[[], None]] = None,
        http: Optional[HttpOptions] = None,
    ):
        """
        Initialize the API client for off-core Query v3.

        Args:
            tenant_endpoint: Data Cloud tenant endpoint (e.g., https://{tenant}.c360a.salesforce.com)
            auth_token_getter: Callable that returns a valid CDP token
            dataspace: Data space name (default: "default")
            workload: Optional workload name for observability
            user_agent: Optional caller identifier appended to the driver's own
                User-Agent token (e.g. "my-app/1.0")
            query_settings: Connection-wide default query settings (e.g. {"time_zone": "UTC"}),
                merged into every execute_query() call's "settings" field. See
                https://tableau.github.io/hyper-db/docs/hyper-api/connection#connection-settings
            on_unauthorized: Optional callback invoked when the API answers 401,
                before the request is retried once with a fresh token from
                auth_token_getter (typically the token provider's
                invalidate_token). Without it, a 401 is raised immediately.
            http: Transport options (session, timeout, TLS verification, proxies)
        """
        self.tenant_endpoint = tenant_endpoint.rstrip("/")
        self.auth_token_getter = auth_token_getter
        self._on_unauthorized = on_unauthorized
        self._http = http or HttpOptions()
        self.dataspace = dataspace or "default"
        self.workload = workload
        self.user_agent = user_agent
        self.default_settings: Dict[str, str] = dict(query_settings) if query_settings else {}
        self._base_url = f"{self.tenant_endpoint}/api/v3/query"

    def _build_user_agent(self) -> str:
        """
        Build the User-Agent header value.

        Format: "salesforce-cdp-connector/{version}", optionally followed by a
        space and the caller-supplied user_agent (e.g. "my-app/1.0"), matching
        the JDBC driver's "salesforce-datacloud-jdbc/{version}" convention. The
        product token is the literal driver name (stable across the PyPI
        rename), not the package name.
        """
        # Local import avoids a circular import: the package __init__ imports
        # this module (via connection.py) while defining __version__.
        from .. import __version__

        user_agent = f"salesforce-cdp-connector/{__version__}"
        if self.user_agent and self.user_agent.strip():
            user_agent = f"{user_agent} {self.user_agent.strip()}"
        return user_agent

    def _get_headers(self, accept: str = _JSON_MEDIA_TYPE) -> Dict[str, str]:
        """Get headers for v3 API requests.

        Args:
            accept: Value for the Accept header. Only execute_query and
                fetch_results override this to negotiate Arrow; every other
                endpoint defaults to JSON.
        """
        token = self.auth_token_getter()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": accept,
            "User-Agent": self._build_user_agent(),
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
        accept: Optional[str] = None,
        auth_refreshed: bool = False,
    ) -> requests.Response:
        """
        Make an HTTP request with retry logic.

        Network errors, 5xx and 429 responses are retried up to MAX_RETRIES
        times, except for POST: the only POST is the query submission, which
        creates a query on the server, so retrying it would run the SQL again
        (and the earlier attempts could not be cancelled). A 401 is the
        exception, since the request was rejected before it ran.

        Args:
            method: HTTP method (GET, POST, DELETE)
            url: Request URL
            params: Query parameters
            json_data: JSON request body
            retry_count: Current retry attempt
            accept: Accept header value. Defaults to JSON when omitted; pass
                the client's Arrow media type explicitly for the data-bearing
                endpoints (execute_query, fetch_results).

        Returns:
            Response object

        Raises:
            Exception: If request fails after all retries
        """
        try:
            response = self._http.session.request(
                method=method,
                url=url,
                headers=self._get_headers(accept=accept or _JSON_MEDIA_TYPE),
                params=params,
                json=json_data,
                timeout=self._http.timeout,
            )
            # Check for errors
            if not response.ok:
                # An expired/revoked token: drop the cached one and retry once
                # with a freshly exchanged token.
                if response.status_code == 401 and self._on_unauthorized is not None and not auth_refreshed:
                    self._on_unauthorized()
                    return self._make_request(
                        method,
                        url,
                        params,
                        json_data,
                        retry_count,
                        accept,
                        auth_refreshed=True,
                    )

                # Don't retry client errors (4xx except 429)
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    self._raise_api_error(response, method)

                # Retry on 5xx and 429 (rate limiting), but never the query POST
                if method != "POST" and retry_count < self.MAX_RETRIES:
                    time.sleep(self.RETRY_WAIT_SECONDS)

                    return self._make_request(
                        method,
                        url,
                        params,
                        json_data,
                        retry_count + 1,
                        accept,
                        auth_refreshed,
                    )

                # All retries exhausted
                self._raise_api_error(response, method)

            return response

        except requests.exceptions.RequestException as e:
            # Network errors - retry if possible (never the query POST)
            if method != "POST" and retry_count < self.MAX_RETRIES:
                time.sleep(self.RETRY_WAIT_SECONDS)
                return self._make_request(
                    method,
                    url,
                    params,
                    json_data,
                    retry_count + 1,
                    accept,
                    auth_refreshed,
                )
            raise OperationalError(f"Request failed: {e}") from e

    def _raise_api_error(self, response: requests.Response, method: str):
        """
        Raise appropriate exception for API error response.

        The server reports a failure in the x-hyperdb-status header (JSON with
        error.error = SQLSTATE, error.message and error.details); the body is
        often empty and is not read. The message follows the JDBC driver:

            Failed to execute query: <server message>
            SQLSTATE: <sqlstate>
            QUERY-ID: <query id>        (only when the server sent one)

        The "Failed to execute query" prefix is only for the query POST; the
        status, row fetch and cancel calls use "Query request failed".

        A response without a usable header gets the generic SQLSTATE HY000 and
        the HTTP status line as its message; the HTTP status still selects the
        exception class.
        """
        header_error = self._error_from_status_header(response)
        if header_error:
            sqlstate = header_error.get("error")
            if not isinstance(sqlstate, str) or not sqlstate:
                sqlstate = _GENERIC_SQLSTATE
            message_text = header_error.get("message")
            reason = (
                message_text
                if isinstance(message_text, str) and message_text
                else f"HTTP {response.status_code} {response.reason}"
            )
            details = header_error.get("details")
            details = details if isinstance(details, dict) and details else None
        else:
            sqlstate, details = _GENERIC_SQLSTATE, None
            reason = f"HTTP {response.status_code} {response.reason}"

        action = "Failed to execute query" if method == "POST" else "Query request failed"
        message = f"{action}: {reason}\nSQLSTATE: {sqlstate}"
        query_id = self._query_id_from_status_header(response)
        if query_id:
            message += f"\nQUERY-ID: {query_id}"

        raise map_http_error_to_exception(response.status_code, message, sqlstate=sqlstate, details=details)

    @staticmethod
    def _status_header(response: requests.Response) -> Optional[Dict[str, Any]]:
        """Return the parsed x-hyperdb-status header, or None if absent or not a JSON object."""
        header = response.headers.get("x-hyperdb-status")
        if not header:
            return None
        try:
            parsed = json.loads(header)
        except ValueError:
            return None
        return parsed if isinstance(parsed, dict) else None

    @classmethod
    def _error_from_status_header(cls, response: requests.Response) -> Optional[Dict[str, Any]]:
        """Return the "error" object of the x-hyperdb-status header, or None if absent or malformed."""
        status = cls._status_header(response)
        error = status.get("error") if status else None
        return error if isinstance(error, dict) else None

    @classmethod
    def _query_id_from_status_header(cls, response: requests.Response) -> Optional[str]:
        status = cls._status_header(response)
        query_id = status.get("queryId") if status else None
        return query_id if isinstance(query_id, str) and query_id else None

    @staticmethod
    def _param_entry(value: Any) -> Dict[str, Any]:
        """Build one v3 parameter entry: {"type": <lowercase>, "value": ...}."""
        return {"type": infer_sql_parameter_type(value).lower(), "value": value}

    def _bind_parameters(self, sql: str, params: Optional[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
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
                raise ProgrammingError(f"No value supplied for named parameter ':{name}'")
            sql_params.append(self._param_entry(params[name]))
            return "?"

        # re.sub invokes _replace left-to-right, so sql_params ends up in SQL order.
        new_sql = _NAMED_PARAM_RE.sub(_replace, sql)
        return new_sql, sql_params

    def _merge_settings(self, settings: Optional[Dict[str, str]]) -> Dict[str, str]:
        """
        Merge connection-level default settings with per-call settings.

        Per-call settings win on key collision. Values must be strings, matching
        the server's Map<String,String> contract (same shape as the JDBC driver's
        gRPC QueryParam.settings).

        Raises:
            ProgrammingError: If any setting value is not a string
        """
        merged = {**self.default_settings, **(settings or {})}
        for key, value in merged.items():
            if not isinstance(value, str):
                raise ProgrammingError(f"Query setting '{key}' must be a string, got {type(value).__name__}")
        return merged

    def execute_query(
        self,
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, str]] = None,
    ) -> QueryResponse:
        """
        Execute a SQL query via POST /api/v3/query.

        Args:
            sql: SQL query string
            parameters: Named parameters dict (e.g., {"status": "Active"})
            settings: Per-call query settings (e.g. {"time_zone": "UTC"}), merged
                over the connection-level defaults; per-call wins on collision

        Returns:
            QueryResponse with initial results and status

        Raises:
            ProgrammingError: For SQL syntax errors (400) or a non-string setting value
            OperationalError: For auth/network failures (401, 403, 500+)
        """
        # v3 accepts only positional (qmark) parameters; rewrite any :name
        # placeholders and build the positional array in SQL order.
        sql, sql_params = self._bind_parameters(sql, parameters)

        merged_settings = self._merge_settings(settings)

        request_body = {
            "sql": sql,
            "transferMode": "ADAPTIVE",
        }

        if sql_params:
            request_body["parameters"] = sql_params

        if merged_settings:
            request_body["settings"] = merged_settings

        response = self._make_request("POST", self._base_url, json_data=request_body, accept=_ARROW_MEDIA_TYPE)

        # Parse status from x-hyperdb-status header (v3) first. The header carries
        # the queryId/rowCount/completionStatus the cursor relies on, and must be
        # read before the body: under the ADAPTIVE transfer mode this client always
        # sends, a query that hasn't produced rows yet still gets a 200 with this
        # header set but a zero-length Arrow body (QueryV3ResponseArrowHttpMessageConverter
        # writes `new byte[0]` when there's no Hyper binary part yet).
        status_header = response.headers.get("x-hyperdb-status")
        if not status_header:
            raise OperationalError("Missing x-hyperdb-status header in query response")

        status_data = json.loads(status_header)

        # Parse response body. A zero-length Arrow body means Hyper hasn't produced
        # any rows yet (still running) rather than a malformed response -- unlike an
        # empty-but-valid IPC stream (schema message present, zero rows), which
        # from_arrow_bytes handles on its own.
        if not response.content:
            query_response = QueryResponse(data=[], metadata=[], returned_rows=0)
        else:
            query_response = QueryResponse.from_arrow_bytes(response.content)

        query_response.status = QueryStatus.from_dict(status_data)

        return query_response

    def get_query_status(self, query_id: str, wait_time_ms: Optional[int] = None) -> QueryStatus:
        """
        Get query status via GET /api/v3/query/{queryId}.

        Args:
            query_id: Query ID from execute_query
            wait_time_ms: Milliseconds to wait before returning (long-polling, max 10000)

        Returns:
            QueryStatus object (parsed from the JSON response body)

        Raises:
            OperationalError: For network failures
        """
        url = f"{self._base_url}/{query_id}"
        params = {}

        # Use long-polling to reduce API calls
        if wait_time_ms is not None:
            params["waitTimeMs"] = min(wait_time_ms, self.MAX_WAIT_TIME_MS)

        response = self._make_request("GET", url, params=params)

        # Unlike execute_query's POST, this endpoint never sets x-hyperdb-status
        # (QueryV3Controller.getQueryStatusV3 returns the status as the JSON
        # body directly); parse the body instead of looking for the header.
        return QueryStatus.from_dict(response.json())

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
            response = self._make_request("GET", url, params=params, accept=_ARROW_MEDIA_TYPE)
            # Same zero-length-body case as execute_query: under ADAPTIVE
            # transfer mode, a chunk that hasn't produced any Arrow binary
            # part yet still comes back 200 with an empty body rather than
            # a parseable (even if empty) IPC stream.
            if not response.content:
                return QueryResponse(data=[], metadata=[], returned_rows=0)
            return QueryResponse.from_arrow_bytes(response.content)
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
                raise OperationalError(f"Query timed out after {timeout_seconds} seconds")

            # Long-polling with waitTimeMs means we don't need additional sleep
            # The server will wait up to waitTimeMs before responding
