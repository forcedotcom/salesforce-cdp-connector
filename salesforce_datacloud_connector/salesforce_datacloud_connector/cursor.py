"""
DB-API 2.0 Cursor implementation for Salesforce Data Cloud.

The Cursor class executes queries and manages result retrieval.
"""

from typing import Any, Dict, List, Optional, Tuple

from .api.client import DataCloudQueryClient
from .api.models import ColumnMetadata
from .exceptions import InterfaceError, NotSupportedError
from .types import build_description_tuple, convert_datacloud_value


class Cursor:
    """
    DB-API 2.0 Cursor for executing queries and fetching results.

    The cursor manages query execution, result pagination, and type conversion.
    Results are buffered in memory one chunk at a time to minimize memory usage.
    """

    # Cursor attributes per DB-API 2.0
    arraysize = 1  # Default number of rows to fetch with fetchmany()

    def __init__(self, client: DataCloudQueryClient):
        """
        Initialize cursor.

        Args:
            client: API client for executing queries
        """
        self._client = client
        self._query_id: Optional[str] = None
        self._metadata: List[ColumnMetadata] = []
        self._buffer: List[List[Any]] = []  # Current chunk buffer
        self._buffer_offset: int = 0  # Position in current buffer
        self._total_row_count: int = 0  # Total rows in result set
        self._fetched_rows: int = 0  # Total rows fetched from server
        self._description: Optional[List[Tuple]] = None
        self._rowcount: int = -1  # DB-API 2.0: -1 for SELECT, else number of rows affected
        self._closed: bool = False

    @property
    def description(self) -> Optional[List[Tuple]]:
        """
        DB-API 2.0 cursor.description attribute.

        Returns a list of 7-element tuples describing result columns:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)

        Returns:
            List of column description tuples, or None if no query executed
        """
        return self._description

    @property
    def rowcount(self) -> int:
        """
        DB-API 2.0 cursor.rowcount attribute.

        For SELECT queries, returns -1 (per DB-API 2.0 spec).
        For DML queries (not supported in V1), would return affected rows.

        Returns:
            -1 for SELECT queries
        """
        return self._rowcount

    def _check_closed(self):
        """Raise InterfaceError if cursor is closed."""
        if self._closed:
            raise InterfaceError("Cursor is closed")

    def _check_query_executed(self):
        """Raise InterfaceError if no query has been executed."""
        if self._query_id is None:
            raise InterfaceError("No query has been executed")

    def _build_description(self):
        """Build cursor.description from metadata."""
        if not self._metadata:
            self._description = None
        else:
            self._description = [
                build_description_tuple(col.__dict__) for col in self._metadata
            ]

    def _fetch_next_chunk(self):
        """
        Fetch the next chunk of results from the server.

        Updates _buffer, _buffer_offset, and _fetched_rows.
        """
        # Calculate offset for next chunk
        offset = self._fetched_rows

        # Check if we've already fetched all rows
        if offset >= self._total_row_count:
            self._buffer = []
            self._buffer_offset = 0
            return

        # Fetch chunk from server
        response = self._client.fetch_results(
            query_id=self._query_id,
            offset=offset,
            row_limit=1000000,  # Large limit, server determines actual chunk
            omit_schema=True,  # We already have metadata
        )

        # Update buffer
        self._buffer = response.data
        self._buffer_offset = 0
        self._fetched_rows += len(response.data)

    def _convert_row(self, row: List[Any]) -> Tuple[Any, ...]:
        """
        Convert a row from API format to Python types.

        Args:
            row: Raw row data from API

        Returns:
            Tuple of converted values
        """
        converted = []
        for i, value in enumerate(row):
            if i < len(self._metadata):
                col = self._metadata[i]
                converted_value = convert_datacloud_value(
                    value, col.type, col.precision, col.scale
                )
                converted.append(converted_value)
            else:
                converted.append(value)
        return tuple(converted)

    def execute(
        self, operation: str, parameters: Optional[Dict[str, Any]] = None
    ) -> "Cursor":
        """
        Execute a SQL query.

        Args:
            operation: SQL query string (may contain :param placeholders)
            parameters: Named parameters dict (e.g., {"param": "value"})

        Returns:
            Self (allows chaining)

        Raises:
            ProgrammingError: For SQL syntax errors
            NotSupportedError: For unsupported operations (DML/DDL in V1)
            InterfaceError: If cursor is closed
        """
        self._check_closed()

        # Check for unsupported operations (V1 is read-only)
        operation_upper = operation.strip().upper()
        if any(
            operation_upper.startswith(cmd)
            for cmd in ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE")
        ):
            raise NotSupportedError(
                "V1 driver is read-only. DML/DDL operations are not supported."
            )

        # Execute query
        response = self._client.execute_query(operation, parameters)

        # Store metadata and query ID
        self._query_id = response.status.query_id
        self._metadata = response.metadata
        self._total_row_count = response.status.row_count
        self._rowcount = -1  # SELECT queries return -1
        self._build_description()

        # Handle sync vs async responses
        if response.status.is_complete():
            # Synchronous response - data is already available
            self._buffer = response.data
            self._buffer_offset = 0
            self._fetched_rows = len(response.data)
        else:
            # Asynchronous response - need to poll until complete
            final_status = self._client.poll_until_complete(self._query_id)
            self._total_row_count = final_status.row_count

            # Fetch first chunk
            self._fetch_next_chunk()

        return self

    def executemany(self, operation: str, seq_of_parameters: List[Dict[str, Any]]):
        """
        Execute a query multiple times with different parameters.

        Note: This is not optimized in V1 - it simply calls execute() in a loop.

        Args:
            operation: SQL query string
            seq_of_parameters: List of parameter dicts

        Raises:
            NotSupportedError: V1 does not optimize executemany
        """
        self._check_closed()

        # V1: Simple implementation (not optimized)
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """
        Fetch the next row from the result set.

        Returns:
            Tuple of column values, or None if no more rows

        Raises:
            InterfaceError: If no query has been executed or cursor is closed
        """
        self._check_closed()
        self._check_query_executed()

        # Check if we need to fetch next chunk
        if self._buffer_offset >= len(self._buffer):
            # Current buffer exhausted, try to fetch next chunk
            if self._fetched_rows < self._total_row_count:
                self._fetch_next_chunk()
            else:
                # No more data
                return None

        # Check if buffer is empty (no more data)
        if not self._buffer:
            return None

        # Get row from buffer
        row = self._buffer[self._buffer_offset]
        self._buffer_offset += 1

        # Convert and return
        return self._convert_row(row)

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        """
        Fetch the next set of rows from the result set.

        Args:
            size: Number of rows to fetch (default: cursor.arraysize)

        Returns:
            List of row tuples

        Raises:
            InterfaceError: If no query has been executed or cursor is closed
        """
        self._check_closed()
        self._check_query_executed()

        if size is None:
            size = self.arraysize

        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    def fetchall(self) -> List[Tuple[Any, ...]]:
        """
        Fetch all remaining rows from the result set.

        Warning: For large result sets, this may consume significant memory.

        Returns:
            List of all remaining row tuples

        Raises:
            InterfaceError: If no query has been executed or cursor is closed
        """
        self._check_closed()
        self._check_query_executed()

        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    def close(self):
        """
        Close the cursor.

        The cursor is unusable after this call.
        """
        if not self._closed:
            # Clean up resources
            self._buffer = []
            self._metadata = []
            self._query_id = None
            self._closed = True

    def cancel(self):
        """
        Cancel the currently executing query (extension method).

        This is an extension to DB-API 2.0.

        Raises:
            InterfaceError: If no query has been executed or cursor is closed
        """
        self._check_closed()
        self._check_query_executed()

        self._client.cancel_query(self._query_id)

    def fetch_df(self):
        """
        Fetch all results as a pandas DataFrame (extension method).

        This is an extension to DB-API 2.0.

        Returns:
            pandas.DataFrame with query results

        Raises:
            ImportError: If pandas is not installed
            InterfaceError: If no query has been executed or cursor is closed
        """
        self._check_closed()
        self._check_query_executed()

        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is required for fetch_df(). "
                "Install with: pip install salesforce-datacloud[pandas]"
            ) from e

        # Fetch all rows
        rows = self.fetchall()

        # Get column names from description
        if self._description:
            columns = [desc[0] for desc in self._description]
        else:
            columns = None

        # Create DataFrame
        return pd.DataFrame(rows, columns=columns)

    def __iter__(self):
        """
        Allow iteration over cursor results.

        Example:
            for row in cursor:
                print(row)
        """
        return self

    def __next__(self) -> Tuple[Any, ...]:
        """
        Get next row for iteration.

        Raises:
            StopIteration: When no more rows
        """
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
