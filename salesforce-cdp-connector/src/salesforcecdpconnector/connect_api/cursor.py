"""
Connect API Cursor implementation for Salesforce CDP.
"""

import time
from typing import Optional, Union, Sequence, Dict, Any, List, Tuple
from loguru import logger

from ..dbapi import BaseCursor
from .client import Client
from ..types import get_type_object
from ..constants import DEFAULT_ROW_LIMIT
from ..exceptions import ApiError, QueryError, AuthenticationError

QUERY_STATE_IN_PROGRESS = set(['Unspecified', 'Queued', 'Running'])
QUERY_STATE_FINISHED = set(['Completed'])
QUERY_STATE_FAILED = set(['Failed'])

class Cursor(BaseCursor):
    """Connect API (REST) Cursor implementation for Salesforce CDP."""

    def __init__(self, client: Client, connection):
        super().__init__(connection)
        self._client = client
        self.arraysize = DEFAULT_ROW_LIMIT

        self._query_id: Optional[str] = None
        self._metadata: Optional[List[Dict[str, Any]]] = None
        self._description: Optional[List[Tuple]] = None
        self._rows_buffer: List[Tuple] = []
        self._row_index: int = 0
        self._next_offset: int = 0
        self._query_finished: bool = True
        self._rowcount: int = -1

    def _clear_state(self):
        """Reset cursor state before executing a new query."""
        logger.debug("Clearing cursor state.")
        self._query_id = None
        self._metadata = None
        self._description = None
        self._rows_buffer = []
        self._row_index = 0
        self._next_offset = 0
        self._query_finished = True
        self._rowcount = -1

    @property
    def description(self) -> Optional[List[Tuple]]:
        """Read-only attribute sequence of 7-item sequences."""
        self._check_closed()
        return self._description

    @property
    def rowcount(self) -> int:
        """Read-only attribute specifying the number of rows affected/fetched."""
        self._check_closed()
        return self._rowcount

    def close(self) -> None:
        """Close the cursor."""
        if self._is_closed:
            return
        logger.debug("Closing cursor.")
        self._is_closed = True
        self._clear_state()
        self._client = None
        self._connection = None

    def execute(self, operation: str, parameters: Optional[Union[Sequence, Dict[str, Any]]] = None) -> None:
        """Prepare and execute a database operation (query or command)."""
        self._check_closed()
        self._clear_state()

        try:
            logger.debug(f"Executing SQL: {operation[:150]}{'...' if len(operation) > 150 else ''}")
            if parameters:
                logger.debug(f"With parameters: {parameters}")

            # submit query
            response = self._client.submit_query(operation, parameters)

            status_info = response.get('status', {})
            query_id = status_info.get('queryId')
            

            if self._query_id != query_id:
                self._query_id = query_id
                logger.info(f"New Query ID: {self._query_id}")

            if not self._query_id:
                 raise self._connection.InternalError("API did not return a queryId.")

            query_state = status_info.get('completionStatus', 'Unspecified').upper()
            logger.info(f"Query State: {query_state} from status_info: {status_info}")

            if query_state in QUERY_STATE_FAILED:
                 error_msg = status_info.get('error', 'Query failed without specific error message.')
                 raise self._connection.QueryError(f"Query execution failed: {error_msg} (Query ID: {self._query_id})")

            self._metadata = response.get('metadata')
            self._parse_description()

            initial_data = response.get('data', [])
            if initial_data:
                self._rows_buffer.extend(self._data_to_tuples(initial_data))
            else:
                logger.info(f"No initial data returned for query ID: {self._query_id}")

            returned_rows = response.get('returnedRows', len(initial_data))
            total_rows = response.get('totalRows')
            next_offset = response.get('nextOffset')
            
            self._next_offset = returned_rows

            # Determine if query is finished based on multiple indicators
            self._query_finished = False
            
            if query_state in QUERY_STATE_FINISHED:
                self._query_finished = True
                logger.debug("Query marked as finished based on state")
            self._rowcount = -1
            if self._query_finished:
                self._rowcount = len(self._rows_buffer)
                logger.debug(f"Query finished, rowcount set to {self._rowcount}")

        except (ApiError, QueryError, AuthenticationError) as e:
            if isinstance(e, AuthenticationError):
                raise self._connection.AuthenticationError(str(e)) from e
            elif isinstance(e, QueryError):
                raise self._connection.QueryError(str(e)) from e
            else:
                 raise self._connection.OperationalError(str(e)) from e
        except Exception as e:
            logger.exception("Unexpected error during execute.")
            raise self._connection.InterfaceError(f"Unexpected error during execute: {e}") from e

    def _parse_description(self):
        """Parses metadata from API into PEP 249 description."""
        if not self._metadata:
            self._description = None
            return

        desc = []
        try:
            for col_meta in self._metadata:
                name = col_meta.get('name')
                type_name = col_meta.get('type', 'TEXT')
                type_code = get_type_object(type_name)

                display_size = None
                internal_size = None
                precision = col_meta.get('precision')
                scale = col_meta.get('scale')
                null_ok = col_meta.get('nullable', True)

                desc.append((name, type_code, display_size, internal_size, precision, scale, null_ok))
            self._description = desc
        except Exception as e:
            logger.error(f"Failed to parse metadata into description: {e}")
            self._description = None

    def _data_to_tuples(self, data: List[Union[List, Dict]]) -> List[Tuple]:
        """Converts API data rows (list of lists or list of dicts) to tuples."""
        if not data:
            return []

        if isinstance(data[0], dict):
            if not self.description:
                logger.warning("Cannot reliably convert dict rows to tuples without description. Column order may be wrong.")
                ordered_keys = list(data[0].keys())
                return [tuple(row.get(key) for key in ordered_keys) for row in data]
            else:
                ordered_keys = [desc[0] for desc in self.description]
                return [tuple(row.get(key) for key in ordered_keys) for row in data]
        elif isinstance(data[0], list):
            return [tuple(row) for row in data]
        else:
            logger.warning(f"Unexpected data format received: {type(data[0])}. Returning empty list.")
            return []

    def _fetch_more_data(self) -> bool:
        """Internal helper to fetch next chunk of data from API."""
        if self._query_finished:
            return False

        if not self._query_id:
            raise self._connection.InternalError("Cannot fetch data, query ID is missing.")


        try:
            logger.debug(f"Fetching next chunk. Offset: {self._next_offset}, Limit: {self.arraysize}")
            response = self._client.get_query_results(self._query_id, self._next_offset, self.arraysize)
            new_data = response.get('data', [])
            
            returned_rows = response.get('returnedRows', len(new_data))

            if not new_data or returned_rows == 0:
                logger.debug("Received empty data chunk, assuming query finished.")
                self._query_finished = True
                self._rowcount = self._row_index + len(self._rows_buffer)
                return False

            self._rows_buffer.extend(self._data_to_tuples(new_data))
            self._next_offset += returned_rows

            total_rows = response.get('totalRows')
            next_offset = response.get('nextOffset')
            
            if next_offset is None or next_offset < 0:
                logger.debug("Inferred query finished: no nextOffset or negative nextOffset")
                self._query_finished = True
                self._rowcount = self._row_index + len(self._rows_buffer)
            elif total_rows is not None and self._next_offset >= total_rows:
                logger.debug(f"Inferred query finished: next offset {self._next_offset} >= total rows {total_rows}")
                self._query_finished = True
                self._rowcount = self._row_index + len(self._rows_buffer)
            elif returned_rows < self.arraysize:
                logger.debug(f"Inferred query finished: returned {returned_rows} rows, less than limit {self.arraysize}")
                self._query_finished = True
                self._rowcount = self._row_index + len(self._rows_buffer)

            return True

        except (ApiError, QueryError, AuthenticationError) as e:
            error_msg = str(e).lower()
            if "out of range" in error_msg or "offset" in error_msg and "available" in error_msg:
                logger.debug(f"Offset out of range error detected: {e}. Marking query as finished.")
                self._query_finished = True
                self._rowcount = self._row_index + len(self._rows_buffer)
                return False
            
            if isinstance(e, AuthenticationError): 
                raise self._connection.AuthenticationError(str(e)) from e
            elif isinstance(e, QueryError): 
                raise self._connection.QueryError(str(e)) from e
            else: 
                raise self._connection.OperationalError(str(e)) from e
        except Exception as e:
            logger.exception("Unexpected error during _fetch_more_data.")
            raise self._connection.InterfaceError(f"Unexpected error fetching data: {e}") from e

    def fetchone(self) -> Optional[Tuple]:
        """Fetch the next row of a query result set."""
        self._check_closed()

        if self._row_index < len(self._rows_buffer):
            row = self._rows_buffer[self._row_index]
            self._row_index += 1
            return row
        else:
            if not self._query_finished:
                if self._fetch_more_data():
                    if self._row_index < len(self._rows_buffer):
                         row = self._rows_buffer[self._row_index]
                         self._row_index += 1
                         return row
                    else:
                         logger.warning("Fetch succeeded but buffer still empty.")
                         self._query_finished = True
                         return None
                else:
                    return None
            else:
                 return None

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple]:
        """Fetch the next set of rows of a query result."""
        self._check_closed()
        fetch_size = size if size is not None else self.arraysize
        if fetch_size <= 0:
            return []

        results = []
        while len(results) < fetch_size:
            available_in_buffer = len(self._rows_buffer) - self._row_index
            needed = fetch_size - len(results)
            fetch_from_buffer = min(available_in_buffer, needed)

            if fetch_from_buffer > 0:
                end_index = self._row_index + fetch_from_buffer
                results.extend(self._rows_buffer[self._row_index:end_index])
                self._row_index = end_index

            if len(results) < fetch_size and not self._query_finished:
                if not self._fetch_more_data():
                    break
            elif len(results) >= fetch_size:
                 break
            elif self._query_finished:
                 break

        return results

    def fetchall(self) -> List[Tuple]:
        """Fetch all remaining rows of a query result."""
        if not self._query_id:
            raise self._connection.InternalError("Cannot fetch data, query ID is missing. Call execute() first.")

        self._check_closed()

        results = self._rows_buffer[self._row_index:]
        self._row_index = len(self._rows_buffer)

        while not self._query_finished:
            if not self._fetch_more_data():
                break
            results.extend(self._rows_buffer[self._row_index:])
            self._row_index = len(self._rows_buffer)

        return results

    def executemany(self, operation: str, seq_of_parameters: Sequence) -> None:
        """Execute a command against a sequence of parameters."""
        self._check_closed()
        logger.warning("executemany called: Executing queries individually.")
        count = 0
        for params in seq_of_parameters:
            self.execute(operation, params)
            count += 1
        self._rowcount = -1
        logger.info(f"executemany completed {count} operations.")

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        """Does nothing by default."""
        self._check_closed()
        pass

    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """Does nothing by default."""
        self._check_closed()
        pass

    def nextset(self) -> Optional[bool]:
        """Skip to the next available set, discarding any remaining rows from the current set."""
        self._check_closed()
        return None

    def callproc(self, procname: str, parameters: Optional[Sequence[Any]] = None) -> Optional[Sequence[Any]]:
        """Call a stored procedure with the given name."""
        self._check_closed()
        raise self._connection.NotSupportedError("Stored procedures not supported by Salesforce CDP.")

    def __iter__(self):
        """Return self to make cursor compatible with Python iteration protocol."""
        self._check_closed()
        return self

    def __next__(self):
        """Return the next row or raise StopIteration when exhausted."""
        self._check_closed()
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self):
        """Enter context manager."""
        self._check_closed()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.close() 