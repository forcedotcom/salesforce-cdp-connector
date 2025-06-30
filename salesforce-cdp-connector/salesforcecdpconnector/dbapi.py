import time
from typing import Optional, Union, Sequence, Dict, Any, List, Tuple
from loguru import logger

from .client import SalesforceCDPClient
from .auth import PasswordGrantAuth, ClientCredentialsAuth
from .exceptions import (
    Warning, Error, InterfaceError, DatabaseError, DataError,
    OperationalError, IntegrityError, InternalError, ProgrammingError,
    NotSupportedError, QueryError, AuthenticationError, ApiError
)
from .types import get_type_object, STRING, NUMBER, DATETIME, BINARY, ROWID
from .constants import DEFAULT_ROW_LIMIT, DEFAULT_API_VERSION, DEFAULT_TOKEN_TIMEOUT_SECONDS

# PEP 249 Module Globals
apilevel = "2.0"
threadsafety = 1 # Threads may share the module, but not connections. Check Salesforce API concurrency limits.
paramstyle = 'named'

class Connection:
    """PEP 249 Connection object for Salesforce CDP."""

    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    # Custom errors re-exported
    AuthenticationError = AuthenticationError
    QueryError = QueryError
    ApiError = ApiError

    def __init__(self, client: SalesforceCDPClient):
        self._client = client
        self._is_closed = False

    def _check_closed(self):
        if self._is_closed:
            raise self.Error("Connection is closed.")

    def close(self) -> None:
        """Close the connection now."""
        if self._is_closed:
            return
        logger.info("Closing Salesforce CDP connection.")
        self._is_closed = True
        # Clean up resources if needed (e.g., close session in client/auth)
        if hasattr(self._client, 'session') and hasattr(self._client.session, 'close'):
            self._client.session.close()
        self._client = None

    def commit(self) -> None:
        """Commit any pending transaction to the database."""
        self._check_closed()
        # Salesforce CDP API likely auto-commits, so this is a no-op.
        logger.debug("Commit called (no-op for Salesforce CDP).")
        pass

    def rollback(self) -> None:
        """Roll back to the start of any pending transaction."""
        self._check_closed()
        # Salesforce CDP API does not support transactions in the traditional sense.
        logger.warning("Rollback called (not supported by Salesforce CDP).")
        raise self.NotSupportedError("Salesforce CDP does not support rollback.")

    def cursor(self) -> 'Cursor':
        """Return a new Cursor Object using the connection."""
        self._check_closed()
        logger.debug("Creating new cursor.")
        # Pass the client instance to the cursor
        return Cursor(self._client, self) # Pass connection for error reporting

    def __enter__(self):
        """Enter context manager."""
        self._check_closed()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        # Don't rollback on error, as it's not supported
        # Always close the connection
        self.close()


class Cursor:
    """PEP 249 Cursor object for Salesforce CDP."""

    def __init__(self, client: SalesforceCDPClient, connection: Connection):
        self._client = client
        self._connection = connection # Keep ref for raising connection errors
        self.arraysize = DEFAULT_ROW_LIMIT # Default number of rows to fetch with fetchmany

        self._query_id: Optional[str] = None
        self._metadata: Optional[List[Dict[str, Any]]] = None
        self._description: Optional[List[Tuple]] = None
        self._rows_buffer: List[Tuple] = [] # Buffer for fetched rows (as tuples)
        self._row_index: int = 0 # Current position within _rows_buffer
        self._next_offset: int = 0 # Offset for the *next* API call to fetch results
        self._query_finished: bool = True # Assume finished until a query starts
        self._rowcount: int = -1 # -1 means row count is unknown or not applicable
        self._is_closed: bool = False

    def _check_closed(self):
        if self._is_closed:
            raise self._connection.Error("Cursor is closed.")
        self._connection._check_closed() # Also check connection

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
        # PEP 249: -1 if the number of rows is unknown or not applicable.
        # For SELECT, this is often -1 until all rows are fetched.
        # For DML, it *should* be the number of rows affected, but CDP API might not return this.
        # Let's stick to -1 for SELECTs until fetchall completes.
        return self._rowcount

    def close(self) -> None:
        """Close the cursor."""
        if self._is_closed:
            return
        logger.debug("Closing cursor.")
        self._is_closed = True
        self._clear_state()
        self._client = None # type: ignore
        self._connection = None # type: ignore

    def execute(self, operation: str, parameters: Optional[Union[Sequence, Dict[str, Any]]] = None) -> None:
        """Prepare and execute a database operation (query or command)."""
        self._check_closed()
        self._clear_state()

        try:
            logger.debug(f"Executing SQL: {operation[:150]}{'...' if len(operation) > 150 else ''}")
            if parameters:
                logger.debug(f"With parameters: {parameters}")

            response = self._client.submit_query(operation, parameters)
            logger.debug(f"Response: {response}")
            # Example response structure (adjust based on actual API):
            # {
            #   "metadata": [{"name": "col1", "type": "TEXT"}, {"name": "col2", "type": "NUMBER"}],
            #   "status": {"queryId": "xyz", "state": "Running | Finished | Failed"},
            #   "data": [[val1, val2], [val3, val4]], # Or list of dicts
            #   "returnedRows": 2,
            #   "totalRows": 10, # Optional
            #   "nextOffset": 2 # Optional, indicates more data
            # }

            status_info = response.get('status', {})
            self._query_id = status_info.get('queryId')
            if not self._query_id:
                 raise self._connection.InternalError("API did not return a queryId.")

            query_state = status_info.get('completionStatus', 'Unspecified').upper()
            logger.debug(f"Query ID: {self._query_id}, Initial State: {query_state}")

            if query_state == 'FAILED':
                 # Try to get error details if available
                 error_msg = status_info.get('error', 'Query failed without specific error message.')
                 raise self._connection.QueryError(f"Query execution failed: {error_msg} (Query ID: {self._query_id})")

            self._metadata = response.get('metadata')
            self._parse_description() # Sets self._description

            initial_data = response.get('data', [])
            # Convert initial data to tuples
            self._rows_buffer.extend(self._data_to_tuples(initial_data))

            returned_rows = response.get('returnedRows', len(initial_data))
            total_rows = response.get('totalRows')
            next_offset = response.get('nextOffset')
            
            # Set the next offset for potential future fetches
            self._next_offset = returned_rows

            # Determine if query is finished based on multiple indicators
            self._query_finished = False
            
            # Check if query state indicates finished
            if query_state == 'FINISHED':
                self._query_finished = True
                logger.debug("Query marked as finished based on state")
            
            # Check if we have all the data (totalRows matches returnedRows)
            elif total_rows is not None and returned_rows >= total_rows:
                self._query_finished = True
                logger.debug(f"Query marked as finished: returned {returned_rows} rows, total {total_rows} rows")
            
            # Check if nextOffset indicates no more data
            elif next_offset is None or next_offset < 0:
                self._query_finished = True
                logger.debug("Query marked as finished: no nextOffset or negative nextOffset")
            
            # Check if returned rows is less than the limit (indicating last page)
            elif returned_rows < self.arraysize:
                self._query_finished = True
                logger.debug(f"Query marked as finished: returned {returned_rows} rows, less than limit {self.arraysize}")

            # Rowcount: For non-SELECT, maybe API gives affected count? Default to -1.
            # For SELECT, set to -1 until fetchall or query finishes.
            self._rowcount = -1
            if self._query_finished:
                # If finished immediately, rowcount is known
                self._rowcount = len(self._rows_buffer) # Or use totalRows if reliable
                logger.debug(f"Query finished, rowcount set to {self._rowcount}")

        except (ApiError, QueryError, AuthenticationError) as e:
            # Re-raise errors using the connection's error hierarchy
            if isinstance(e, AuthenticationError):
                raise self._connection.AuthenticationError(str(e)) from e
            elif isinstance(e, QueryError):
                raise self._connection.QueryError(str(e)) from e
            else: # General ApiError maps to OperationalError
                 raise self._connection.OperationalError(str(e)) from e
        except Exception as e:
            # Catch unexpected errors
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
                type_name = col_meta.get('type', 'TEXT') # Default type if missing
                type_code = get_type_object(type_name)

                # These values are often hard to get from APIs. Provide defaults.
                display_size = None # Or some sensible default based on type
                internal_size = None # Or based on type
                precision = col_meta.get('precision') # If available
                scale = col_meta.get('scale') # If available
                null_ok = col_meta.get('nullable', True) # Assume nullable if not specified

                desc.append((name, type_code, display_size, internal_size, precision, scale, null_ok))
            self._description = desc
        except Exception as e:
            logger.error(f"Failed to parse metadata into description: {e}")
            self._description = None # Indicate failure
            # Optionally raise an InterfaceError here

    def _data_to_tuples(self, data: List[Union[List, Dict]]) -> List[Tuple]:
        """Converts API data rows (list of lists or list of dicts) to tuples."""
        if not data:
            return []

        # If data is list of dicts, use description to order columns
        if isinstance(data[0], dict):
            if not self.description:
                logger.warning("Cannot reliably convert dict rows to tuples without description. Column order may be wrong.")
                # Fallback: use keys from the first dict, hoping order is consistent
                ordered_keys = list(data[0].keys())
                return [tuple(row.get(key) for key in ordered_keys) for row in data]
            else:
                # Use description order
                ordered_keys = [desc[0] for desc in self.description]
                return [tuple(row.get(key) for key in ordered_keys) for row in data]
        # If data is list of lists, assume order is correct
        elif isinstance(data[0], list):
            return [tuple(row) for row in data]
        else:
            logger.warning(f"Unexpected data format received: {type(data[0])}. Returning empty list.")
            return []


    def _fetch_more_data(self) -> bool:
        """Internal helper to fetch next chunk of data from API."""
        if self._query_finished:
            return False # No more data expected

        if not self._query_id:
            raise self._connection.InternalError("Cannot fetch data, query ID is missing.")

        try:
            logger.debug(f"Fetching next chunk. Offset: {self._next_offset}, Limit: {self.arraysize}")
            response = self._client.get_query_results(self._query_id, self._next_offset, self.arraysize)

            # Example response structure:
            # {
            #   "data": [...],
            #   "returnedRows": N,
            #   "nextOffset": M, # Might be absent if finished
            #   "totalRows": T # Optional
            # }
            # Status might need a separate check if not included here

            new_data = response.get('data', [])
            returned_rows = response.get('returnedRows', len(new_data))

            if not new_data or returned_rows == 0:
                logger.debug("Received empty data chunk, assuming query finished.")
                self._query_finished = True
                self._rowcount = self._row_index + len(self._rows_buffer) # Update based on *all* fetched rows so far
                return False

            self._rows_buffer.extend(self._data_to_tuples(new_data))
            self._next_offset += returned_rows # Update offset for the *next* call

            # Check if this chunk was the last one
            total_rows = response.get('totalRows')
            next_offset = response.get('nextOffset')
            
            # Multiple indicators that the query is finished
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

            return True # Data was fetched

        except (ApiError, QueryError, AuthenticationError) as e:
            # Check if this is an offset out of range error
            error_msg = str(e).lower()
            if "out of range" in error_msg or "offset" in error_msg and "available" in error_msg:
                logger.debug(f"Offset out of range error detected: {e}. Marking query as finished.")
                self._query_finished = True
                self._rowcount = self._row_index + len(self._rows_buffer)
                return False
            
            # Re-raise using connection's hierarchy
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

        # Check if there are rows remaining in the buffer
        if self._row_index < len(self._rows_buffer):
            row = self._rows_buffer[self._row_index]
            self._row_index += 1
            return row
        else:
            # Buffer is exhausted, try fetching more data if query not finished
            if not self._query_finished:
                if self._fetch_more_data():
                    # Data was fetched, check buffer again
                    if self._row_index < len(self._rows_buffer):
                         row = self._rows_buffer[self._row_index]
                         self._row_index += 1
                         return row
                    else:
                        # Fetch succeeded but returned 0 rows unexpectedly? Mark finished.
                         logger.warning("Fetch succeeded but buffer still empty.")
                         self._query_finished = True
                         return None
                else:
                    # Fetch attempted but no more data was available
                    return None
            else:
                 # Query is finished and buffer is empty
                 return None


    def fetchmany(self, size: Optional[int] = None) -> List[Tuple]:
        """Fetch the next set of rows of a query result."""
        self._check_closed()
        fetch_size = size if size is not None else self.arraysize
        if fetch_size <= 0:
            return []

        results = []
        while len(results) < fetch_size:
            # Fill from buffer first
            available_in_buffer = len(self._rows_buffer) - self._row_index
            needed = fetch_size - len(results)
            fetch_from_buffer = min(available_in_buffer, needed)

            if fetch_from_buffer > 0:
                end_index = self._row_index + fetch_from_buffer
                results.extend(self._rows_buffer[self._row_index:end_index])
                self._row_index = end_index

            # If more rows are needed and query not finished, fetch from API
            if len(results) < fetch_size and not self._query_finished:
                if not self._fetch_more_data():
                    # No more data from API, break the loop
                    break
            elif len(results) >= fetch_size:
                 # Reached the desired size
                 break
            elif self._query_finished:
                 # No more data in buffer and query is finished
                 break

        return results


    def fetchall(self) -> List[Tuple]:
        """Fetch all remaining rows of a query result."""
        self._check_closed()

        # Get rows already in buffer
        results = self._rows_buffer[self._row_index:]
        self._row_index = len(self._rows_buffer) # Mark buffer as consumed

        # Fetch remaining rows from API
        while not self._query_finished:
            if not self._fetch_more_data():
                break # No more data available
            # Append newly fetched rows (which are now at the end of _rows_buffer)
            results.extend(self._rows_buffer[self._row_index:])
            self._row_index = len(self._rows_buffer) # Update consumed marker

        return results

    # --- Optional/Unsupported Methods ---

    def executemany(self, operation: str, seq_of_parameters: Sequence) -> None:
        """Execute a command against a sequence of parameters."""
        self._check_closed()
        # This is often complex with APIs unless they support batch operations.
        # Simple implementation: loop execute. More efficient might need specific API endpoint.
        logger.warning("executemany called: Executing queries individually.")
        count = 0
        for params in seq_of_parameters:
            self.execute(operation, params)
            # Note: rowcount might be tricky here. PEP 249 suggests it should be
            # the total rows affected, or -1 if unavailable for any execution.
            count += 1 # Simplistic count of executions, not rows affected.
        # self._rowcount = count # Or -1 if individual execute doesn't set it reliably
        self._rowcount = -1 # Safer default for executemany over API
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
        # Typically used for stored procedures returning multiple result sets.
        # Likely not applicable to Salesforce CDP query API.
        return None

    def callproc(self, procname: str, parameters: Optional[Sequence[Any]] = None) -> Optional[Sequence[Any]]:
        """Call a stored procedure with the given name."""
        self._check_closed()
        raise self._connection.NotSupportedError("Stored procedures not supported by Salesforce CDP.")

    # --- Iterator Protocol ---

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


# --- Connect Function ---

def connect(
    login_url: str,
    client_id: str,
    client_secret: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    api_version: str = DEFAULT_API_VERSION,
    token_timeout: int = DEFAULT_TOKEN_TIMEOUT_SECONDS,
    **kwargs # Allow passing other args if needed later
) -> Connection:
    """
    Connect to the Salesforce CDP database.

    Returns a PEP 249 Connection object.
    """
    logger.debug(f"Initiating connection to Salesforce CDP: domain={login_url}, user={username}")

    try:
        # 1. Create Auth Handler
        if username and password:
            auth_handler = PasswordGrantAuth(
                username=username,
                password=password,
                client_id=client_id,
                client_secret=client_secret,
                domain=login_url,
                token_timeout=token_timeout
            )
        else:
            auth_handler = ClientCredentialsAuth(
                client_id=client_id,
                client_secret=client_secret,
                domain=login_url,
                token_timeout=token_timeout
            )
        # Perform initial authentication immediately to catch errors early
        auth_handler.authenticate()

        # 2. Create API Client
        client = SalesforceCDPClient(auth_handler=auth_handler, api_version=api_version)

        # 3. Create and return PEP 249 Connection
        connection = Connection(client=client)
        logger.debug("Salesforce CDP connection established successfully.")
        return connection

    except AuthenticationError as e:
        logger.error(f"Connection failed during authentication: {e}")
        # Re-raise as DatabaseError or OperationalError for connect failure
        raise DatabaseError(f"Connection failed: Authentication error - {e}") from e
    except Exception as e:
        logger.exception("Unexpected error during connect.")
        raise InterfaceError(f"Connection failed: Unexpected error - {e}") from e