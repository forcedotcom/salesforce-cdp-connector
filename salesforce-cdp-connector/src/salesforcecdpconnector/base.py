from abc import ABC, abstractmethod
from typing import Optional, List, Tuple, Union, Sequence, Dict, Any
from .exceptions import Error, Warning, InterfaceError, DatabaseError, DataError, OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError, AuthenticationError, QueryError, ApiError
from .constants import DEFAULT_ROW_LIMIT


class BaseAuthHandler(ABC):
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

class BaseConnection(ABC):
    """Abstract base class for PEP 249 Connection objects."""

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

    def __init__(self):
        self._is_closed = False

    def _check_closed(self):
        if self._is_closed:
            raise self.Error("Connection is closed.")

    @abstractmethod
    def close(self) -> None:
        """Close the connection now."""
        pass

    @abstractmethod
    def commit(self) -> None:
        """Commit any pending transaction to the database."""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Roll back to the start of any pending transaction."""
        pass

    @abstractmethod
    def cursor(self) -> 'BaseCursor':
        """Return a new Cursor Object using the connection."""
        pass

    def __enter__(self):
        """Enter context manager."""
        self._check_closed()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        # Don't rollback on error, as it's not supported
        # Always close the connection
        self.close()


class BaseCursor(ABC):
    """Abstract base class for PEP 249 Cursor objects."""

    def __init__(self, connection: BaseConnection):
        self._connection = connection
        self.arraysize = DEFAULT_ROW_LIMIT
        self._is_closed = False

    def _check_closed(self):
        if self._is_closed:
            raise self._connection.Error("Cursor is closed.")
        self._connection._check_closed()

    @property
    @abstractmethod
    def description(self) -> Optional[List[Tuple]]:
        """Read-only attribute sequence of 7-item sequences."""
        pass

    @property
    @abstractmethod
    def rowcount(self) -> int:
        """Read-only attribute specifying the number of rows affected/fetched."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the cursor."""
        pass

    @abstractmethod
    def execute(self, operation: str, parameters: Optional[Union[Sequence, Dict[str, Any]]] = None) -> None:
        """Prepare and execute a database operation (query or command)."""
        pass

    @abstractmethod
    def fetchone(self) -> Optional[Tuple]:
        """Fetch the next row of a query result set."""
        pass

    @abstractmethod
    def fetchmany(self, size: Optional[int] = None) -> List[Tuple]:
        """Fetch the next set of rows of a query result."""
        pass

    @abstractmethod
    def fetchall(self) -> List[Tuple]:
        """Fetch all (remaining) rows of a query result."""
        pass

    def executemany(self, operation: str, seq_of_parameters: Sequence) -> None:
        """Prepare a database operation (query or command) and then execute it against all parameter sequences or mappings found in the sequence seq_of_parameters."""
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        """This can be used before a call to execute() to predefine memory areas for the operation's parameters."""
        pass

    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """Set a column buffer size for fetches of large columns."""
        pass

    def nextset(self) -> Optional[bool]:
        """This method will make the cursor skip to the next available set, discarding any remaining rows from the current set."""
        return None

    def callproc(self, procname: str, parameters: Optional[Sequence[Any]] = None) -> Optional[Sequence[Any]]:
        """Call a stored database procedure with the given name."""
        raise self._connection.NotSupportedError("Stored procedures not supported.")

    def __iter__(self):
        """Return self to make cursors compatible to the iteration protocol."""
        return self

    def __next__(self):
        """Return the next row from the currently executing SQL statement using the same semantics as fetchone()."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.close()


class BaseClient(ABC):
    """Abstract base class for client implementations that communicate with Salesforce CDP."""

    def __init__(self):
        """Initialize the base client."""
        pass

    @abstractmethod
    def submit_query(self, sql: str, params: Optional[Union[Sequence, Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Submit a SQL query to the CDP API.
        
        Args:
            sql: The SQL query string to execute
            params: Optional parameters for the query
            
        Returns:
            Dictionary containing the query response
        """
        pass

    @abstractmethod
    def get_query_status(self, query_id: str) -> Dict[str, Any]:
        """Get the status of a previously submitted query.
        
        Args:
            query_id: The ID of the query to check
            
        Returns:
            Dictionary containing the query status
        """
        pass

    @abstractmethod
    def get_query_results(self, query_id: str, offset: int, limit: int) -> Dict[str, Any]:
        """Fetch a page of results for a query.
        
        Args:
            query_id: The ID of the query
            offset: The offset for pagination
            limit: The maximum number of rows to return
            
        Returns:
            Dictionary containing the query results
        """
        pass

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        # Default implementation does nothing, subclasses can override if needed
        pass