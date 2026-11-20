"""
DB-API 2.0 Exception Hierarchy for Salesforce Data Cloud Driver.

This module implements the standard exception hierarchy defined in PEP 249.
All exceptions inherit from the base Error class, which itself inherits from
the standard Exception class.
"""

from typing import Any, Dict, Optional


class Warning(Exception):
    """
    Exception raised for important warnings like data truncations while inserting, etc.

    Note: In V1 of the driver (read-only), this is rarely used.
    """

    pass


class Error(Exception):
    """
    Base class for all database-related errors.

    All other exceptions in this module inherit from this class.
    """

    def __init__(
        self,
        message: str,
        http_status: Optional[int] = None,
        sqlstate: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the error.

        Args:
            message: Human-readable error message
            http_status: Optional HTTP status code from the API response
            sqlstate: Optional five-character SQLSTATE reported by the server
                (e.g. "42P01" for an unknown table)
            details: Optional server-side error details (customerDetail,
                position, errorSource, ...)
        """
        super().__init__(message)
        self.message = message
        self.http_status = http_status
        self.sqlstate = sqlstate
        self.details = details


class InterfaceError(Error):
    """
    Exception raised for errors related to the database interface rather than
    the database itself. Examples: connection issues, invalid parameter types.
    """

    pass


class DatabaseError(Error):
    """
    Exception raised for errors related to the database.

    All database-specific errors should inherit from this class.
    """

    pass


class DataError(DatabaseError):
    """
    Exception raised for errors due to problems with the processed data.

    Examples: division by zero, numeric value out of range, invalid date format.
    """

    pass


class OperationalError(DatabaseError):
    """
    Exception raised for errors related to the database's operation.

    Examples: unexpected disconnect, database not found, transaction failed,
    authentication failures, memory allocation errors.

    This is commonly used for HTTP 401/403 (authentication/authorization errors)
    and network-related issues.
    """

    pass


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is affected.

    Examples: foreign key check fails, duplicate key violations.

    Note: In V1 of the driver (read-only), this is rarely used.
    """

    pass


class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error.

    Examples: cursor not valid anymore, transaction out of sync.

    This is commonly used for HTTP 500 (internal server errors).
    """

    pass


class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors.

    Examples: table not found, SQL syntax errors, wrong number of parameters,
    invalid SQL statement.

    This is commonly used for HTTP 400 (bad request) errors related to SQL.
    """

    pass


class NotSupportedError(DatabaseError):
    """
    Exception raised when a method or database API is not supported.

    Examples: requesting a rollback on a connection that doesn't support
    transactions, attempting to close a closed cursor, trying to execute
    DML/DDL operations on a read-only driver.
    """

    pass


# SQLSTATE class (first two characters) -> PEP 249 exception. Classes not listed
# fall back to the HTTP status mapping.
_SQLSTATE_CLASS_TO_EXCEPTION = {
    # Data exceptions (bad cast, division by zero, value out of range, ...)
    "22": DataError,
    # Integrity constraint violations
    "23": IntegrityError,
    # Feature not supported
    "0A": NotSupportedError,
    # Syntax errors, unknown tables/columns/functions, access rule violations
    "42": ProgrammingError,
    "3D": ProgrammingError,
    "3F": ProgrammingError,
    "21": ProgrammingError,
    # Connection, resources, operator intervention (incl. 57014 query canceled), system errors
    "08": OperationalError,
    "53": OperationalError,
    "54": OperationalError,
    "57": OperationalError,
    "58": OperationalError,
    "40": OperationalError,
    # Internal error
    "XX": InternalError,
}


def map_http_error_to_exception(
    status_code: int,
    message: str,
    default_message: str = "Database error",
    sqlstate: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Error:
    """
    Map an API error to the appropriate DB-API 2.0 exception type.

    When the server supplies a SQLSTATE, its class selects the exception (an
    unknown table is a ProgrammingError, a bad cast a DataError, ...).
    Otherwise, or for a SQLSTATE class not
    listed above, the HTTP status code decides.

    Args:
        status_code: HTTP status code from the API response
        message: Error message from the API response
        default_message: Default message if none provided
        sqlstate: Optional SQLSTATE reported by the server
        details: Optional server-side error details

    Returns:
        Appropriate exception instance
    """
    error_message = message or default_message

    exception_class = _SQLSTATE_CLASS_TO_EXCEPTION.get(sqlstate[:2]) if sqlstate else None
    if exception_class is None:
        exception_class = _http_status_to_exception_class(status_code)
    return exception_class(error_message, status_code, sqlstate=sqlstate, details=details)


def _http_status_to_exception_class(status_code: int):
    if status_code == 400:
        # Bad request - usually SQL syntax errors or invalid parameters
        return ProgrammingError
    elif status_code in (401, 403):
        # Authentication or authorization errors
        return OperationalError
    elif status_code == 404:
        # Resource not found
        return ProgrammingError
    elif status_code == 429:
        # Rate limiting
        return OperationalError
    elif 500 <= status_code < 600:
        # Server errors
        return InternalError
    else:
        # Unknown error - use generic DatabaseError
        return DatabaseError
