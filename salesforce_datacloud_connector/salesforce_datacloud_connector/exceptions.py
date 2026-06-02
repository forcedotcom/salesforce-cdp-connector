"""
DB-API 2.0 Exception Hierarchy for Salesforce Data Cloud Driver.

This module implements the standard exception hierarchy defined in PEP 249.
All exceptions inherit from the base Error class, which itself inherits from
the standard Exception class.
"""

from typing import Optional


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

    def __init__(self, message: str, http_status: Optional[int] = None):
        """
        Initialize the error.

        Args:
            message: Human-readable error message
            http_status: Optional HTTP status code from the API response
        """
        super().__init__(message)
        self.message = message
        self.http_status = http_status


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


def map_http_error_to_exception(
    status_code: int, message: str, default_message: str = "Database error"
) -> Error:
    """
    Map HTTP status codes to appropriate DB-API 2.0 exception types.

    Args:
        status_code: HTTP status code from the API response
        message: Error message from the API response
        default_message: Default message if none provided

    Returns:
        Appropriate exception instance
    """
    error_message = message or default_message

    if status_code == 400:
        # Bad request - usually SQL syntax errors or invalid parameters
        return ProgrammingError(error_message, status_code)
    elif status_code in (401, 403):
        # Authentication or authorization errors
        return OperationalError(error_message, status_code)
    elif status_code == 404:
        # Resource not found
        return ProgrammingError(error_message, status_code)
    elif status_code == 429:
        # Rate limiting
        return OperationalError(error_message, status_code)
    elif 500 <= status_code < 600:
        # Server errors
        return InternalError(error_message, status_code)
    else:
        # Unknown error - use generic DatabaseError
        return DatabaseError(error_message, status_code)
