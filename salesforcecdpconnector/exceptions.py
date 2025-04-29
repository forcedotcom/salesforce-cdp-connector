# salesforce_cdp_connector/exceptions.py

import requests.exceptions

# PEP 249 Standard Exceptions
# https://peps.python.org/pep-0249/#exceptions

class Error(Exception):
    """Exception that is the base class of all other error exceptions.
    You can use this to catch all errors with one single except statement.
    """
    pass

class Warning(Exception):
    """Exception raised for important warnings like data truncations
    while inserting, etc."""
    pass

class InterfaceError(Error):
    """Exception raised for errors that are related to the database
    interface rather than the database itself."""
    pass

class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""
    pass

class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range,
    etc."""
    pass

class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer,
    e.g. an unexpected disconnect occurs, the data source name is not
    found, a transaction could not be processed, a memory allocation
    error occurred during processing, etc."""
    pass

class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database
    is affected, e.g. a foreign key check fails."""
    pass

class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of sync,
    etc."""
    pass

class ProgrammingError(DatabaseError):
    """Exception raised for programming errors, e.g. table not found
    or already exists, syntax error in the SQL statement, wrong number
    of parameters specified, etc."""
    pass

class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used which
    is not supported by the database, e.g. requesting a .rollback()
    on a connection that does not support transaction or has transactions
    turned off."""
    pass

# Custom Exceptions specific to this driver
class AuthenticationError(DatabaseError):
    """Exception raised for authentication failures."""
    pass

class ApiError(OperationalError):
    """Exception raised for general API errors returned by Salesforce CDP."""
    def __init__(self, message, http_error=None):
        super().__init__(message)
        self.http_error = http_error

class QueryError(ProgrammingError):
    """Exception raised for errors during query execution specifically."""
    pass