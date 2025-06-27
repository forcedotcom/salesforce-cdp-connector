import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


from .dbapi import connect, apilevel, threadsafety, paramstyle
from .types import STRING, BINARY, NUMBER, DATETIME, ROWID
from .exceptions import (
    Warning, Error, InterfaceError, DatabaseError, DataError,
    OperationalError, IntegrityError, InternalError, ProgrammingError,
    NotSupportedError, AuthenticationError, ApiError, QueryError
)

# Version will be automatically set by Hatch during build
__version__ = "unknown"

__all__ = [
    # connect function
    "connect",
    # Globals
    "apilevel",
    "threadsafety",
    "paramstyle",
    # Type Objects
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    # Exceptions (standard and custom)
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "AuthenticationError",
    "ApiError",
    "QueryError",
    # Version
    "__version__",
]