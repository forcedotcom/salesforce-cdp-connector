import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""
PEP 249 compliant DB-API 2.0 driver for Salesforce CDP.
"""

from .dbapi import connect, apilevel, threadsafety, paramstyle
from .types import STRING, BINARY, NUMBER, DATETIME, ROWID
from .exceptions import (
    Warning, Error, InterfaceError, DatabaseError, DataError,
    OperationalError, IntegrityError, InternalError, ProgrammingError,
    NotSupportedError, AuthenticationError, ApiError, QueryError
)

# Optional: Set up logging defaults for the library if desired
# from loguru import logger
# import sys
# logger.add(sys.stderr, level="INFO") # Example: default logging to stderr at INFO level

__version__ = "0.1.0" # Example version

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