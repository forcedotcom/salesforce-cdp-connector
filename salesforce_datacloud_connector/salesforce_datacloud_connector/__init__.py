"""
Salesforce Data Cloud Python Driver - DB-API 2.0 compliant driver.

This package provides a Python database driver for querying Salesforce Data Cloud
using the Query API. It follows the Python DB-API 2.0 specification (PEP 249).

Basic usage:
    import salesforce_datacloud_connector as sfdc

    conn = sfdc.connect(
        login_url="https://login.salesforce.com",
        auth_type="username_password",
        username="user@example.com",
        password="password",
        client_id="client_id",
        client_secret="client_secret"
    )

    cursor = conn.cursor()
    cursor.execute("SELECT Id, Name FROM Account WHERE Status = :status", {"status": "Active"})

    for row in cursor:
        print(row)

    conn.close()
"""

from typing import Optional

# DB-API 2.0 module globals
apilevel = "2.0"  # DB-API specification version
threadsafety = 1  # Threads may share the module, but not connections
paramstyle = "named"  # Named parameter style (:param)

# Import and expose exceptions
from .exceptions import (
    DataError,
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

# Import and expose type objects
from .types import BINARY, DATETIME, NUMBER, ROWID, STRING

# Import and expose metadata structures
from .metadata import DataCloudTable, Field

# Import connection
from .connection import Connection

# Import authenticators
from .auth.oauth import (
    JWTAuthenticator,
    RefreshTokenAuthenticator,
    UsernamePasswordAuthenticator,
)


def connect(
    login_url: str = "https://login.salesforce.com",
    auth_type: str = "username_password",
    username: Optional[str] = None,
    password: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    jwt_private_key: Optional[str] = None,
    refresh_token: Optional[str] = None,
    dataspace: Optional[str] = None,
    workload: Optional[str] = None,
) -> Connection:
    """
    Create a connection to Salesforce Data Cloud.

    This is the primary entry point for the driver. It creates and configures
    an appropriate OAuth authenticator based on the auth_type, then returns
    a Connection instance.

    Args:
        login_url: Salesforce login URL (default: "https://login.salesforce.com")
                  Use "https://test.salesforce.com" for sandboxes
        auth_type: Authentication type - "username_password", "jwt", or "refresh_token"
        username: Salesforce username (required for username_password and jwt)
        password: Salesforce password (required for username_password)
        client_id: Connected app client ID (required for all auth types)
        client_secret: Connected app client secret (required for username_password and refresh_token)
        jwt_private_key: Private key in PEM format for JWT flow (required for jwt)
        refresh_token: OAuth refresh token (required for refresh_token)
        dataspace: Data space name (default: "default")
        workload: Optional workload name for logging/debugging

    Returns:
        Connection instance

    Raises:
        ValueError: If required parameters are missing for the selected auth type
        OperationalError: If authentication fails

    Examples:
        # Username/Password authentication
        conn = connect(
            login_url="https://login.salesforce.com",
            auth_type="username_password",
            username="user@example.com",
            password="password123",
            client_id="3MVG9...",
            client_secret="secret123"
        )

        # JWT authentication
        conn = connect(
            login_url="https://login.salesforce.com",
            auth_type="jwt",
            username="user@example.com",
            client_id="3MVG9...",
            jwt_private_key=open("private.pem").read()
        )

        # Refresh token authentication
        conn = connect(
            login_url="https://login.salesforce.com",
            auth_type="refresh_token",
            client_id="3MVG9...",
            client_secret="secret123",
            refresh_token="refresh_token_here"
        )
    """
    # Validate and create authenticator based on auth_type
    if auth_type == "username_password":
        if not all([username, password, client_id, client_secret]):
            raise ValueError(
                "username_password auth requires: username, password, client_id, client_secret"
            )
        authenticator = UsernamePasswordAuthenticator(
            login_url=login_url,
            username=username,
            password=password,
            client_id=client_id,
            client_secret=client_secret,
        )

    elif auth_type == "jwt":
        if not all([username, client_id, jwt_private_key]):
            raise ValueError("jwt auth requires: username, client_id, jwt_private_key")
        authenticator = JWTAuthenticator(
            login_url=login_url,
            client_id=client_id,
            username=username,
            jwt_private_key=jwt_private_key,
        )

    elif auth_type == "refresh_token":
        if not all([client_id, client_secret, refresh_token]):
            raise ValueError(
                "refresh_token auth requires: client_id, client_secret, refresh_token"
            )
        authenticator = RefreshTokenAuthenticator(
            login_url=login_url,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
        )

    else:
        raise ValueError(
            f"Invalid auth_type: {auth_type}. "
            f"Must be 'username_password', 'jwt', or 'refresh_token'"
        )

    # Create and return connection
    return Connection(authenticator, dataspace=dataspace, workload=workload)


# Public API
__all__ = [
    # DB-API 2.0 globals
    "apilevel",
    "threadsafety",
    "paramstyle",
    # Connection factory
    "connect",
    "Connection",
    # Exceptions
    "Error",
    "Warning",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    # Type objects
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    # Metadata structures
    "DataCloudTable",
    "Field",
    # Authenticators (advanced usage)
    "UsernamePasswordAuthenticator",
    "JWTAuthenticator",
    "RefreshTokenAuthenticator",
]

# Package metadata
__version__ = "0.1.0"
