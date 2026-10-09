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

import os
from typing import Dict, Optional, Union

import requests

# DB-API 2.0 module globals
apilevel = "2.0"  # DB-API specification version
threadsafety = 2  # Threads may share the module and connections, but not cursors
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

# Import and expose query status structure (used by Cursor.get_query_status)
from .api.models import QueryStatus

# Import connection
from .connection import Connection

# Import authenticators
from .auth.oauth import (
    ClientCredentialsAuthenticator,
    JWTAuthenticator,
    RefreshTokenAuthenticator,
    SfCliAuthenticator,
    UsernamePasswordAuthenticator,
)

# Import token exchanger
from .auth._http import RetryConfig
from .http_options import HttpOptions
from .auth.direct_token import DirectCdpTokenProvider
from .auth.token_exchanger import DataCloudTokenExchanger


def connect(
    login_url: str = "https://login.salesforce.com",
    auth_type: str = "username_password",
    username: Optional[str] = None,
    password: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    jwt_private_key: Optional[Union[str, bytes, os.PathLike]] = None,
    refresh_token: Optional[str] = None,
    dataspace: Optional[str] = None,
    workload: Optional[str] = None,
    target_org: Optional[str] = None,
    user_agent: Optional[str] = None,
    query_settings: Optional[Dict[str, str]] = None,
    output_format: str = "arrow",
    auth_retry: Optional[RetryConfig] = None,
    cdp_token: Optional[str] = None,
    tenant_endpoint: Optional[str] = None,
    timeout: float = HttpOptions.DEFAULT_TIMEOUT_SECONDS,
    verify: Optional[Union[bool, str]] = None,
    proxies: Optional[Dict[str, str]] = None,
    session: Optional[requests.Session] = None,
) -> Connection:
    """
    Create a connection to Salesforce Data Cloud.

    This is the primary entry point for the driver. It creates and configures
    an appropriate OAuth authenticator based on the auth_type, then returns
    a Connection instance.

    Args:
        login_url: Salesforce login URL (default: "https://login.salesforce.com")
                  Use "https://test.salesforce.com" for sandboxes
        auth_type: Authentication type - "username_password", "jwt", "refresh_token",
                   "client_credentials", "sf_cli", or "cdp_token"
        username: Salesforce username (required for username_password and jwt)
        password: Salesforce password (required for username_password)
        client_id: Connected app client ID (required for all auth types except sf_cli)
        client_secret: Connected app client secret (required for username_password and refresh_token)
        jwt_private_key: RSA private key for the JWT flow (required for jwt): PEM
            text or bytes, or a path to a PEM file. Validated before any network I/O.
        refresh_token: OAuth refresh token (required for refresh_token)
        dataspace: Data space name (default: "default")
        workload: Optional workload name for logging/debugging
        target_org: Org alias or username for the sf_cli auth type. If omitted,
                    the Salesforce CLI's own default org is used.
        cdp_token: Data Cloud (CDP) access token for the cdp_token auth type, a JWT
                   with an ``exp`` claim. Skips OAuth login and token exchange;
                   the token cannot be refreshed, so once it expires the
                   connection fails and a new one must be created.
        tenant_endpoint: Data Cloud tenant endpoint for the cdp_token auth type
                   (e.g. "tenant.c360a.salesforce.com"; scheme optional).
        user_agent: Optional caller identifier appended to the driver's
                    User-Agent header (e.g. "my-app/1.0"). The header sent is
                    "salesforce-cdp-connector/{version} {user_agent}".
        query_settings: Connection-wide default query settings (e.g. {"time_zone": "UTC"}),
            merged into every cursor.execute() call on this connection. See
            https://tableau.github.io/hyper-db/docs/hyper-api/connection#connection-settings
        output_format: "arrow" (default) or "json". Selects the query result
            wire format negotiated with off-core Query v3. Arrow is the
            recommended default; "json" remains available as an opt-in
            fallback.
        auth_retry: Retry policy for transient (5xx/429/network) failures of the
            OAuth and CDP token-exchange endpoints, as a RetryConfig
            (default: 3 retries, exponential backoff from 1s up to 30s).
            Use RetryConfig(max_retries=0) to disable. Credential errors
            (400/401/403) are never retried.

        timeout: Per-request timeout in seconds for every HTTP call (OAuth,
            token exchange, queries). Default 30.
        verify: TLS verification for every HTTP call: True/False or a path to a
            CA bundle. Default: requests' behaviour (system/certifi CAs).
        proxies: Proxy mapping such as {"https": "http://proxy:3128"}. Default:
            standard proxy environment variables.
        session: A caller-supplied requests.Session used for every HTTP call
            (custom adapters, client certs, ...). Mutually exclusive with
            verify and proxies.

    Returns:
        Connection instance

    Raises:
        ValueError: If required parameters are missing for the selected auth type,
            or if output_format is not "arrow" or "json"
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
    # Validate output_format before any network I/O (authentication happens
    # below via the token exchanger). Without this, an invalid output_format
    # would only surface after a real login round-trip, inside
    # DataCloudQueryClient.__init__ via Connection.__init__.
    if output_format not in ("arrow", "json"):
        raise ValueError(
            f"Invalid output_format: {output_format!r}. Must be 'arrow' or 'json'"
        )

    http = HttpOptions(timeout=timeout, verify=verify, proxies=proxies, session=session)

    # A pre-minted CDP token needs no authenticator or token exchange.
    if auth_type == "cdp_token":
        if not all([cdp_token, tenant_endpoint]):
            raise ValueError("cdp_token auth requires: cdp_token, tenant_endpoint")
        return Connection(
            DirectCdpTokenProvider(cdp_token, tenant_endpoint),
            dataspace=dataspace,
            workload=workload,
            user_agent=user_agent,
            query_settings=query_settings,
            output_format=output_format,
            http=http,
        )

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
            retry=auth_retry,
            http=http,
        )

    elif auth_type == "jwt":
        if not all([username, client_id, jwt_private_key]):
            raise ValueError("jwt auth requires: username, client_id, jwt_private_key")
        if client_secret:
            raise ValueError(
                "client_secret is not allowed for jwt auth: the JWT bearer flow "
                "does not use a client secret"
            )
        authenticator = JWTAuthenticator(
            login_url=login_url,
            client_id=client_id,
            username=username,
            jwt_private_key=jwt_private_key,
            retry=auth_retry,
            http=http,
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
            retry=auth_retry,
            http=http,
        )

    elif auth_type == "client_credentials":
        if not all([client_id, client_secret]):
            raise ValueError(
                "client_credentials auth requires: client_id, client_secret"
            )
        authenticator = ClientCredentialsAuthenticator(
            login_url=login_url,
            client_id=client_id,
            client_secret=client_secret,
            retry=auth_retry,
            http=http,
        )

    elif auth_type == "sf_cli":
        authenticator = SfCliAuthenticator(target_org=target_org)

    else:
        raise ValueError(
            f"Invalid auth_type: {auth_type}. "
            f"Must be 'username_password', 'jwt', 'refresh_token', 'client_credentials', 'sf_cli', or 'cdp_token'"
        )

    # Wrap authenticator in token exchanger for CDP token + tenant endpoint
    exchanger = DataCloudTokenExchanger(
        core_authenticator=authenticator,
        dataspace=dataspace,
        retry=auth_retry,
        http=http,
    )

    # Create and return connection with exchanger
    return Connection(
        exchanger,
        dataspace=dataspace,
        workload=workload,
        user_agent=user_agent,
        query_settings=query_settings,
        output_format=output_format,
        http=http,
    )


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
    # Query status structure
    "QueryStatus",
    # Authenticators (advanced usage)
    "UsernamePasswordAuthenticator",
    "JWTAuthenticator",
    "RefreshTokenAuthenticator",
    "ClientCredentialsAuthenticator",
    "SfCliAuthenticator",
    "DataCloudTokenExchanger",
    "DirectCdpTokenProvider",
    "RetryConfig",
]

# Package metadata
__version__ = "2.0.0b2"
