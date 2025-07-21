import time
from abc import ABC, abstractmethod
from typing import Optional, Union, Sequence, Dict, Any, List, Tuple, Type
from loguru import logger
import importlib.metadata

from .auth import PasswordGrantAuth, ClientCredentialsAuth
from .exceptions import (
    Warning, Error, InterfaceError, DatabaseError, DataError,
    OperationalError, IntegrityError, InternalError, ProgrammingError,
    NotSupportedError, QueryError, AuthenticationError, ApiError
)
from .types import get_type_object, STRING, NUMBER, DATETIME, BINARY, ROWID
from .constants import DEFAULT_ROW_LIMIT, DEFAULT_API_VERSION, DEFAULT_TOKEN_TIMEOUT_SECONDS
from .base import BaseConnection, BaseCursor

ENTRYPOINT_GROUP_NAME = "salesforcecdpconnector.connections"

# PEP 249 Module Globals
apilevel = "2.0"
threadsafety = 1 # Threads may share the module, but not connections. Check Salesforce API concurrency limits.
paramstyle = 'named'


def load_plugin(protocol: str) -> Type[BaseConnection]:
    """Discover and register plugins using entry points."""
    logger.debug("Discovering plugins...")
    # Discover connection plugins - handle both old and new entry points API
    entry_points = importlib.metadata.entry_points()
    if hasattr(entry_points, 'get'):
        # New API (Python 3.10+)
        plugins = entry_points.get('salesforcecdpconnector.connections', [])
    else:
        # Old API (Python < 3.10) - entry_points is a dict-like object
        plugins = entry_points.get('salesforcecdpconnector.connections', []) if 'salesforcecdpconnector.connections' in entry_points else []
    logger.debug(f"plugins: {plugins}")
    for entry_point in plugins:
        try:
            plugin_module = entry_point.load()
            logger.debug(f"Found plugin module: {plugin_module}")
            logger.debug(f"entry_point.name: {entry_point.name}")
            if protocol == entry_point.name:
                logger.debug(f"Loading plugin module: {plugin_module}")
                return plugin_module
        except Exception as e:
            logger.warning(f"Failed to load connection plugin {entry_point.name}: {e}")
            raise e
    else:
        logger.error("No plugins found")
        raise InterfaceError("No plugins found")



def connect(
    domain: str,
    client_id: str,
    client_secret: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    api_version: str = DEFAULT_API_VERSION,
    token_timeout: int = DEFAULT_TOKEN_TIMEOUT_SECONDS,
    protocol: str = 'connect_api',
    **kwargs
) -> BaseConnection:
    """
    Create a connection to Salesforce CDP.
    
    Args:
        login_url: Salesforce login URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        username: Username for password grant (optional)
        password: Password for password grant (optional)
        api_version: API version to use
        token_timeout: Token timeout in seconds
        protocol: Protocol to use ('connect_api', 'grpc', etc.). If None, auto-discovers best available.
        **kwargs: Additional arguments passed to the connection constructor
    
    Returns:
        A connection object implementing BaseConnection
    """
    # Load plugin
    connection = getattr(load_plugin(protocol), 'Connection')

    
    # Create auth handler based on provided credentials
    if username and password:
        auth = PasswordGrantAuth(
            domain=domain,
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            token_timeout=token_timeout
        )
    else:
        auth = ClientCredentialsAuth(
            domain=domain,
            client_id=client_id,
            client_secret=client_secret,
            token_timeout=token_timeout
        )
    
    # Create connection using the selected plugin
    return connection(auth, api_version=api_version, **kwargs)