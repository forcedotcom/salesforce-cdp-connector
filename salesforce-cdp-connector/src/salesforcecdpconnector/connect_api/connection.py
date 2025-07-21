"""
Connect API Connection implementation for Salesforce CDP.
"""

from typing import Optional
from loguru import logger

from ..base import BaseConnection
from .client import Client
from ..auth import BaseAuthHandler
from .cursor import Cursor

class Connection(BaseConnection):
    """Connect API (REST) Connection implementation for Salesforce CDP."""

    def __init__(self, auth: BaseAuthHandler, api_version: str = "v60.0", **kwargs):
        super().__init__()
        self._client = Client(auth, api_version)
        self._is_closed = False

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
        return Cursor(self._client, self) 