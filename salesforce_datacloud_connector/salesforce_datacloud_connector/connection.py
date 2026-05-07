"""
DB-API 2.0 Connection implementation for Salesforce Data Cloud.

The Connection class manages the database connection and creates cursors.
"""

from typing import List, Optional

from .api.client import DataCloudQueryClient
from .auth.oauth import OAuthAuthenticator
from .cursor import Cursor
from .exceptions import InterfaceError
from .metadata import DataCloudTable


class Connection:
    """
    DB-API 2.0 Connection for Salesforce Data Cloud.

    The connection manages authentication and creates cursor instances for
    executing queries.
    """

    def __init__(
        self,
        authenticator: OAuthAuthenticator,
        dataspace: Optional[str] = None,
        workload: Optional[str] = None,
    ):
        """
        Initialize connection.

        Args:
            authenticator: OAuth authenticator instance
            dataspace: Data space name (default: "default")
            workload: Optional workload name for logging/debugging

        Note: Use the connect() factory function instead of instantiating directly.
        """
        self._authenticator = authenticator
        self._dataspace = dataspace
        self._workload = workload
        self._closed = False

        # Create API client
        # Note: instance_url is obtained from OAuth response, not user input
        self._client = DataCloudQueryClient(
            instance_url=authenticator.get_instance_url(),
            auth_token_getter=authenticator.get_oauth_token,
            dataspace=dataspace,
            workload=workload,
        )

    def _check_closed(self):
        """Raise InterfaceError if connection is closed."""
        if self._closed:
            raise InterfaceError("Connection is closed")

    def cursor(self) -> Cursor:
        """
        Create a new cursor for executing queries.

        Returns:
            Cursor instance

        Raises:
            InterfaceError: If connection is closed
        """
        self._check_closed()
        return Cursor(self._client)

    def close(self):
        """
        Close the connection.

        The connection is unusable after this call.
        Any outstanding cursors should not be used after closing the connection.
        """
        if not self._closed:
            # Note: In V1, we don't explicitly close cursors.
            # Users should close cursors manually if needed.
            self._closed = True

    def commit(self):
        """
        Commit any pending transaction.

        Note: V1 driver is read-only, so this is a no-op.
        Provided for DB-API 2.0 compliance.

        Raises:
            InterfaceError: If connection is closed
        """
        self._check_closed()
        # No-op for read-only driver

    def rollback(self):
        """
        Rollback any pending transaction.

        Note: V1 driver is read-only, so this is a no-op.
        Provided for DB-API 2.0 compliance.

        Raises:
            InterfaceError: If connection is closed
        """
        self._check_closed()
        # No-op for read-only driver

    def list_tables(
        self,
        schema_pattern: Optional[str] = None,
        table_name_pattern: Optional[str] = None,
        table_types: Optional[List[str]] = None,
    ) -> List[DataCloudTable]:
        """
        List tables using pg_catalog queries.

        Args:
            schema_pattern: SQL LIKE pattern for schema filtering (e.g., "public", "sales%")
            table_name_pattern: SQL LIKE pattern for table name filtering
            table_types: List of table types to include (e.g., ["TABLE", "VIEW"])
                         If None, returns all user table types

        Returns:
            List of DataCloudTable objects with table and column metadata.

        Raises:
            InterfaceError: If connection is closed
            ProgrammingError: For SQL syntax errors
            OperationalError: For network failures
        """
        self._check_closed()
        cursor = self.cursor()
        try:
            from ._metadata_pg import list_tables_pg
            return list_tables_pg(
                cursor,
                schema_pattern=schema_pattern,
                table_name_pattern=table_name_pattern,
                table_types=table_types,
                dataspace=self._dataspace
            )
        finally:
            cursor.close()

    def get_table_metadata(
        self,
        table_name: str,
        schema: Optional[str] = None,
    ) -> Optional[DataCloudTable]:
        """
        Get complete metadata for a specific table using pg_catalog queries.

        Args:
            table_name: Table name (required). Can be unqualified ("Account") or
                        qualified ("public.Account")
            schema: Schema name. If provided, searches only that schema.
                    If None and table_name is unqualified, searches all user schemas.

        Returns:
            DataCloudTable object with full column metadata, or None if table not found.

        Raises:
            InterfaceError: If connection is closed
            ProgrammingError: If multiple tables match an unqualified name
            OperationalError: For network failures
        """
        self._check_closed()

        # Parse schema.table if qualified
        parsed_schema = schema
        parsed_table = table_name
        if '.' in table_name:
            parts = table_name.split('.', 1)
            parsed_schema = parts[0]
            parsed_table = parts[1]

        cursor = self.cursor()
        try:
            from ._metadata_pg import get_table_metadata_pg
            return get_table_metadata_pg(
                cursor,
                schema=parsed_schema,
                table_name=parsed_table,
                dataspace=self._dataspace
            )
        finally:
            cursor.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

    # Additional properties for introspection
    @property
    def dataspace(self) -> str:
        """Get the configured data space name."""
        return self._dataspace

    @property
    def workload(self) -> Optional[str]:
        """Get the configured workload name."""
        return self._workload

    @property
    def closed(self) -> bool:
        """Check if connection is closed."""
        return self._closed
