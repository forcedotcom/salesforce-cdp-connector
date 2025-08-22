"""
gRPC Client implementation for Salesforce CDP Connector.

This is a placeholder implementation that throws an error when used.
"""

from salesforcecdpconnector.base import BaseClient

class Client(BaseClient):
    """
    Placeholder Client class for gRPC plugin.
    
    This implementation throws an error to indicate that the gRPC plugin
    is not yet fully implemented.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__()
        raise NotImplementedError(
            "gRPC plugin is not yet implemented. "
            "This is a placeholder implementation. "
            "Please use the standard Salesforce CDP Connector instead."
        )
    
    def submit_query(self, sql: str, params=None):
        raise NotImplementedError(
            "gRPC plugin is not yet implemented. "
            "This is a placeholder implementation. "
            "Please use the standard Salesforce CDP Connector instead."
        )
    
    def get_query_status(self, query_id: str):
        raise NotImplementedError(
            "gRPC plugin is not yet implemented. "
            "This is a placeholder implementation. "
            "Please use the standard Salesforce CDP Connector instead."
        )
    
    def get_query_results(self, query_id: str, offset: int, limit: int):
        raise NotImplementedError(
            "gRPC plugin is not yet implemented. "
            "This is a placeholder implementation. "
            "Please use the standard Salesforce CDP Connector instead."
        )
    
    def connect(self, *args, **kwargs):
        raise NotImplementedError(
            "gRPC plugin is not yet implemented. "
            "This is a placeholder implementation. "
            "Please use the standard Salesforce CDP Connector instead."
        )
    
    def disconnect(self, *args, **kwargs):
        raise NotImplementedError(
            "gRPC plugin is not yet implemented. "
            "This is a placeholder implementation. "
            "Please use the standard Salesforce CDP Connector instead."
        )
    
    def execute_query(self, *args, **kwargs):
        raise NotImplementedError(
            "gRPC plugin is not yet implemented. "
            "This is a placeholder implementation. "
            "Please use the standard Salesforce CDP Connector instead."
        )
    
    def get_connection_status(self, *args, **kwargs):
        raise NotImplementedError(
            "gRPC plugin is not yet implemented. "
            "This is a placeholder implementation. "
            "Please use the standard Salesforce CDP Connector instead."
        ) 