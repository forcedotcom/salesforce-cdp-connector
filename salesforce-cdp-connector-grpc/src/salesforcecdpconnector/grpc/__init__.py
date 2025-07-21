"""
gRPC plugin for Salesforce CDP Connector.

This plugin provides gRPC-based connectivity to Salesforce Data Cloud.
"""

from .connection import Connection
from .cursor import Cursor
from .client import Client

__all__ = ['Connection', 'Cursor', 'Client'] 