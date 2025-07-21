"""
Connect API (REST) plugin for Salesforce CDP Connector.

This plugin provides REST API-based connectivity to Salesforce Data Cloud.
"""

from .connection import Connection # type: ignore
from .cursor import Cursor # type: ignore
from .client import Client # type: ignore

__all__ = ['Connection', 'Cursor', 'Client'] 