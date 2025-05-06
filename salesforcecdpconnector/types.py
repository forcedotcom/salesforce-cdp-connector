from datetime import datetime
import decimal

# PEP 249 Type Objects
# https://peps.python.org/pep-0249/#type-objects

STRING = str
BINARY = bytes
NUMBER = decimal.Decimal # Use Decimal for precision
DATETIME = datetime
ROWID = int # Assuming ROWID can be represented as int, adjust if needed

# Mapping from Salesforce CDP types (adjust based on actual metadata values)
# This is a placeholder - you need to inspect the actual 'metadata' response
# from the API to get the correct type names (e.g., 'Text', 'Number', 'Date/Time')
# and map them to the PEP 249 types.
SALESFORCE_TYPE_MAP = {
    'TEXT': STRING,
    'STRING': STRING,
    'ID': STRING,
    'URL': STRING,
    'EMAIL': STRING,
    'PHONE': STRING,
    'PICKLIST': STRING,
    'MULTIPICKLIST': STRING,
    'TEXTAREA': STRING,
    'BOOLEAN': bool, # Might map to NUMBER (0/1) depending on preference
    'NUMBER': NUMBER,
    'INTEGER': int, # Might map to NUMBER
    'LONG': int, # Might map to NUMBER
    'DOUBLE': float, # Might map to NUMBER
    'CURRENCY': NUMBER,
    'PERCENT': NUMBER,
    'DATE': DATETIME, # Or date type if available
    'DATETIME': DATETIME,
    'TIME': DATETIME, # Or time type if available
    # Add other Salesforce types as needed
}

def get_type_object(salesforce_type_name: str):
    """Maps a Salesforce type name (from metadata) to a PEP 249 Type Object."""
    return SALESFORCE_TYPE_MAP.get(str(salesforce_type_name).upper(), STRING) # Default to STRING if unknown