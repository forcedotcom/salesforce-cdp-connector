# Salesforce CDP Connector - Developer Documentation

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Code Structure](#code-structure)
3. [Component Responsibilities](#component-responsibilities)
4. [Data Flow](#data-flow)
5. [Extending the Connector](#extending-the-connector)
6. [Implementing Alternative Protocols](#implementing-alternative-protocols)
7. [Testing Strategy](#testing-strategy)

## Architecture Overview

The Salesforce CDP Connector follows a **layered architecture** pattern with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    User Application Layer                    │
│  (Uses PEP 249 Database API)                                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Database Interface Layer                   │
│  dbapi.py - PEP 249 compliant Connection/Cursor classes     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Protocol Layer                           │
│  client.py - HTTP API client (can be replaced)             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Authentication Layer                      │
│  auth.py - OAuth2 token management                          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Salesforce CDP API                        │
│  (External REST API)                                        │
└─────────────────────────────────────────────────────────────┘
```

## Code Structure

```
src/salesforcecdpconnector/
├── __init__.py          # Public API exports
├── auth.py              # Authentication handlers
├── client.py            # HTTP API client
├── dbapi.py             # PEP 249 database interface
├── exceptions.py        # Custom exception classes
├── types.py             # Data type mappings
├── utils.py             # Utility functions
└── constants.py         # Configuration constants
```

### Key Design Principles

1. **Separation of Concerns**: Each module has a single, well-defined responsibility
2. **Interface Segregation**: Clear contracts between layers
3. **Dependency Inversion**: High-level modules don't depend on low-level modules
4. **PEP 249 Compliance**: Standard database interface for Python
5. **Protocol Agnostic**: Transport layer can be swapped out

## Component Responsibilities

### 1. `dbapi.py` - Database Interface Layer

**Purpose**: Implements PEP 249 Database API specification

**Key Classes**:
- `Connection`: Manages database connections and transactions
- `Cursor`: Handles query execution and result fetching
- `connect()`: Factory function for creating connections

**Responsibilities**:
- Query state management (pagination, metadata)
- Result set transformation (API response → database rows)
- Error handling and exception translation
- Connection lifecycle management

**Example Usage**:
```python
import salesforcecdpconnector as sf

# Create connection
conn = sf.connect(
    username="user@example.com",
    password="password",
    client_id="your_client_id",
    client_secret="your_client_secret",
    domain="login.salesforce.com"
)

# Execute queries
cursor = conn.cursor()
cursor.execute("SELECT Id, Name FROM Contact LIMIT 10")
rows = cursor.fetchall()
```

### 2. `client.py` - Protocol Layer

**Purpose**: Handles low-level communication with Salesforce CDP API

**Key Class**: `SalesforceCDPClient`

**Responsibilities**:
- HTTP request/response handling
- URL construction and path management
- JSON serialization/deserialization
- HTTP error handling and retry logic
- Session management

**Key Methods**:
- `submit_query()`: POST /query-sql
- `get_query_status()`: GET /query-sql/{id}
- `get_query_results()`: GET /query-sql/{id}/rows

### 3. `auth.py` - Authentication Layer

**Purpose**: Manages OAuth2 authentication and token lifecycle

**Key Classes**:
- `AuthHandler` (ABC): Abstract base class for auth strategies
- `PasswordGrantAuth`: Username/password authentication
- `ClientCredentialsAuth`: Client credentials flow

**Responsibilities**:
- Token acquisition and refresh
- Header generation for authenticated requests
- Instance URL management
- Token expiration handling

### 4. Supporting Modules

- **`exceptions.py`**: Custom exception hierarchy
- **`types.py`**: Salesforce data type mappings
- **`utils.py`**: Shared utility functions
- **`constants.py`**: Configuration constants

## Data Flow

### Query Execution Flow

```
1. User calls cursor.execute(sql, params)
   ↓
2. Cursor calls client.submit_query(sql, params)
   ↓
3. Client ensures valid auth token
   ↓
4. Client makes HTTP POST to /query-sql
   ↓
5. Salesforce CDP processes query asynchronously
   ↓
6. Client returns query ID and initial results
   ↓
7. Cursor stores query state and metadata
   ↓
8. User calls fetchone/fetchmany/fetchall
   ↓
9. Cursor paginates through results via client.get_query_results()
```

### Authentication Flow

```
1. User creates connection with credentials
   ↓
2. AuthHandler authenticates via OAuth2
   ↓
3. Token and instance URL stored
   ↓
4. Before each API call, ensure_valid_token() called
   ↓
5. If token expired, re-authenticate automatically
   ↓
6. Return Authorization header with Bearer token
```

## Extending the Connector

### Adding New Authentication Methods

1. **Create new auth handler**:
```python
from .auth import AuthHandler

class JWTBearerAuth(AuthHandler):
    def __init__(self, private_key_path: str, client_id: str, username: str):
        self.private_key_path = private_key_path
        self.client_id = client_id
        self.username = username
        # ... initialization
    
    def authenticate(self) -> None:
        # Implement JWT token generation
        pass
    
    def get_headers(self) -> dict:
        # Return JWT Bearer token
        pass
    
    def get_instance_url(self) -> str:
        # Return instance URL
        pass
    
    def ensure_valid_token(self) -> None:
        # Check and refresh JWT token
        pass
```

2. **Update connection factory**:
```python
def connect(
    auth_method: str = "password",
    **kwargs
) -> Connection:
    if auth_method == "jwt":
        auth_handler = JWTBearerAuth(**kwargs)
    elif auth_method == "password":
        auth_handler = PasswordGrantAuth(**kwargs)
    # ... other methods
    
    client = SalesforceCDPClient(auth_handler)
    return Connection(client)
```

### Adding New Query Types

1. **Extend client methods**:
```python
class SalesforceCDPClient:
    def submit_dml_query(self, operation: str, table: str, data: dict) -> Dict[str, Any]:
        """Submit DML operations (INSERT, UPDATE, DELETE)"""
        payload = {
            "operation": operation,
            "table": table,
            "data": data
        }
        return self._request("POST", "/dml", json=payload)
```

2. **Add cursor support**:
```python
class Cursor:
    def execute_dml(self, operation: str, table: str, data: dict) -> None:
        """Execute DML operations"""
        response = self._client.submit_dml_query(operation, table, data)
        self._rowcount = response.get("affectedRows", -1)
```

## Implementing Alternative Protocols

The connector is designed to be **protocol-agnostic**. You can implement alternative transport layers (gRPC, WebSockets, etc.) by following these patterns:

### 1. Define Protocol Interface

Create an abstract base class for the protocol layer:

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union, Sequence

class CDPProtocolHandler(ABC):
    """Abstract base class for CDP protocol implementations."""
    
    @abstractmethod
    def submit_query(self, sql: str, params: Optional[Union[Sequence, Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Submit a SQL query and return initial response."""
        pass
    
    @abstractmethod
    def get_query_status(self, query_id: str) -> Dict[str, Any]:
        """Get the status of a query."""
        pass
    
    @abstractmethod
    def get_query_results(self, query_id: str, offset: int, limit: int) -> Dict[str, Any]:
        """Fetch paginated query results."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the protocol connection."""
        pass
```

### 2. Implement gRPC Client

```python
import grpc
from typing import Dict, Any, Optional, Union, Sequence
from .exceptions import ApiError, QueryError

class SalesforceCDPGRPCClient(CDPProtocolHandler):
    """gRPC implementation of CDP protocol handler."""
    
    def __init__(self, auth_handler: AuthHandler, grpc_endpoint: str):
        self.auth_handler = auth_handler
        self.grpc_endpoint = grpc_endpoint
        self._channel = None
        self._stub = None
        self._setup_grpc_connection()
    
    def _setup_grpc_connection(self):
        """Initialize gRPC channel and stub."""
        # Create secure channel with auth credentials
        credentials = self._create_grpc_credentials()
        self._channel = grpc.secure_channel(self.grpc_endpoint, credentials)
        
        # Import and create stub (you'd need to generate from .proto files)
        # from .generated import cdp_service_pb2_grpc
        # self._stub = cdp_service_pb2_grpc.CDPServiceStub(self._channel)
    
    def _create_grpc_credentials(self):
        """Create gRPC credentials from auth token."""
        # This would depend on your gRPC auth strategy
        # Could be SSL + Bearer token, or custom auth metadata
        pass
    
    def submit_query(self, sql: str, params: Optional[Union[Sequence, Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Submit query via gRPC."""
        try:
            # Convert to protobuf message
            # request = cdp_service_pb2.SubmitQueryRequest(
            #     sql=sql,
            #     parameters=params or []
            # )
            
            # Make gRPC call
            # response = self._stub.SubmitQuery(request)
            
            # Convert response to dict format expected by dbapi
            # return self._grpc_response_to_dict(response)
            
            # Placeholder implementation
            return {"status": {"queryId": "grpc_query_id", "state": "Running"}}
            
        except grpc.RpcError as e:
            self._handle_grpc_error(e)
    
    def get_query_status(self, query_id: str) -> Dict[str, Any]:
        """Get query status via gRPC."""
        try:
            # request = cdp_service_pb2.GetQueryStatusRequest(query_id=query_id)
            # response = self._stub.GetQueryStatus(request)
            # return self._grpc_response_to_dict(response)
            
            # Placeholder
            return {"status": {"queryId": query_id, "state": "Finished"}}
            
        except grpc.RpcError as e:
            self._handle_grpc_error(e)
    
    def get_query_results(self, query_id: str, offset: int, limit: int) -> Dict[str, Any]:
        """Get query results via gRPC."""
        try:
            # request = cdp_service_pb2.GetQueryResultsRequest(
            #     query_id=query_id,
            #     offset=offset,
            #     limit=limit
            # )
            # response = self._stub.GetQueryResults(request)
            # return self._grpc_response_to_dict(response)
            
            # Placeholder
            return {"data": [], "returnedRows": 0}
            
        except grpc.RpcError as e:
            self._handle_grpc_error(e)
    
    def _handle_grpc_error(self, error: grpc.RpcError):
        """Convert gRPC errors to domain exceptions."""
        if error.code() == grpc.StatusCode.UNAUTHENTICATED:
            raise AuthenticationError(f"gRPC authentication failed: {error.details()}")
        elif error.code() == grpc.StatusCode.INVALID_ARGUMENT:
            raise QueryError(f"gRPC query error: {error.details()}")
        else:
            raise ApiError(f"gRPC error ({error.code()}): {error.details()}")
    
    def close(self) -> None:
        """Close gRPC channel."""
        if self._channel:
            self._channel.close()
```

### 3. Update Connection Factory

```python
def connect(
    protocol: str = "http",
    **kwargs
) -> Connection:
    """Create connection with specified protocol."""
    
    # Create auth handler
    auth_handler = PasswordGrantAuth(**kwargs)
    
    # Create appropriate protocol handler
    if protocol == "grpc":
        grpc_endpoint = kwargs.get("grpc_endpoint", "localhost:9090")
        protocol_handler = SalesforceCDPGRPCClient(auth_handler, grpc_endpoint)
    else:  # default to HTTP
        protocol_handler = SalesforceCDPClient(auth_handler)
    
    return Connection(protocol_handler)
```

### 4. Protocol-Specific Considerations

#### gRPC Implementation Notes

1. **Protocol Buffers**: Define `.proto` files for your API schema
2. **Code Generation**: Use `protoc` to generate Python stubs
3. **Authentication**: Implement gRPC interceptors for auth headers
4. **Streaming**: Consider streaming for large result sets
5. **Error Handling**: Map gRPC status codes to domain exceptions

#### WebSocket Implementation Example

```python
import asyncio
import websockets
import json

class SalesforceCDPWebSocketClient(CDPProtocolHandler):
    """WebSocket implementation for real-time query updates."""
    
    def __init__(self, auth_handler: AuthHandler, ws_endpoint: str):
        self.auth_handler = auth_handler
        self.ws_endpoint = ws_endpoint
        self._websocket = None
        self._connected = False
    
    async def _connect(self):
        """Establish WebSocket connection."""
        headers = self.auth_handler.get_headers()
        self._websocket = await websockets.connect(
            self.ws_endpoint,
            extra_headers=headers
        )
        self._connected = True
    
    async def submit_query(self, sql: str, params=None) -> Dict[str, Any]:
        """Submit query via WebSocket."""
        if not self._connected:
            await self._connect()
        
        message = {
            "type": "submit_query",
            "sql": sql,
            "parameters": params or []
        }
        
        await self._websocket.send(json.dumps(message))
        response = await self._websocket.recv()
        return json.loads(response)
```

## Testing Strategy

### Unit Testing

1. **Mock Protocol Layer**: Test dbapi with mocked client
2. **Mock Authentication**: Test client with mocked auth handler
3. **Error Scenarios**: Test exception handling and error propagation

### Integration Testing

1. **Real API Calls**: Test against Salesforce CDP sandbox
2. **Protocol Switching**: Test HTTP vs gRPC implementations
3. **Authentication Flows**: Test token refresh and expiry

### Example Test Structure

```python
import pytest
from unittest.mock import Mock, patch
from .dbapi import Connection, Cursor
from .client import SalesforceCDPClient

class TestConnection:
    def test_connection_creation(self):
        mock_client = Mock(spec=SalesforceCDPClient)
        conn = Connection(mock_client)
        assert not conn._is_closed
    
    def test_cursor_creation(self):
        mock_client = Mock(spec=SalesforceCDPClient)
        conn = Connection(mock_client)
        cursor = conn.cursor()
        assert isinstance(cursor, Cursor)

class TestCursor:
    @patch('salesforcecdpconnector.client.SalesforceCDPClient')
    def test_query_execution(self, mock_client_class):
        mock_client = Mock()
        mock_client.submit_query.return_value = {
            "status": {"queryId": "test_id", "state": "Finished"},
            "metadata": [{"name": "col1", "type": "TEXT"}],
            "data": [["value1"]]
        }
        
        cursor = Cursor(mock_client, Mock())
        cursor.execute("SELECT * FROM test")
        
        assert cursor._query_id == "test_id"
        assert cursor._description is not None
```

## Best Practices

1. **Error Handling**: Always wrap protocol-specific errors in domain exceptions
2. **Resource Management**: Implement proper cleanup in protocol handlers
3. **Configuration**: Use dependency injection for protocol selection
4. **Logging**: Add comprehensive logging for debugging
5. **Type Safety**: Use type hints throughout the codebase
6. **Documentation**: Document protocol-specific requirements and limitations

## Future Enhancements

1. **Connection Pooling**: Implement connection pooling for high-throughput scenarios
2. **Async Support**: Add async/await support for non-blocking operations
3. **Caching**: Implement query result caching
4. **Metrics**: Add performance monitoring and metrics collection
5. **Plugin Architecture**: Support for third-party protocol implementations 