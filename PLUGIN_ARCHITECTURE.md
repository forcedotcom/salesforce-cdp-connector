# Salesforce CDP Connector Plugin Architecture

This document describes the plugin-based architecture of the Salesforce CDP Connector, which allows support for multiple protocols (Connect API/REST, gRPC, and potentially others) while maintaining clean separation and backward compatibility.

## Overview

The Salesforce CDP Connector has been refactored from a monolithic implementation to a plugin-based architecture that supports multiple transport protocols:

- **Connect API (REST)**: The original REST API implementation
- **gRPC**: A new gRPC-based implementation for improved performance
- **Future protocols**: Extensible architecture for additional protocols

## Architecture Components

### 1. Abstract Base Classes (`base.py`)

The core of the plugin architecture consists of abstract base classes that define the PEP 249 interface:

- `BaseConnection`: Abstract base class for connection implementations
- `BaseCursor`: Abstract base class for cursor implementations
- `BaseAuthHandlder`: Abstract base class for authentication implementations


### 2. Plugin System

Plugins are organized in the following structure:

```
salesforcecdpconnector/
├── src/
│   └── salesforcecdpconnector/ # Namespace package
│    ├── dbapi.py                    # Abstract base classes and plugin registry
│    ├── auth.py                     # Authentication handlers (shared)
│    ├── exceptions.py               # Exception classes (shared)
│    ├── types.py                    # Type definitions (shared)
│    ├── constants.py                # Constants (shared)
│    ├── connect_api/                # Connect API (REST) plugin
│    │   ├── __init__.py
│    │   ├── connection.py
│    │   ├── cursor.py
│    │   └── client.py               # REST-specific client implementation
│    └── __init__.py                 # Registers default plugins

salesforcecdpconnector-grpc/    # gRPC plugin package
├── src/
│   └── salesforcecdpconnector/ # Namespace package
│       ├── __init__.py         # Namespace package declaration
│       └── grpc/               # gRPC plugin
│           ├── __init__.py
│           ├── connection.py
│           ├── cursor.py
│           └── client.py       # gRPC-specific client implementation
```

### 3. Plugin Discovery

Plugins are discovered using Python entry points:

```toml
# Main package (salesforcecdpconnector)
[project.entry-points."salesforcecdpconnector.connections"]
connect_api = "salesforcecdpconnector.connect_api:Connection"

# gRPC package (salesforcecdpconnector-grpc)
[project.entry-points."salesforcecdpconnector.connections"]
grpc = "salesforcecdpconnector.grpc:Connection"
```

### 4. Auto-Discovery Logic

The `connect()` function automatically discovers and uses the best available plugin:

1. **Connect API**
2. **gRPC**
3. **Error** if no plugins are available

Feel free to bake in gRPC code in to the core package as default and move Connect API code to a package outside.

## Design Principles

### Separation of Concerns

- **Core package**: Contains only abstract base classes, shared utilities, and plugin infrastructure
- **Protocol-specific code**: Each plugin contains its own client implementation and protocol-specific logic
- **Shared components**: Authentication, exceptions, types, and constants are shared across plugins

### Plugin Independence

Each plugin is self-contained and includes:
- Its own client, Connection, and Cursor implementation
- Protocol-specific connection and cursor implementations
- Any protocol-specific dependencies

This allows plugins to be developed, tested, and deployed independently.

## Usage

### Basic Usage (Auto-Discovery)

```python
import salesforcecdpconnector.dbapi as cdp

# Automatically uses the best available plugin
cdp = connect(
    login_url="https://login.salesforce.com",
    client_id="your_client_id",
    client_secret="your_client_secret",
    username="your_username",
    password="your_password"
)
```

### Explicit Protocol Selection

```python
# Use Connect API (REST) explicitly
conn = connect(
    protocol='connect_api',
    login_url="https://login.salesforce.com",
    client_id="your_client_id",
    client_secret="your_client_secret",
    username="your_username",
    password="your_password"
)

# Use gRPC explicitly (if available)
conn = connect(
    protocol='grpc',
    login_url="https://login.salesforce.com",
    client_id="your_client_id",
    client_secret="your_client_secret",
    username="your_username",
    password="your_password"
)
```

### Installation Options

#### Basic Installation (Connect API only)
```bash
pip install salesforcecdpconnector
```

#### With gRPC Support
```bash
pip install salesforcecdpconnector[grpc]
```


## Package Strategy

### Main Package (`salesforcecdpconnector`)
- Contains abstract base classes and plugin infrastructure
- Includes Connect API (REST) plugin by default
- Provides shared components (auth, exceptions, types, constants)
- Registers Connect API plugin automatically

### gRPC Package (`salesforcecdpconnector-grpc`)
- Separate package for gRPC support
- Contains gRPC-specific client implementation
- Registers gRPC plugin via entry points
- Depends on the main package for base classes and shared components
