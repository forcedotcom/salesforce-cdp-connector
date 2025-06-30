# Building and Installing Salesforce CDP Connector Packages

This repository contains two separate Python packages in a monorepo structure:

1. **salesforcecdpconnector** - Main connector package with DBAPI interface
2. **salesforcecdpconnector-grpc** - gRPC client package

## Repository Structure

```
├── pyproject.toml                    # Root configuration (monorepo)
├── salesforce-cdp-connector/
│   ├── pyproject.toml               # Main package configuration
│   └── salesforcecdpconnector/
│       ├── __init__.py
│       ├── auth.py
│       ├── client.py
│       ├── dbapi.py
│       └── ...
├── salesforce-cdp-connector-grpc/
│   ├── pyproject.toml               # gRPC package configuration
│   └── src/
│       └── salesforcecdpconnector/
│           └── grpc/
│               └── client.py
└── Makefile                         # Build automation
```

## Quick Start

### Using Makefile (Recommended)

The easiest way to build and install both packages is using the provided Makefile:

```bash
# Setup development environment and install both packages
make dev

# Or step by step:
make dev-setup      # Install root dev dependencies
make install-dev    # Install both packages with dev dependencies

# Build both packages
make build

# Run tests for both packages
make test

# Run linting for both packages
make lint

# Format code for both packages
make format

# Clean build artifacts
make clean

# See all available commands
make help
```

### Manual Installation

#### Root Level (Recommended)
```bash
# Install root dev dependencies
pip install -e ".[dev]"

# Install both packages in development mode
make install-dev
```

#### Individual Packages

**Main Package (salesforcecdpconnector)**:
```bash
cd salesforce-cdp-connector
pip install -e .  # Development mode
# or
pip install -e ".[dev]"  # With dev dependencies
```

**gRPC Package (salesforcecdpconnector-grpc)**:
```bash
cd salesforce-cdp-connector-grpc
pip install -e .  # Development mode
# or
pip install -e ".[dev]"  # With dev dependencies
```

## Development Workflow

1. **Setup development environment**: `make dev`
2. **Make changes to code**
3. **Run tests**: `make test`
4. **Run linting**: `make lint`
5. **Format code**: `make format`

## Building for Distribution

To build distribution packages:

```bash
# Build both packages
make build

# This creates wheel and source distributions in:
# - salesforce-cdp-connector/dist/
# - salesforce-cdp-connector-grpc/dist/
```

## Available Make Targets

- `help` - Show all available commands
- `dev-setup` - Install root dev dependencies
- `dev` - Setup development environment and install packages
- `clean` - Remove build artifacts
- `build` - Build both packages
- `build-main` - Build main package only
- `build-grpc` - Build gRPC package only
- `install` - Install both packages in development mode
- `install-dev` - Install both packages with dev dependencies
- `test` - Run tests for both packages
- `lint` - Run linting for both packages
- `format` - Format code for both packages
- `all` - Clean, build, install, test, and lint both packages
- `check-installed` - Check if packages are installed
- `quick` - Quick install of both packages

## Root pyproject.toml Benefits

The root-level `pyproject.toml` provides:

- **Centralized tool configuration** (ruff, pytest, etc.)
- **Shared development dependencies**
- **Monorepo workspace support**
- **Consistent versioning across packages**
- **Better IDE and tooling support**

## Build System

This project uses the **modern Python build system** with:

- **setuptools>=61.0** - Modern setuptools with improved pyproject.toml support
- **build** - The standard Python build tool
- **setuptools_scm** - Version management from git tags

This provides:
- Faster builds
- Better error messages
- More reliable dependency resolution
- Full support for modern Python packaging standards (PEP 517/518)

## Package Dependencies

### Main Package (salesforcecdpconnector)
- pandas>=2.0.3
- pyarrow>=17.0.0
- pyjwt>=2.9.0
- python-dateutil>=2.9.0.post0
- requests>=2.32.3
- urllib3>=2.2.3
- loguru>=0.7.3

### gRPC Package (salesforcecdpconnector-grpc)
- grpcio>=1.60.0
- grpcio-tools>=1.60.0
- protobuf>=4.25.0
- loguru>=0.7.3

### Development Dependencies (Root)
- cryptography>=44.0.0
- pytest>=8.3.4
- pytest-cov>=5.0.0
- responses>=0.25.6
- ruff>=0.9.6
- build>=1.0.0
- twine>=4.0.0 