# Makefile for Salesforce CDP Connector packages
# Supports both salesforcecdpconnector and salesforcecdpconnector-grpc packages

.PHONY: help clean build build-main build-grpc install install-main install-grpc install-dev install-dev-main install-dev-grpc uninstall uninstall-main uninstall-grpc test test-main test-grpc lint lint-main lint-grpc all

# Default target
help:
	@echo "Available targets:"
	@echo "  build        - Build both packages"
	@echo "  build-main   - Build main salesforcecdpconnector package"
	@echo "  build-grpc   - Build salesforcecdpconnector-grpc package"
	@echo "  install      - Install both packages in development mode"
	@echo "  install-main - Install main package in development mode"
	@echo "  install-grpc - Install gRPC package in development mode"
	@echo "  install-dev  - Install both packages with dev dependencies"
	@echo "  install-dev-main - Install main package with dev dependencies"
	@echo "  install-dev-grpc - Install gRPC package with dev dependencies"
	@echo "  uninstall    - Uninstall both packages"
	@echo "  uninstall-main - Uninstall main package"
	@echo "  uninstall-grpc - Uninstall gRPC package"
	@echo "  test         - Run tests for both packages"
	@echo "  test-main    - Run tests for main package"
	@echo "  test-grpc    - Run tests for gRPC package"
	@echo "  lint         - Run linting for both packages"
	@echo "  lint-main    - Run linting for main package"
	@echo "  lint-grpc    - Run linting for gRPC package"
	@echo "  format       - Format code for both packages"
	@echo "  clean        - Clean build artifacts"
	@echo "  all          - Build, install, and test both packages"
	@echo "  dev-setup    - Setup development environment (install dev deps)"

# Check if build module is available
check-build:
	@python -c "import build" 2>/dev/null || (echo "Installing build..." && pip install build)

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf salesforce-cdp-connector/build/
	rm -rf salesforce-cdp-connector/dist/
	rm -rf salesforce-cdp-connector/*.egg-info/
	rm -rf salesforce-cdp-connector-grpc/build/
	rm -rf salesforce-cdp-connector-grpc/dist/
	rm -rf salesforce-cdp-connector-grpc/*.egg-info/
	rm -rf salesforce-cdp-connector-grpc/src/*.egg-info/
	rm -rf src/*.egg-info/
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	@echo "Clean complete!"

# Build targets
build: check-build build-main build-grpc

build-main: check-build
	@echo "Building salesforcecdpconnector..."
	cd salesforce-cdp-connector && python -m build

build-grpc: check-build
	@echo "Building salesforcecdpconnector-grpc..."
	cd salesforce-cdp-connector-grpc && python -m build

# Install targets (development mode)
install: install-main install-grpc

install-main:
	@echo "Installing salesforcecdpconnector in development mode..."
	cd salesforce-cdp-connector && pip install -e .

install-grpc:
	@echo "Installing salesforcecdpconnector-grpc in development mode..."
	cd salesforce-cdp-connector-grpc && pip install -e .

# Install targets with dev dependencies
install-dev: install-dev-main install-dev-grpc

install-dev-main:
	@echo "Installing salesforcecdpconnector with dev dependencies..."
	cd salesforce-cdp-connector && pip install -e ".[dev]"

install-dev-grpc:
	@echo "Installing salesforcecdpconnector-grpc with dev dependencies..."
	cd salesforce-cdp-connector-grpc && pip install -e ".[dev]"

# Uninstall targets
uninstall: uninstall-main uninstall-grpc

uninstall-main:
	@echo "Uninstalling salesforcecdpconnector..."
	pip uninstall -y salesforce-cdp-connector || true

uninstall-grpc:
	@echo "Uninstalling salesforcecdpconnector-grpc..."
	pip uninstall -y salesforce-cdp-connector-grpc || true

# Development setup - install root dev dependencies
dev-setup:
	@echo "Setting up development environment..."
	pip install -e ".[dev]"
	@echo "Development environment setup complete!"

# Test targets
test: test-main test-grpc

test-main:
	@echo "Running tests for salesforcecdpconnector..."
	cd salesforce-cdp-connector && python -m pytest tests/ -v

test-grpc:
	@echo "Running tests for salesforcecdpconnector-grpc..."
	cd salesforce-cdp-connector-grpc && python -m pytest tests/ -v

# Lint targets
lint: lint-main lint-grpc

lint-main:
	@echo "Running linting for salesforcecdpconnector..."
	cd salesforce-cdp-connector && python -m ruff check .

lint-grpc:
	@echo "Running linting for salesforcecdpconnector-grpc..."
	cd salesforce-cdp-connector-grpc && python -m ruff check .

# Format targets
format: format-main format-grpc

format-main:
	@echo "Formatting salesforcecdpconnector..."
	cd salesforce-cdp-connector && python -m ruff format .

format-grpc:
	@echo "Formatting salesforcecdpconnector-grpc..."
	cd salesforce-cdp-connector-grpc && python -m ruff format .

# All-in-one target
all: clean build install-dev test lint
	@echo "All tasks completed!"

# Check if packages are installed
check-installed:
	@echo "Checking installed packages..."
	@python -c "import salesforcecdpconnector; print('✓ salesforcecdpconnector installed')" 2>/dev/null || echo "✗ salesforcecdpconnector not installed"
	@python -c "import salesforcecdpconnector.grpc; print('✓ salesforcecdpconnector-grpc installed')" 2>/dev/null || echo "✗ salesforcecdpconnector-grpc not installed"

# Quick development workflow
dev: dev-setup install-dev
	@echo "Development environment ready!"

# Build and install both packages quickly
quick: install-dev
	@echo "Quick install complete!" 