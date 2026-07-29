"""Tests for package metadata and version information."""

import salesforce_datacloud_connector as sfdc


def test_version_is_set():
    """Test that __version__ is defined."""
    assert hasattr(sfdc, "__version__")
    assert isinstance(sfdc.__version__, str)
    assert len(sfdc.__version__) > 0


def test_version_format():
    """Test that __version__ follows semantic versioning."""
    version = sfdc.__version__

    # Should match pattern: MAJOR.MINOR.PATCH[alpha/beta/rc suffix]
    # e.g., "2.0.0b2" or "2.0.0"
    parts = version.split(".")
    assert len(parts) >= 2, f"Version should have at least MAJOR.MINOR: {version}"

    # Major and minor should be numeric (patch may have suffix like 0b2)
    assert parts[0].isdigit(), f"Major version should be numeric: {parts[0]}"
    assert parts[1].isdigit(), f"Minor version should be numeric: {parts[1]}"


def test_version_matches_pyproject():
    """Test that __version__ matches pyproject.toml version."""
    try:
        import tomllib  # Python 3.11+ standard library
    except ModuleNotFoundError:
        import tomli as tomllib  # Python 3.8-3.10 backport (dev dependency)
    from pathlib import Path

    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)

    pyproject_version = pyproject["project"]["version"]
    module_version = sfdc.__version__

    assert module_version == pyproject_version, (
        f"Version mismatch: __init__.__version__ = {module_version}, "
        f"pyproject.toml version = {pyproject_version}"
    )
