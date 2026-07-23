"""
End-to-End Smoke Test — Off-Core Query v3 (JWT Auth)

This test verifies the full off-core query path:
1. Authenticate via JWT bearer token
2. Exchange core token for CDP token + tenant endpoint
3. Execute a synthetic query against off-core v3 (`generate_series`)
4. Verify results

Requires environment variables:
  SFDC_LOGIN_URL        — e.g., "https://login.salesforce.com"
  SFDC_USERNAME         — Connected app user
  SFDC_CLIENT_ID        — Connected app client ID
  SFDC_JWT_PRIVATE_KEY  — Path to private key PEM file, or literal key
  SFDC_DATASPACE        — Optional dataspace (default: "default")

Synthetic query pattern (no tenant data required):
  SELECT n FROM generate_series(1, 10) AS t(n)

This test is gated by @pytest.mark.e2e and skipped in CI.
"""

import os
import pytest

import salesforce_datacloud_connector as sfdc

pytestmark = pytest.mark.e2e


# Test configuration from environment
TEST_CONFIG = {
    "login_url": os.getenv("SFDC_LOGIN_URL"),
    "username": os.getenv("SFDC_USERNAME"),
    "client_id": os.getenv("SFDC_CLIENT_ID"),
    "jwt_private_key_path": os.getenv("SFDC_JWT_PRIVATE_KEY_PATH"),
    "jwt_private_key": os.getenv("SFDC_JWT_PRIVATE_KEY"),
    "dataspace": os.getenv("SFDC_DATASPACE", "default"),
    "workload": "python-connector-e2e-smoke",
}


def get_private_key():
    """Load private key from path or env var."""
    if TEST_CONFIG["jwt_private_key"]:
        return TEST_CONFIG["jwt_private_key"]
    elif TEST_CONFIG["jwt_private_key_path"]:
        with open(TEST_CONFIG["jwt_private_key_path"], "r") as f:
            return f.read()
    else:
        return None


# Validate required configuration at module load time
if not all([TEST_CONFIG["login_url"], TEST_CONFIG["username"], TEST_CONFIG["client_id"]]):
    pytest.skip(
        "Off-core e2e smoke test requires SFDC_LOGIN_URL, SFDC_USERNAME, "
        "and SFDC_CLIENT_ID environment variables.",
        allow_module_level=True,
    )

if not get_private_key():
    pytest.skip(
        "Off-core e2e smoke test requires SFDC_JWT_PRIVATE_KEY or "
        "SFDC_JWT_PRIVATE_KEY_PATH environment variable.",
        allow_module_level=True,
    )


@pytest.fixture(scope="module")
def offcore_connection():
    """Create connection to off-core v3 via JWT auth."""
    conn = sfdc.connect(
        login_url=TEST_CONFIG["login_url"],
        auth_type="jwt",
        username=TEST_CONFIG["username"],
        client_id=TEST_CONFIG["client_id"],
        jwt_private_key=get_private_key(),
        dataspace=TEST_CONFIG["dataspace"],
        workload=TEST_CONFIG["workload"],
    )
    yield conn
    conn.close()


class TestOffCoreSmoke:
    """Smoke tests for off-core v3 query path."""

    def test_connection_established(self, offcore_connection):
        """Test that connection is established."""
        assert offcore_connection is not None
        assert not offcore_connection.closed
        assert isinstance(offcore_connection, sfdc.Connection)

    def test_cursor_creation(self, offcore_connection):
        """Test that cursor can be created."""
        cursor = offcore_connection.cursor()
        assert cursor is not None
        cursor.close()

    def test_synthetic_query_generate_series(self, offcore_connection):
        """
        Test synthetic query via generate_series (no tenant data required).

        This verifies the full off-core v3 stack:
        - JWT auth → core token
        - Token exchange → CDP token + tenant endpoint
        - POST /api/v3/query with SQL
        - Parse x-hyperdb-status header
        - Fetch results
        """
        cursor = offcore_connection.cursor()

        # Execute synthetic query
        cursor.execute("SELECT n FROM generate_series(1, 10) AS t(n)")

        # Verify cursor.description (metadata)
        assert cursor.description is not None
        assert len(cursor.description) == 1
        col_name, col_type, *_ = cursor.description[0]
        assert col_name.lower() == "n"
        assert col_type == sfdc.NUMBER  # generate_series returns integers

        # Fetch results
        rows = cursor.fetchall()
        assert len(rows) == 10

        # Verify data: should be [(1,), (2,), ..., (10,)]
        expected = [(i,) for i in range(1, 11)]
        assert rows == expected

        cursor.close()

    def test_synthetic_query_count(self, offcore_connection):
        """Test COUNT aggregation over synthetic data."""
        cursor = offcore_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM generate_series(1, 100) AS t(n)")

        result = cursor.fetchone()
        assert result is not None
        assert len(result) == 1
        assert result[0] == 100

        cursor.close()

    def test_synthetic_query_with_filter(self, offcore_connection):
        """Test WHERE clause over synthetic data."""
        cursor = offcore_connection.cursor()
        cursor.execute(
            "SELECT n FROM generate_series(1, 20) AS t(n) WHERE n > 15 ORDER BY n"
        )

        rows = cursor.fetchall()
        assert len(rows) == 5  # 16, 17, 18, 19, 20
        assert rows == [(16,), (17,), (18,), (19,), (20,)]

        cursor.close()

    def test_fetch_methods(self, offcore_connection):
        """Test fetchone/fetchmany/fetchall."""
        cursor = offcore_connection.cursor()
        cursor.execute("SELECT n FROM generate_series(1, 10) AS t(n)")

        # fetchone
        row1 = cursor.fetchone()
        assert row1 == (1,)

        # fetchmany(3)
        rows_many = cursor.fetchmany(3)
        assert rows_many == [(2,), (3,), (4,)]

        # fetchall (remaining)
        rows_all = cursor.fetchall()
        assert rows_all == [(5,), (6,), (7,), (8,), (9,), (10,)]

        cursor.close()

    def test_cursor_iteration(self, offcore_connection):
        """Test cursor iteration over results."""
        cursor = offcore_connection.cursor()
        cursor.execute("SELECT n FROM generate_series(1, 5) AS t(n)")

        collected = []
        for row in cursor:
            collected.append(row)

        assert collected == [(1,), (2,), (3,), (4,), (5,)]
        cursor.close()

    def test_multiple_cursors(self, offcore_connection):
        """Test multiple cursors on same connection."""
        cursor1 = offcore_connection.cursor()
        cursor2 = offcore_connection.cursor()

        cursor1.execute("SELECT COUNT(*) FROM generate_series(1, 10) AS t(n)")
        cursor2.execute("SELECT COUNT(*) FROM generate_series(1, 10) AS t(n)")

        count1 = cursor1.fetchone()[0]
        count2 = cursor2.fetchone()[0]

        assert count1 == count2 == 10

        cursor1.close()
        cursor2.close()

    def test_context_manager(self, offcore_connection):
        """Test cursor context manager."""
        with offcore_connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM generate_series(1, 5) AS t(n)")
            count = cursor.fetchone()[0]
            assert count == 5
        # Cursor should be closed after context

    def test_error_invalid_sql(self, offcore_connection):
        """Test that invalid SQL raises ProgrammingError."""
        cursor = offcore_connection.cursor()

        with pytest.raises(sfdc.ProgrammingError):
            cursor.execute("INVALID SQL SYNTAX HERE")

        cursor.close()

    def test_error_invalid_table(self, offcore_connection):
        """Test that non-existent table raises ProgrammingError."""
        cursor = offcore_connection.cursor()

        with pytest.raises(sfdc.ProgrammingError):
            cursor.execute("SELECT * FROM NonexistentTable123")

        cursor.close()


def test_smoke_summary(capsys):
    """Print summary of e2e smoke test configuration."""
    print("\n" + "="*70)
    print("Off-Core v3 E2E Smoke Test Configuration")
    print("="*70)
    print(f"Login URL: {TEST_CONFIG['login_url']}")
    print(f"Username: {TEST_CONFIG['username']}")
    print(f"Client ID: {TEST_CONFIG['client_id'][:20]}..." if TEST_CONFIG['client_id'] else "Client ID: (not set)")
    print(f"Dataspace: {TEST_CONFIG['dataspace']}")
    print(f"Workload: {TEST_CONFIG['workload']}")
    print(f"Private key: {'(loaded from path)' if TEST_CONFIG['jwt_private_key_path'] else '(loaded from env)'}")
    print("="*70)
    print("\nTo run this test:")
    print("  pytest tests/test_e2e_offcore_smoke.py -v")
    print("\nOr with explicit e2e marker:")
    print("  pytest -m e2e tests/test_e2e_offcore_smoke.py -v")
