"""
End-to-End Test with Real Salesforce Data Cloud Instance.

This test performs a complete workflow:
1. Obtain refresh token via OAuth Authorization Code Flow
2. Use refresh token to authenticate
3. Query a real Data Cloud table
4. Verify results

NOTE: This test requires browser interaction for OAuth flow.
"""

import os
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import time
import webbrowser

import pytest
import requests

import salesforce_datacloud_connector as sfdc

pytestmark = pytest.mark.e2e


# Test configuration - Load from environment variables for security
# Set these before running tests:
#   export SFDC_LOGIN_URL='https://your-instance.salesforce.com'
#   export SFDC_CLIENT_ID='your_client_id'
#   export SFDC_CLIENT_SECRET='your_client_secret'
#   export SFDC_TABLE_NAME='your_table_name'  # Optional, defaults to 'Account__dll'
TEST_CONFIG = {
    "login_url": os.getenv("SFDC_LOGIN_URL", "https://login.salesforce.com"),
    "client_id": os.getenv("SFDC_CLIENT_ID"),
    "client_secret": os.getenv("SFDC_CLIENT_SECRET"),
    "redirect_uri": "http://localhost:33333/Callback",
    "table_name": os.getenv("SFDC_TABLE_NAME", "Account__dll"),
    "expected_min_rows": int(os.getenv("SFDC_EXPECTED_MIN_ROWS", "1000")),
}

# Validate required configuration. When the live-org env vars are missing we
# skip the entire module at collection time so unit/integration runs (including
# `pytest -m "not e2e"` in CI) don't fail on import.
if not TEST_CONFIG["client_id"] or not TEST_CONFIG["client_secret"]:
    pytest.skip(
        "Live Data Cloud E2E tests require SFDC_CLIENT_ID and "
        "SFDC_CLIENT_SECRET environment variables to be set.",
        allow_module_level=True,
    )

# Global to store auth code from OAuth callback
_auth_code = None


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """Handler for OAuth callback."""

    def do_GET(self):
        """Handle OAuth redirect."""
        global _auth_code

        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)

        if 'code' in params:
            _auth_code = params['code'][0]
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"<html><body><h1>Success! Close this window.</h1></body></html>")
        else:
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"<html><body><h1>Error during OAuth</h1></body></html>")

    def log_message(self, format, *args):
        """Suppress log messages."""
        pass


def get_refresh_token_from_env():
    """Get refresh token from environment variable if available."""
    return os.getenv("SFDC_REFRESH_TOKEN")


def get_refresh_token_via_oauth(login_url, client_id, client_secret, redirect_uri, timeout=120):
    """
    Get refresh token using OAuth Authorization Code Flow.

    Args:
        login_url: Salesforce login URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        redirect_uri: OAuth redirect URI
        timeout: Max time to wait for user interaction (seconds)

    Returns:
        Refresh token string, or None if failed

    Raises:
        TimeoutError: If user doesn't complete OAuth within timeout
    """
    global _auth_code
    _auth_code = None

    # Build authorization URL
    auth_url = f"{login_url}/services/oauth2/authorize"
    params = {
        'response_type': 'code',
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'scope': 'api refresh_token offline_access',
    }
    auth_url_full = f"{auth_url}?{urllib.parse.urlencode(params)}"

    # Start local server
    server = HTTPServer(('localhost', 33333), OAuthCallbackHandler)

    # Open browser in separate thread
    def open_browser():
        time.sleep(1)  # Give server time to start
        try:
            webbrowser.open(auth_url_full)
        except Exception:
            pass

    browser_thread = threading.Thread(target=open_browser, daemon=True)
    browser_thread.start()

    # Wait for callback with timeout
    start_time = time.time()
    while not _auth_code and (time.time() - start_time) < timeout:
        server.handle_request()
        if _auth_code:
            break
        time.sleep(0.1)

    if not _auth_code:
        raise TimeoutError(f"No OAuth callback received within {timeout} seconds")

    # Exchange code for tokens
    token_url = f"{login_url}/services/oauth2/token"
    data = {
        'grant_type': 'authorization_code',
        'code': _auth_code,
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_uri,
    }

    response = requests.post(token_url, data=data, timeout=30)
    response.raise_for_status()
    token_data = response.json()

    refresh_token = token_data.get('refresh_token')
    if not refresh_token:
        raise ValueError("No refresh token in OAuth response")

    return refresh_token


@pytest.fixture(scope="module")
def refresh_token():
    """
    Fixture to get refresh token.

    First tries environment variable, then OAuth flow if needed.
    """
    # Try environment variable first
    token = get_refresh_token_from_env()
    if token:
        return token

    # Fall back to OAuth flow (requires user interaction)
    print("\n" + "="*70)
    print("E2E Test: Getting Refresh Token")
    print("="*70)
    print("\nNo SFDC_REFRESH_TOKEN environment variable found.")
    print("Will attempt OAuth Authorization Code Flow...")
    print("\nA browser window will open. Please:")
    print("  1. Log in to Salesforce")
    print("  2. Click 'Allow' to authorize")
    print("  3. Wait for redirect to localhost")
    print("\nWaiting up to 120 seconds for authorization...")

    try:
        token = get_refresh_token_via_oauth(
            login_url=TEST_CONFIG["login_url"],
            client_id=TEST_CONFIG["client_id"],
            client_secret=TEST_CONFIG["client_secret"],
            redirect_uri=TEST_CONFIG["redirect_uri"],
            timeout=120
        )
        print(f"\n✅ Got refresh token: {token[:50]}...")
        return token
    except TimeoutError:
        pytest.skip("OAuth timeout - user didn't complete authorization in time")
    except Exception as e:
        pytest.skip(f"Could not get refresh token: {e}")


@pytest.fixture(scope="module")
def data_cloud_connection(refresh_token):
    """
    Fixture to create Data Cloud connection.

    Args:
        refresh_token: Refresh token from previous fixture

    Returns:
        Active connection to Data Cloud
    """
    conn = sfdc.connect(
        login_url=TEST_CONFIG["login_url"],
        auth_type="refresh_token",
        client_id=TEST_CONFIG["client_id"],
        client_secret=TEST_CONFIG["client_secret"],
        refresh_token=refresh_token
    )
    yield conn
    conn.close()


class TestE2EDataCloud:
    """End-to-end tests with real Data Cloud instance."""

    def test_01_connection(self, data_cloud_connection):
        """Test that connection is established."""
        assert data_cloud_connection is not None
        assert not data_cloud_connection.closed
        assert isinstance(data_cloud_connection, sfdc.Connection)

    def test_02_cursor_creation(self, data_cloud_connection):
        """Test that cursor can be created."""
        cursor = data_cloud_connection.cursor()
        assert cursor is not None
        cursor.close()

    def test_03_simple_query(self, data_cloud_connection):
        """Test simple SELECT query."""
        cursor = data_cloud_connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_CONFIG['table_name']}")
        result = cursor.fetchone()

        assert result is not None
        assert len(result) == 1
        assert isinstance(result[0], (int, float))
        assert result[0] > 0
        cursor.close()

    def test_04_table_row_count(self, data_cloud_connection):
        """Test that table has expected number of rows."""
        cursor = data_cloud_connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {TEST_CONFIG['table_name']}")
        count = cursor.fetchone()[0]

        assert count >= TEST_CONFIG["expected_min_rows"], \
            f"Expected at least {TEST_CONFIG['expected_min_rows']} rows, got {count}"
        cursor.close()

    def test_05_column_metadata(self, data_cloud_connection):
        """Test that column metadata is available."""
        cursor = data_cloud_connection.cursor()
        cursor.execute(f"SELECT * FROM {TEST_CONFIG['table_name']} LIMIT 1")
        cursor.fetchone()

        # Check cursor.description
        assert cursor.description is not None
        assert len(cursor.description) > 0

        # Verify description format (name, type_code, ...)
        first_col = cursor.description[0]
        assert len(first_col) == 7  # DB-API 2.0 format
        assert isinstance(first_col[0], str)  # Column name
        cursor.close()

    def test_06_fetch_data(self, data_cloud_connection):
        """Test fetching actual data rows."""
        cursor = data_cloud_connection.cursor()
        cursor.execute(f"SELECT * FROM {TEST_CONFIG['table_name']} LIMIT 10")

        rows = cursor.fetchall()
        assert len(rows) == 10
        assert all(isinstance(row, tuple) for row in rows)

        # Verify data structure
        first_row = rows[0]
        assert len(first_row) > 0
        cursor.close()

    def test_07_fetch_methods(self, data_cloud_connection):
        """Test different fetch methods."""
        cursor = data_cloud_connection.cursor()
        cursor.execute(f"SELECT * FROM {TEST_CONFIG['table_name']} LIMIT 10")

        # fetchone
        row1 = cursor.fetchone()
        assert row1 is not None

        # fetchmany
        rows_many = cursor.fetchmany(3)
        assert len(rows_many) == 3

        # fetchall (remaining)
        rows_all = cursor.fetchall()
        assert len(rows_all) == 6  # 10 - 1 - 3 = 6 remaining

        cursor.close()

    def test_08_iterator(self, data_cloud_connection):
        """Test cursor iteration."""
        cursor = data_cloud_connection.cursor()
        cursor.execute(f"SELECT * FROM {TEST_CONFIG['table_name']} LIMIT 5")

        count = 0
        for row in cursor:
            count += 1
            assert isinstance(row, tuple)

        assert count == 5
        cursor.close()

    def test_09_where_clause(self, data_cloud_connection):
        """Test filtering with WHERE clause."""
        cursor = data_cloud_connection.cursor()

        # Query with WHERE clause
        cursor.execute(
            f"SELECT COUNT(*) FROM {TEST_CONFIG['table_name']} WHERE Sex__c = 'Male'"
        )
        male_count = cursor.fetchone()[0]

        cursor.execute(
            f"SELECT COUNT(*) FROM {TEST_CONFIG['table_name']} WHERE Sex__c = 'Female'"
        )
        female_count = cursor.fetchone()[0]

        # Both should have data
        assert male_count > 0
        assert female_count > 0

        # Should be roughly equal (this is mortality data by sex)
        ratio = male_count / female_count
        assert 0.8 < ratio < 1.2, "Male/Female counts should be similar"

        cursor.close()

    def test_10_group_by(self, data_cloud_connection):
        """Test GROUP BY aggregation."""
        cursor = data_cloud_connection.cursor()
        cursor.execute(
            f"SELECT Sex__c, COUNT(*) as count FROM {TEST_CONFIG['table_name']} "
            f"GROUP BY Sex__c"
        )

        results = cursor.fetchall()
        assert len(results) >= 2  # At least Male, Female (maybe Both)

        # Verify structure
        for row in results:
            sex, count = row
            assert isinstance(sex, str)
            assert isinstance(count, (int, float))
            assert count > 0

        cursor.close()

    def test_11_order_by(self, data_cloud_connection):
        """Test ORDER BY sorting."""
        cursor = data_cloud_connection.cursor()
        cursor.execute(
            f"SELECT Year__c FROM {TEST_CONFIG['table_name']} "
            f"ORDER BY Year__c LIMIT 10"
        )

        years = [row[0] for row in cursor.fetchall()]
        assert len(years) == 10

        # Verify sorted
        assert years == sorted(years), "Results should be sorted by year"
        cursor.close()

    def test_12_limit_offset(self, data_cloud_connection):
        """Test LIMIT for pagination."""
        cursor = data_cloud_connection.cursor()

        # First page
        cursor.execute(f"SELECT * FROM {TEST_CONFIG['table_name']} LIMIT 5")
        page1 = cursor.fetchall()
        assert len(page1) == 5

        cursor.close()

    def test_13_multiple_cursors(self, data_cloud_connection):
        """Test using multiple cursors simultaneously."""
        cursor1 = data_cloud_connection.cursor()
        cursor2 = data_cloud_connection.cursor()

        cursor1.execute(f"SELECT COUNT(*) FROM {TEST_CONFIG['table_name']}")
        cursor2.execute(f"SELECT COUNT(*) FROM {TEST_CONFIG['table_name']}")

        count1 = cursor1.fetchone()[0]
        count2 = cursor2.fetchone()[0]

        assert count1 == count2

        cursor1.close()
        cursor2.close()

    def test_14_context_manager(self, data_cloud_connection):
        """Test cursor context manager."""
        with data_cloud_connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TEST_CONFIG['table_name']}")
            count = cursor.fetchone()[0]
            assert count > 0
        # Cursor should be closed after context

    def test_15_real_data_validation(self, data_cloud_connection):
        """Test that we're getting real mortality data."""
        cursor = data_cloud_connection.cursor()
        cursor.execute(f"SELECT * FROM {TEST_CONFIG['table_name']} LIMIT 5")

        # Get column names
        columns = [desc[0] for desc in cursor.description]

        # Verify expected columns exist
        expected_columns = ['Age_Group__c', 'Sex__c', 'Year__c', 'Country_Name__c']
        for expected_col in expected_columns:
            assert expected_col in columns, f"Expected column {expected_col} not found"

        # Fetch and verify data
        rows = cursor.fetchall()
        assert len(rows) == 5

        # Check that we have actual data (not all nulls)
        for row in rows:
            non_null_values = [v for v in row if v is not None]
            assert len(non_null_values) > 0, "Row should have some non-null values"

        cursor.close()


class TestE2EEdgeCases:
    """Test edge cases and error conditions."""

    def test_invalid_table_name(self, data_cloud_connection):
        """Test querying non-existent table."""
        cursor = data_cloud_connection.cursor()

        with pytest.raises(sfdc.ProgrammingError):
            cursor.execute("SELECT * FROM NonexistentTable123")

        cursor.close()

    def test_invalid_sql_syntax(self, data_cloud_connection):
        """Test invalid SQL syntax."""
        cursor = data_cloud_connection.cursor()

        with pytest.raises(sfdc.ProgrammingError):
            cursor.execute("INVALID SQL QUERY HERE")

        cursor.close()

    def test_unsupported_operation(self, data_cloud_connection):
        """Test that write operations are not supported."""
        cursor = data_cloud_connection.cursor()

        with pytest.raises(sfdc.NotSupportedError):
            cursor.execute("INSERT INTO test VALUES (1, 2, 3)")

        cursor.close()

    def test_closed_cursor_operations(self, data_cloud_connection):
        """Test operations on closed cursor."""
        cursor = data_cloud_connection.cursor()
        cursor.close()

        with pytest.raises(sfdc.InterfaceError):
            cursor.execute("SELECT 1")

        with pytest.raises(sfdc.InterfaceError):
            cursor.fetchone()


def test_e2e_summary(capsys):
    """Print summary of E2E test configuration."""
    print("\n" + "="*70)
    print("End-to-End Test Configuration")
    print("="*70)
    print(f"Login URL: {TEST_CONFIG['login_url']}")
    print(f"Table: {TEST_CONFIG['table_name']}")
    print(f"Expected min rows: {TEST_CONFIG['expected_min_rows']:,}")
    print(f"Client ID: {TEST_CONFIG['client_id'][:20]}...")
    print("="*70)
    print("\nTo run these tests:")
    print("  pytest tests/test_e2e_real_datacloud.py -v")
    print("\nTo skip OAuth (use existing token):")
    print("  export SFDC_REFRESH_TOKEN='your_token_here'")
    print("  pytest tests/test_e2e_real_datacloud.py -v")
