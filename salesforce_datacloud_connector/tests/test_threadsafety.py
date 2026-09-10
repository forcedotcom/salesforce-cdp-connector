"""
End-to-end thread-safety tests for the DB-API 2.0 driver (threadsafety=2).

threadsafety=2 (PEP 249) means threads may share the module AND connections,
but not cursors. These tests tie the per-component guarantees together:

- the module advertises threadsafety == 2;
- many threads sharing ONE Connection can each open their own cursor and run a
  query end-to-end without their rows bleeding across;
- two connection-scoped token exchangers sharing ONE core authenticator can
  fetch concurrently without error.

Cursors are thread-confined by design and are intentionally NOT shared here.
"""

import json
from unittest.mock import Mock, patch

import salesforce_datacloud_connector as sfdc
from salesforce_datacloud_connector.auth.oauth import UsernamePasswordAuthenticator
from salesforce_datacloud_connector.auth.token_exchanger import DataCloudTokenExchanger
from salesforce_datacloud_connector.connection import Connection
from tests._concurrency_helpers import ConcurrencyGate


class _FakeResponse:
    """Minimal thread-safe stand-in for requests.Response (``responses`` is not
    thread-safe, so the concurrency tests patch requests.request directly)."""

    def __init__(self, status_code, json_body, headers=None):
        self.status_code = status_code
        self._json = json_body
        self.headers = headers or {}
        self.text = ""

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        return self._json


def _status_header(query_id):
    return {
        "x-hyperdb-status": json.dumps(
            {
                "queryId": query_id,
                "completionStatus": "RESULTS_PRODUCED",
                "progress": 1.0,
                "rowCount": 1,
                "chunkCount": 1,
            }
        )
    }


def _mock_token_provider():
    """A token provider whose tenant endpoint / CDP token are pre-resolved, so
    Connection construction needs no network."""
    provider = Mock()
    provider.get_tenant_endpoint.return_value = "https://test.c360a.salesforce.com"
    provider.get_cdp_token.return_value = "mock_cdp_token"
    return provider


def test_module_advertises_threadsafety_2():
    """PEP 249: threadsafety == 2 tells callers a single Connection is safe to
    share across threads (cursors are not). This is the headline promise of the
    whole change; it stays 1 until the module global is flipped."""
    assert sfdc.threadsafety == 2


def test_shared_connection_end_to_end_queries_keep_rows_distinct():
    """Many threads share ONE Connection: each opens its own cursor, runs its own
    query, and fetches its own rows end-to-end (execute → fetchall). No thread
    sees another's rows — the concrete threadsafety=2 guarantee."""
    conn = Connection(_mock_token_provider())
    parties = 12
    gate = ConcurrencyGate()

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        # The per-thread marker rides in the SQL; echo it back in both the row
        # data and the status header. Hold every caller inside the request so the
        # responses are genuinely built concurrently on the shared connection.
        sql = json["sql"]
        gate.enter()
        marker = sql.split("'")[1]
        return _FakeResponse(
            200,
            {
                "metadata": {"columns": [{"name": "m", "type": "varchar", "nullable": True}]},
                "data": [[marker]],
                "returnedRows": 1,
            },
            headers=_status_header(f"query_{marker}"),
        )

    def worker(i):
        cursor = conn.cursor()
        cursor.execute(f"SELECT '{i}' AS m")
        rows = cursor.fetchall()
        cursor.close()
        return rows

    # Patch once in the main thread: patch() setattrs requests.request globally so
    # the fake is visible to every worker; all threads start+join inside it.
    with patch("requests.request", side_effect=fake_request):
        results, errors = gate.run(worker, parties)

    assert errors == []
    assert gate.entered == parties
    assert gate.max_concurrent == parties  # all genuinely in-flight on one conn
    # Every thread got exactly its own row back — no cross-talk.
    for i, rows in enumerate(results):
        assert rows == [(str(i),)]
    assert not conn.closed


def test_two_exchangers_sharing_one_authenticator_are_concurrency_safe():
    """Two connection-scoped DataCloudTokenExchangers wrapping ONE shared core
    authenticator can exchange concurrently without error. Each exchanger single-
    flights its own cold miss; the shared authenticator serves every core fetch
    safely (matches the real topology of two Connections over one authenticator)."""
    auth = UsernamePasswordAuthenticator(
        login_url="https://test.salesforce.com",
        username="test@example.com",
        password="password123",
        client_id="client_id",
        client_secret="client_secret",
    )
    exchanger_a = DataCloudTokenExchanger(core_authenticator=auth, dataspace="a")
    exchanger_b = DataCloudTokenExchanger(core_authenticator=auth, dataspace="b")
    exchangers = [exchanger_a, exchanger_b]
    parties = 16
    gate = ConcurrencyGate()

    def fake_fetch():
        return ("core_token", 7200, "https://myorg.my.salesforce.com")

    def make_fake_exchange(tag):
        def fake_exchange(instance_url, core_token):
            gate.enter()
            return (f"cdp_{tag}", 3600, f"https://{tag}.c360a.salesforce.com")

        return fake_exchange

    def worker(i):
        exchanger = exchangers[i % 2]
        return exchanger.get_cdp_token()

    with patch.object(auth, "_fetch_new_token", side_effect=fake_fetch), patch.object(
        exchanger_a, "_exchange_token", side_effect=make_fake_exchange("a")
    ), patch.object(exchanger_b, "_exchange_token", side_effect=make_fake_exchange("b")):
        results, errors = gate.run(worker, parties)

    assert errors == []
    # Each exchanger single-flights its own cold miss → exactly two exchanges
    # total across the two exchangers, regardless of party count.
    assert gate.entered == 2
    # Every caller sees its own exchanger's token.
    for i, token in enumerate(results):
        assert token == ("cdp_a" if i % 2 == 0 else "cdp_b")
