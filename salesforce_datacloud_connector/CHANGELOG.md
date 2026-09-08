# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 2.0.0b2 — Beta release (TBD)

### Changed
- Raised DB-API 2.0 `threadsafety` from `1` to `2`: threads may now share a
  single `Connection` (and its cursor factory), not just the module. This
  reaches parity with the legacy v1 driver. Cursors remain thread-confined and
  must not be shared across threads.

### Fixed
- Connection-shared authentication state is now thread-safe. The core
  authenticator publishes its `instance_url` and pairs `(access_token,
  instance_url)` atomically from a single fetch, and the CDP token exchanger
  caches the token, its expiry, and the tenant endpoint as one immutable
  snapshot published atomically under a lock (double-checked locking). Cold-miss
  fetches/exchanges are single-flighted, so N racing threads trigger one fetch
  rather than a thundering herd, and a reader can never observe a token paired
  with a mismatched endpoint.
- `DataCloudTokenExchanger.invalidate_token()` now clears the tenant endpoint
  along with the token and expiry, so a subsequent `get_tenant_endpoint()`
  re-exchanges instead of returning a stale endpoint.

## 2.0.0b1 — Beta release (TBD)

First public beta of the new `salesforce-datacloud-connector` package, the
successor to `salesforce-cdp-connector`. Distributed as a pre-release on PyPI;
install with `pip install --pre salesforce-datacloud-connector`.

### Added
- DB-API 2.0 compliant driver targeting the Salesforce Data Cloud Query API
  (V3 driver path).
- Three OAuth authentication flows:
  - Username/Password (`UsernamePasswordAuthenticator`)
  - JWT Bearer Token (`JWTAuthenticator`)
  - Refresh Token (`RefreshTokenAuthenticator`)
- Connection-level support for `dataspace` and `workload`.
- Cursor surface: `execute`, `executemany`, `fetchone`, `fetchmany`,
  `fetchall`, iteration, `description`, `rowcount`, `arraysize`, and `cancel`.
- Named-parameter style (`:param`) with safe parameter binding.
- Automatic pagination for large result sets with chunked fetching.
- Type conversion from Data Cloud types to Python types (str, int, Decimal,
  float, bool, datetime.date, datetime.datetime).
- Full DB-API 2.0 exception hierarchy.
- `pandas` integration via `cursor.fetch_df()` and `pandas.read_sql()`,
  installed as the optional `[pandas]` extra
  (`pip install --pre "salesforce-datacloud-connector[pandas]"`).
- Token caching with automatic refresh.
- Long-polling for async query execution and retry logic for transient
  failures.
- Context manager support for connections and cursors.
- Module globals: `apilevel = "2.0"`, `threadsafety = 1`,
  `paramstyle = "named"`.

### Notes
- This is a beta release. Public API may change before GA. Pin your version
  explicitly in production environments.
- Read-only operations (SELECT). No INSERT/UPDATE/DELETE/DDL.
- Synchronous API only; async client is on the V2 roadmap.
- See the README for migration guidance from `salesforce-cdp-connector`.
