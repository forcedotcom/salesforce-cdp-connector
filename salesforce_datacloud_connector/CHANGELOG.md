# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0](https://github.com/forcedotcom/salesforce-cdp-connector/compare/salesforce-datacloud-connector-2.0.0...salesforce-datacloud-connector-2.0.0) (2026-10-16)


### Features

* **auth:** add Salesforce CLI authenticator for local/dev ([#63](https://github.com/forcedotcom/salesforce-cdp-connector/issues/63)) ([4706a87](https://github.com/forcedotcom/salesforce-cdp-connector/commit/4706a878ee53e6db3d1c4ca0e5601343a7a0d32f))
* **auth:** surface OAuth errors, retry token calls, refresh on 401, add cdp_token auth, harden JWT ([#70](https://github.com/forcedotcom/salesforce-cdp-connector/issues/70)) ([3f526c7](https://github.com/forcedotcom/salesforce-cdp-connector/commit/3f526c711206abb607eef927ecbda8dd71dce12b))
* **client:** send User-Agent header with optional caller identifier ([#67](https://github.com/forcedotcom/salesforce-cdp-connector/issues/67)) ([0dadc3b](https://github.com/forcedotcom/salesforce-cdp-connector/commit/0dadc3bc77261f402f311afed0469afbc0f90cc2))
* **threadsafety:** allow connections to be shared across threads ([#64](https://github.com/forcedotcom/salesforce-cdp-connector/issues/64)) ([62e2782](https://github.com/forcedotcom/salesforce-cdp-connector/commit/62e2782c7c2c0ee4c08278a62266151a2026bca9))

## [Unreleased]

### Added
- Query settings support: `connect(query_settings=...)` sets connection-wide
  default query settings (e.g. `{"time_zone": "UTC"}`), and
  `cursor.execute(operation, parameters, settings=...)` supplies or overrides
  settings for a single query. Per-call settings take precedence over the
  connection-level defaults on key collision. Settings map to the server's
  Hyper connection/session settings — see
  https://tableau.github.io/hyper-db/docs/hyper-api/connection#connection-settings.

### Changed
- Raised DB-API `threadsafety` from `1` to `2`: threads can share a Connection
  when each thread uses its own cursor. CDP token cache access is synchronized
  so concurrent cache misses perform one exchange.
- `cursor.rowcount` now reports the total number of rows in the result set
  after `execute()`, as read from the server's query status, instead of always
  returning `-1`. It remains `-1` before any query is executed and is reset to
  `-1` at the start of each `execute()` so a failed execution does not report a
  stale count (DB-API 2.0 / PEP 249).
- Query results now default to the Arrow IPC wire format instead of JSON.
  Pass `output_format="json"` to `connect()` to opt back into the previous
  JSON behavior.
- `pyarrow` moved out of the `[pandas]` optional dependency extra into the
  dev dependency group — it is only needed to build Arrow IPC test fixtures,
  not by `pandas` support at runtime (`nanoarrow` is the runtime Arrow
  dependency).
- `nanoarrow` is now upper-bounded (`>=0.6.0,<0.10.0`) instead of unbounded,
  so a future major `nanoarrow` release can't silently change how Arrow
  values map to Python types (e.g. timestamps) underneath
  `convert_datacloud_value` without a deliberate version bump here.

## 2.0.0b1 — Beta release (TBD)

First public beta of the new `salesforce-datacloud-connector` package, the
successor to `salesforce-cdp-connector`. Distributed as a pre-release on PyPI;
install with `pip install --pre salesforce-datacloud-connector`.

### Added
- DB-API 2.0 compliant driver targeting the Salesforce Data Cloud Query API
  (V3 driver path).
- Four OAuth authentication flows:
  - Username/Password (`UsernamePasswordAuthenticator`)
  - JWT Bearer Token (`JWTAuthenticator`)
  - Refresh Token (`RefreshTokenAuthenticator`)
  - Client Credentials (`ClientCredentialsAuthenticator`)
- Salesforce CLI authenticator (`SfCliAuthenticator`, `auth_type="sf_cli"`) for
  local/dev — reuses the org the `sf` CLI is already authenticated against
  instead of requiring a connected app.
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
