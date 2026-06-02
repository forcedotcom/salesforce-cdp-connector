# Salesforce Data Cloud Python Connector

The official Salesforce Data Cloud Python connector — a DB-API 2.0 compliant
driver for querying Salesforce Data Cloud using the Query API. Designed for
use from Jupyter notebooks, pandas pipelines, ETL scripts, and any other
Python data tooling.

This package (`salesforce-datacloud-connector`) supersedes the
`salesforce-cdp-connector` package. New projects should adopt this package;
existing `salesforce-cdp-connector` users should plan to migrate.

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-green.svg)](../LICENSE.txt)

## Important: this is a beta release

`salesforce-datacloud-connector` is currently published as a **beta** on PyPI
(`2.0.0b1`). The public API surface may change before the GA release.

- Install with the `--pre` flag — pip will not pick up pre-releases otherwise:

  ```bash
  pip install --pre salesforce-datacloud-connector
  ```

- **Pin your version explicitly** in production code and your dependency
  manifests:

  ```text
  salesforce-datacloud-connector==2.0.0b1
  ```

  This protects you from accidental upgrades to a later beta or release
  candidate that may include breaking changes.

## Features

- **DB-API 2.0 compliant** — standard Python database interface.
- **Three OAuth flows** — Username/Password, JWT Bearer Token, Refresh Token.
- **Pandas integration** — works with `pandas.read_sql()` and
  `cursor.fetch_df()` (under the `[pandas]` extra).
- **Notebook-ready** — interactive exploration and visualization in Jupyter.
- **Parameterized queries** — safe SQL execution with named parameters
  (`:param`).
- **Efficient pagination** — chunked, low-memory fetching for large result
  sets.
- **Type conversion** — Data Cloud types map to native Python types (`str`,
  `int`, `Decimal`, `float`, `bool`, `datetime.date`, `datetime.datetime`).
- **Comprehensive error hierarchy** — full DB-API 2.0 exception tree.

## Installation

```bash
# Basic installation (always include --pre during the beta)
pip install --pre salesforce-datacloud-connector

# With pandas + pyarrow support (recommended for analytics use)
pip install --pre "salesforce-datacloud-connector[pandas]"
```

### Requirements

- Python 3.8 or newer.
- `requests >= 2.31.0`
- `cryptography >= 41.0.0`
- `pyjwt >= 2.8.0`
- `python-dateutil >= 2.8.0`

Optional (under `[pandas]` extra):

- `pandas >= 2.0.0`
- `pyarrow >= 14.0.0`

## Quickstart

```python
import salesforce_datacloud_connector as sfdc

# Connect to Salesforce Data Cloud (refresh token flow shown — see below for
# all three flows)
conn = sfdc.connect(
    login_url="https://login.salesforce.com",
    auth_type="refresh_token",
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    refresh_token="YOUR_REFRESH_TOKEN",
)

cursor = conn.cursor()
cursor.execute("SELECT Id, Name FROM Account LIMIT 10")

for row in cursor:
    print(row)

cursor.close()
conn.close()
```

The connector also supports the standard Python context-manager idioms — both
the connection and the cursor close cleanly when their `with` blocks exit:

```python
import salesforce_datacloud_connector as sfdc

with sfdc.connect(...) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM Account")
        (count,) = cursor.fetchone()
        print(f"Total accounts: {count}")
```

## Authentication

The connector ships three OAuth flows, all driven through `sfdc.connect(...)`
via the `auth_type` keyword.

### Username / Password

```python
import salesforce_datacloud_connector as sfdc

conn = sfdc.connect(
    login_url="https://login.salesforce.com",
    auth_type="username_password",
    username="user@example.com",
    password="your_password",
    client_id="your_connected_app_client_id",
    client_secret="your_connected_app_client_secret",
)
```

### JWT Bearer Token

```python
import salesforce_datacloud_connector as sfdc

with open("private_key.pem", "r") as f:
    private_key = f.read()

conn = sfdc.connect(
    login_url="https://login.salesforce.com",
    auth_type="jwt",
    username="user@example.com",
    client_id="your_connected_app_client_id",
    jwt_private_key=private_key,
)
```

### Refresh Token (recommended for long-running services)

```python
import os

import salesforce_datacloud_connector as sfdc

conn = sfdc.connect(
    login_url=os.environ["SFDC_LOGIN_URL"],
    auth_type="refresh_token",
    client_id=os.environ["SFDC_CLIENT_ID"],
    client_secret=os.environ["SFDC_CLIENT_SECRET"],
    refresh_token=os.environ["SFDC_REFRESH_TOKEN"],
)
```

### Credential hygiene

Never hardcode credentials in source. Load them from environment variables
(local development) or your platform's secret manager (production). Rotate
credentials regularly and avoid using production credentials for development.

## Usage

### Parameterized queries

```python
cursor.execute(
    """
    SELECT Id, Name, Email
    FROM Contact
    WHERE Status = :status AND CreatedDate > :min_date
    LIMIT :limit
    """,
    {
        "status": "Active",
        "min_date": "2024-01-01",
        "limit": 100,
    },
)
```

### Pandas integration

```python
import pandas as pd
import salesforce_datacloud_connector as sfdc

conn = sfdc.connect(...)

# Method 1 — pandas.read_sql
df = pd.read_sql("SELECT * FROM Account LIMIT 1000", conn)

# Method 2 — cursor.fetch_df
cursor = conn.cursor()
cursor.execute("SELECT Id, Name, Industry FROM Account")
df = cursor.fetch_df()

print(df.describe())
print(df.groupby("Industry").size())
```

`cursor.fetch_df()` lazily imports `pandas`. Calling it without the `[pandas]`
extra installed raises `ImportError` with installation guidance.

### Fetch methods

```python
row = cursor.fetchone()       # one row
rows = cursor.fetchmany(100)  # multiple rows
all_rows = cursor.fetchall()  # all remaining rows

for row in cursor:            # iteration
    process(row)
```

### Configuration: dataspace and workload

```python
conn = sfdc.connect(
    ...,
    dataspace="custom_dataspace",  # optional; defaults to the org default
    workload="my_application",     # optional; surfaced in observability
)
```

### Cursor array size

```python
cursor = conn.cursor()
cursor.arraysize = 1000  # default fetch size for fetchmany()
```

### Type mapping

| Data Cloud type | Python type | Notes |
|---|---|---|
| Varchar | `str` | |
| Numeric | `int`, `Decimal` | `int` when scale is 0; `Decimal` otherwise |
| Integer, BigInt | `int` | |
| Float, Double | `float` | |
| Boolean | `bool` | |
| Date | `datetime.date` | |
| TimestampTZ | `datetime.datetime` | timezone-aware |

## API reference

### DB-API 2.0 surface

This connector follows
[PEP 249 — Python Database API Specification v2.0](https://peps.python.org/pep-0249/).

- **API level:** `2.0`
- **Thread safety:** `1` (threads may share the module, but not connections)
- **Parameter style:** `named` (`:param`)

#### Connection

- `cursor()` — create a new cursor.
- `close()` — close the connection.
- `commit()` — no-op (read-only driver).
- `rollback()` — no-op (read-only driver).

#### Cursor

- `execute(operation, parameters=None)` — execute a query.
- `executemany(operation, seq_of_parameters)` — execute the same query
  multiple times.
- `fetchone()` — fetch the next row.
- `fetchmany(size=cursor.arraysize)` — fetch up to `size` rows.
- `fetchall()` — fetch all remaining rows.
- `close()` — close the cursor.
- `cancel()` — cancel a running query (extension).
- `fetch_df()` — fetch results as a pandas DataFrame (extension; requires the
  `[pandas]` extra).

#### Cursor attributes

- `description` — column metadata.
- `rowcount` — number of rows affected (`-1` for SELECT until exhausted).
- `arraysize` — default fetch size for `fetchmany()`.

### Exception hierarchy

```
Exception
└── Warning
└── Error
    ├── InterfaceError
    └── DatabaseError
        ├── DataError
        ├── OperationalError      (auth failures, network errors)
        ├── IntegrityError
        ├── InternalError         (server errors)
        ├── ProgrammingError      (SQL syntax errors)
        └── NotSupportedError     (unsupported operations)
```

```python
import salesforce_datacloud_connector as sfdc

try:
    cursor.execute("SELECT * FROM NonexistentTable")
except sfdc.ProgrammingError as e:
    print(f"SQL error: {e}")

try:
    cursor.execute("INSERT INTO Account VALUES (...)")
except sfdc.NotSupportedError as e:
    print(f"Operation not supported: {e}")
```

### `fetch_df()` extension

`cursor.fetch_df()` returns a `pandas.DataFrame` containing all remaining rows
of the most recent query. It is available only when the `[pandas]` extra is
installed:

```bash
pip install --pre "salesforce-datacloud-connector[pandas]"
```

```python
cursor.execute("SELECT Id, Name, Industry FROM Account LIMIT 5000")
df = cursor.fetch_df()
```

If pandas is missing, `fetch_df()` raises an `ImportError` with the install
command above.

## Beta-period limitations

`2.0.0b1` is intentionally scoped:

1. **Read-only** — no `INSERT`, `UPDATE`, `DELETE`, or DDL operations.
2. **No streaming** — results buffer in memory (chunked pagination keeps
   memory bounded but does not stream row-by-row).
3. **Synchronous API only** — async client is on the V2 roadmap.
4. **No connection pooling** — create new connections as needed.
5. **No prepared statements** — parameters bind on each execution.
6. **No SQLAlchemy dialect** — direct DB-API usage only.
7. **In-memory token cache** — tokens are not persisted across runs.

These constraints will be reconsidered as the package moves from beta to GA.

## Development

The repository uses [`uv`](https://docs.astral.sh/uv/) for dependency
management and [`hatchling`](https://hatch.pypa.io/) as the build backend.

### Setup

```bash
# From the repo root
cd salesforce_datacloud_connector

# Install everything (runtime + pandas extra + dev dependency group)
uv sync --all-extras --dev
```

### Run tests

```bash
# Unit + integration tests (skips live-org E2E)
uv run pytest -m "not e2e"

# Include the live-org E2E suite (requires real Salesforce credentials —
# see tests/test_e2e_real_datacloud.py for the env vars it expects)
uv run pytest
```

### Lint

```bash
uv run ruff check salesforce_datacloud_connector tests --statistics
```

### Build

```bash
uv build
# produces dist/salesforce_datacloud_connector-<version>-py3-none-any.whl
# and  dist/salesforce_datacloud_connector-<version>.tar.gz
```

### Repo layout

The repository publishes two PyPI packages from one branch — see the
[repo-root README](../README.md) for the full layout. This package's source
lives under `salesforce_datacloud_connector/salesforce_datacloud_connector/`,
its tests under `salesforce_datacloud_connector/tests/`, and its build config
under `salesforce_datacloud_connector/pyproject.toml`.

### Contributing

Contributions are welcome. Please open an issue before starting on a large
change. PRs should include tests for new functionality and pass `ruff check`
and `pytest -m "not e2e"` locally.

See [`CONTRIBUTING.md`](../CONTRIBUTING.md) at the repo root for the full
contribution guide.

## License

BSD-3-Clause. See [`LICENSE.txt`](../LICENSE.txt) at the repo root.
