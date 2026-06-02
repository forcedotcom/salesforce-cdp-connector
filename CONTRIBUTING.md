# Contributing

Thanks for your interest in contributing. This repository hosts two Python packages for Salesforce Data Cloud:

- [`salesforcecdpconnector/`](salesforcecdpconnector/) — `salesforce-cdp-connector` (v1, deprecated). See the [v1 section of the root README](README.md#salesforce-cdp-connector-deprecated).
- [`salesforce_datacloud_connector/`](salesforce_datacloud_connector/) — `salesforce-datacloud-connector` (v2, beta). See the [v2 README](salesforce_datacloud_connector/README.md).

Each package has its own `pyproject.toml`, `uv.lock`, and CI workflow. Treat them as independent projects that happen to share a repo and a master branch.

## Local development

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then bootstrap the package you want to work on:

```shell
# v1 (run from repo root)
uv sync --all-extras --dev

# v2
cd salesforce_datacloud_connector && uv sync --all-extras --dev
```

## Running tests

Run pytest inside the package directory:

```shell
# v1 (run from repo root)
uv run pytest

# v2
cd salesforce_datacloud_connector && uv run pytest
```

The v2 suite uses `@pytest.mark.e2e` for tests that require a live Salesforce org; CI runs `uv run pytest -m "not e2e"` and skips them by default.

## Running the linter

```shell
# v1 (run from repo root)
uv run ruff check .

# v2
cd salesforce_datacloud_connector && uv run ruff check .
```

## Filing pull requests

- Branch off `master` — it is the trunk for both packages.
- CI is paths-filtered: changing files under `salesforcecdpconnector/` runs `v1-package.yml`, changing files under `salesforce_datacloud_connector/` runs `v2-package.yml`. Touching both runs both.
- Keep a PR scoped to a single package when possible; cross-package refactors are fine but please call them out in the PR description.
- For new features or behavior changes in v2, add or update tests under `salesforce_datacloud_connector/tests/` and a `CHANGELOG.md` entry.

## Questions

Please open a [GitHub Issue](https://github.com/forcedotcom/salesforce-cdp-connector/issues) and tag it `package: v1` or `package: v2` so the right people see it.
