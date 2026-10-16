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

## Running the linter and formatter

```shell
# v1 (run from repo root)
uv run ruff check .

# v2
cd salesforce_datacloud_connector && uv run ruff check .
cd salesforce_datacloud_connector && uv run ruff format .
```

CI enforces both `ruff check` and `ruff format --check` for v2.

## Pre-commit hook (v2 only)

v2 (`salesforce_datacloud_connector/`) has a local git hook that runs `ruff check --fix` and `ruff format` on
staged files before every commit, so lint/format issues are caught (and usually auto-fixed) before they ever
reach CI. It's scoped to `salesforce_datacloud_connector/` only — v1 commits are unaffected.

One-time setup per clone:

```shell
cd salesforce_datacloud_connector && uv sync --all-extras --dev
uv run --project salesforce_datacloud_connector pre-commit install
```

If a commit is rejected because files were auto-fixed, just `git add` the fixed files and commit again.

## Filing pull requests

- Branch off `master` — it is the trunk for both packages.
- CI is paths-filtered: changing files under `salesforcecdpconnector/` runs `v1-package.yml`, changing files under `salesforce_datacloud_connector/` runs `v2-package.yml`. Touching both runs both.
- Keep a PR scoped to a single package when possible; cross-package refactors are fine but please call them out in the PR description.
- For new features or behavior changes in v2, add or update tests under `salesforce_datacloud_connector/tests/` and use a conventional-commit PR title (the changelog is generated; see [Releasing v2](#releasing-v2)).

## Releasing v2

v2 releases are automated with [release-please](https://github.com/googleapis/release-please) (`.github/workflows/v2-release-please.yml`). v1 is not released this way.

- Use [Conventional Commits](https://www.conventionalcommits.org/) for PR titles (enforced by `pr-title.yml`; PRs are squash-merged, so the title becomes the commit message). `feat:` bumps minor, `fix:` bumps patch, `feat!:` / `BREAKING CHANGE:` bumps major. Only commits touching `salesforce_datacloud_connector/` count.
- On every push to `master`, release-please opens or updates a "release" PR that bumps `pyproject.toml` and `__version__` and updates `salesforce_datacloud_connector/CHANGELOG.md`. Don't edit the changelog by hand outside of a release PR.
- The config currently pins `"release-as": "2.0.0"` for the first GA release. Remove that line from `release-please-config.json` once 2.0.0 is out, or every later release PR will keep proposing 2.0.0.
- The release PR is created with `GITHUB_TOKEN` (like `datacloud-jdbc`), so its commits are signed as required by the repo ruleset. The trade-off is that GitHub does not trigger workflows for it. To get the required `test` checks to run, close and reopen the release PR.
- Merging the release PR creates the tag `salesforce-datacloud-connector-<version>` and a GitHub release. That does not trigger publishing, so run the **v2 Publish to PyPI** workflow manually (Actions → Run workflow) with `tag_name` set to the new tag.
- release-please only lists conventional commits. Before merging the release PR, add any hand-written notes from earlier non-conventional changes to its changelog entry.

## Questions

Please open a [GitHub Issue](https://github.com/forcedotcom/salesforce-cdp-connector/issues) and tag it `package: v1` or `package: v2` so the right people see it.
