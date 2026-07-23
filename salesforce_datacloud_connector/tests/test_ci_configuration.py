"""Tests for CI configuration files (Dependabot, GitHub Actions workflows)."""

from pathlib import Path

import pytest

try:
    import yaml
except ImportError:
    pytest.skip("PyYAML not installed (not a required dependency)", allow_module_level=True)


# This test file lives at salesforce_datacloud_connector/tests/test_ci_configuration.py.
# .github/ lives at the TRUE repo root, three parents up (tests -> package-subdir -> repo root).
REPO_ROOT = Path(__file__).parent.parent.parent


class TestDependabotConfiguration:
    """Tests for .github/dependabot.yml."""

    def test_dependabot_file_exists(self):
        """Verify Dependabot configuration file exists."""
        dependabot_path = REPO_ROOT / ".github" / "dependabot.yml"
        assert dependabot_path.exists(), "Dependabot configuration file not found"

    def test_dependabot_yaml_valid(self):
        """Verify Dependabot YAML is valid and parses without errors."""
        dependabot_path = REPO_ROOT / ".github" / "dependabot.yml"
        with open(dependabot_path, "r") as f:
            config = yaml.safe_load(f)
        assert config is not None, "Dependabot config is empty"
        assert "version" in config, "Missing 'version' key"
        assert config["version"] == 2, "Dependabot version must be 2"

    def test_dependabot_has_v1_and_v2_pip_updates(self):
        """Verify Dependabot configures separate updates for v1 (root) and v2 (subdir)."""
        dependabot_path = REPO_ROOT / ".github" / "dependabot.yml"
        with open(dependabot_path, "r") as f:
            config = yaml.safe_load(f)

        assert "updates" in config, "Missing 'updates' key"
        pip_updates = [u for u in config["updates"] if u["package-ecosystem"] == "pip"]
        assert len(pip_updates) >= 2, "Expected at least 2 pip update configurations (v1 + v2)"

        directories = {u["directory"] for u in pip_updates}
        assert "/" in directories, "Missing v1 pip updates (root directory)"
        assert "/salesforce_datacloud_connector" in directories, "Missing v2 pip updates (subdir)"

    def test_dependabot_v1_ignores_major_updates(self):
        """Verify v1 (deprecated) ignores major version updates."""
        dependabot_path = REPO_ROOT / ".github" / "dependabot.yml"
        with open(dependabot_path, "r") as f:
            config = yaml.safe_load(f)

        v1_update = next(u for u in config["updates"] if u["directory"] == "/")
        assert "ignore" in v1_update, "V1 should have 'ignore' rules"

        ignores = v1_update["ignore"]
        major_ignore = next(
            (i for i in ignores if "version-update:semver-major" in i.get("update-types", [])),
            None,
        )
        assert major_ignore is not None, "V1 must ignore major version updates (deprecated)"

    def test_dependabot_v2_allows_major_updates(self):
        """Verify v2 (active) allows major version updates."""
        dependabot_path = REPO_ROOT / ".github" / "dependabot.yml"
        with open(dependabot_path, "r") as f:
            config = yaml.safe_load(f)

        v2_update = next(u for u in config["updates"] if u["directory"] == "/salesforce_datacloud_connector")
        if "ignore" in v2_update:
            ignores = v2_update["ignore"]
            major_ignore = next(
                (i for i in ignores if "version-update:semver-major" in i.get("update-types", [])),
                None,
            )
            assert major_ignore is None, "V2 should NOT ignore major version updates"

    def test_dependabot_has_github_actions_updates(self):
        """Verify Dependabot configures GitHub Actions dependency updates."""
        dependabot_path = REPO_ROOT / ".github" / "dependabot.yml"
        with open(dependabot_path, "r") as f:
            config = yaml.safe_load(f)

        actions_updates = [u for u in config["updates"] if u["package-ecosystem"] == "github-actions"]
        assert len(actions_updates) >= 1, "Expected at least 1 github-actions update configuration"
        assert actions_updates[0]["directory"] == "/", "GitHub Actions updates should target root"

    def test_dependabot_weekly_schedule(self):
        """Verify all updates run on a weekly schedule."""
        dependabot_path = REPO_ROOT / ".github" / "dependabot.yml"
        with open(dependabot_path, "r") as f:
            config = yaml.safe_load(f)

        for update in config["updates"]:
            assert "schedule" in update, f"Missing schedule for {update['package-ecosystem']}"
            assert update["schedule"]["interval"] == "weekly", f"Expected weekly interval for {update['directory']}"

    def test_dependabot_commit_message_prefixes(self):
        """Verify distinct commit message prefixes for v1, v2, and actions."""
        dependabot_path = REPO_ROOT / ".github" / "dependabot.yml"
        with open(dependabot_path, "r") as f:
            config = yaml.safe_load(f)

        prefixes = {}
        for update in config["updates"]:
            directory = update["directory"]
            ecosystem = update["package-ecosystem"]
            prefix = update.get("commit-message", {}).get("prefix", "")
            prefixes[f"{ecosystem}:{directory}"] = prefix

        assert prefixes.get("pip:/", "") == "deps(v1):", "V1 prefix must be 'deps(v1):'"
        assert prefixes.get("pip:/salesforce_datacloud_connector", "") == "deps(v2):", "V2 prefix must be 'deps(v2):'"
        assert prefixes.get("github-actions:/", "") == "deps(actions):", "Actions prefix must be 'deps(actions):'"


class TestV2WorkflowConfiguration:
    """Tests for .github/workflows/v2-package.yml."""

    def test_v2_workflow_file_exists(self):
        """Verify v2 workflow file exists."""
        workflow_path = REPO_ROOT / ".github" / "workflows" / "v2-package.yml"
        assert workflow_path.exists(), "V2 workflow file not found"

    def test_v2_workflow_yaml_valid(self):
        """Verify v2 workflow YAML is valid and parses without errors."""
        workflow_path = REPO_ROOT / ".github" / "workflows" / "v2-package.yml"
        with open(workflow_path, "r") as f:
            config = yaml.safe_load(f)
        assert config is not None, "V2 workflow config is empty"
        assert "jobs" in config, "Missing 'jobs' key"

    def test_v2_workflow_runs_ruff(self):
        """Verify v2 workflow runs ruff linting for both package and tests."""
        workflow_path = REPO_ROOT / ".github" / "workflows" / "v2-package.yml"
        with open(workflow_path, "r") as f:
            content = f.read()

        assert "ruff check salesforce_datacloud_connector" in content, "Missing ruff check for package"
        assert "ruff check tests" in content, "Missing ruff check for tests"

    def test_v2_workflow_runs_pytest_with_marker_filter(self):
        """Verify v2 workflow runs pytest with 'not e2e' marker filter."""
        workflow_path = REPO_ROOT / ".github" / "workflows" / "v2-package.yml"
        with open(workflow_path, "r") as f:
            content = f.read()

        assert 'pytest -m "not e2e"' in content, "Missing pytest marker filter (must skip e2e tests)"

    def test_v2_workflow_python_matrix(self):
        """Verify v2 workflow tests Python 3.8, 3.9, 3.10, 3.11."""
        workflow_path = REPO_ROOT / ".github" / "workflows" / "v2-package.yml"
        with open(workflow_path, "r") as f:
            config = yaml.safe_load(f)

        matrix = config["jobs"]["test"]["strategy"]["matrix"]
        assert "python-version" in matrix, "Missing python-version matrix"
        versions = matrix["python-version"]
        assert set(versions) == {"3.8", "3.9", "3.10", "3.11"}, f"Unexpected Python versions: {versions}"

    def test_v2_workflow_working_directory(self):
        """Verify v2 workflow sets working directory to salesforce_datacloud_connector."""
        workflow_path = REPO_ROOT / ".github" / "workflows" / "v2-package.yml"
        with open(workflow_path, "r") as f:
            config = yaml.safe_load(f)

        defaults = config["jobs"]["test"].get("defaults", {})
        run_config = defaults.get("run", {})
        working_dir = run_config.get("working-directory", "")
        assert working_dir == "salesforce_datacloud_connector", f"Unexpected working directory: {working_dir}"
