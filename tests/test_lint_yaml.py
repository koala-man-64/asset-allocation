import os
from pathlib import Path
import re

import pytest
import yaml

_GITHUB_EXPR_IN_QUOTED_QUERY = re.compile(r'--query\s+\\?"[^"\n]*\$\{\{[^"\n]*"')
_PINNED_CHECKOUT_ACTION = re.compile(r"actions/checkout@[0-9a-f]{40}")
_PINNED_SETUP_PYTHON_ACTION = re.compile(r"actions/setup-python@[0-9a-f]{40}")

_YAML_SCAN_EXCLUDED_DIRS = {
    "__pycache__",
    "node_modules",
    ".pnpm-store",
}


def _iter_yaml_files(repo_root: Path):
    for current_root, dirs, files in os.walk(repo_root):
        current_path = Path(current_root)
        rel_parts = current_path.relative_to(repo_root).parts
        if any(part.startswith(".") and part != ".github" for part in rel_parts):
            dirs[:] = []
            continue

        dirs[:] = [
            directory
            for directory in dirs
            if directory not in _YAML_SCAN_EXCLUDED_DIRS
            and (not directory.startswith(".") or directory == ".github")
        ]

        for file_name in files:
            if not (file_name.endswith(".yml") or file_name.endswith(".yaml")):
                continue
            yaml_file = current_path / file_name
            if any(part.startswith(".") and part != ".github" for part in yaml_file.parts):
                continue
            yield yaml_file

def test_yaml_syntax():
    """Validates that all YAML files in the repository have valid syntax."""
    repo_root = Path(__file__).resolve().parents[1]
    for yaml_file in _iter_yaml_files(repo_root):
        with open(yaml_file, "r", encoding="utf-8") as f:
            try:
                yaml.safe_load(f)
            except yaml.YAMLError as exc:
                pytest.fail(f"YAML syntax error in {yaml_file.relative_to(repo_root)}:\n{exc}")


def _iter_workflow_run_steps(workflow_doc: dict):
    jobs = workflow_doc.get("jobs") or {}
    if not isinstance(jobs, dict):
        return

    for job_name, job_config in jobs.items():
        if not isinstance(job_config, dict):
            continue
        steps = job_config.get("steps") or []
        if not isinstance(steps, list):
            continue

        for index, step in enumerate(steps, start=1):
            if not isinstance(step, dict):
                continue
            script = step.get("run")
            if not isinstance(script, str):
                continue
            step_name = step.get("name") or step.get("id") or f"step-{index}"
            yield job_name, step_name, script


def _workflow_on_block(doc: dict) -> dict:
    # PyYAML treats `on` as a boolean key in YAML 1.1.
    if "on" in doc:
        value = doc.get("on")
    else:
        value = doc.get(True)
    if not isinstance(value, dict):
        raise AssertionError("Workflow document must define an `on` mapping.")
    return value


def test_workflow_run_scripts_do_not_embed_github_expr_in_quoted_query() -> None:
    """Prevents shellcheck/actionlint parse failures for --query strings in run steps."""
    repo_root = Path(__file__).resolve().parents[1]
    workflow_dir = repo_root / ".github" / "workflows"
    violations = []

    for workflow_file in sorted(workflow_dir.glob("*.yml")):
        doc = yaml.safe_load(workflow_file.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            continue

        for job_name, step_name, script in _iter_workflow_run_steps(doc):
            for line_number, line in enumerate(script.splitlines(), start=1):
                if _GITHUB_EXPR_IN_QUOTED_QUERY.search(line):
                    violations.append(
                        f"{workflow_file.relative_to(repo_root)}:{job_name}:{step_name}:{line_number}: {line.strip()}"
                    )

    assert not violations, (
        "GitHub expressions must not be embedded inside quoted --query values in run scripts. "
        "Use shell env vars (for example, $ACR_NAME) inside the quoted string instead.\n"
        + "\n".join(violations)
    )


def test_run_tests_workflow_ui_step_enforces_format_and_lint() -> None:
    """Guards the UI quality gate so formatting/lint regressions fail CI early."""
    repo_root = Path(__file__).resolve().parents[1]
    workflow_file = repo_root / ".github" / "workflows" / "run_tests.yml"
    doc = yaml.safe_load(workflow_file.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), "run_tests.yml must parse to a workflow document object."

    target_script = None
    for job_name, step_name, script in _iter_workflow_run_steps(doc):
        if job_name == "test" and step_name == "Build UI (ui)":
            target_script = script
            break

    assert target_script, "run_tests.yml must define job 'test' step 'Build UI (ui)' with a run script."

    compact_script = " ".join(target_script.split())
    format_check_cmd = "pnpm format:check"
    lint_cmd = "pnpm lint"

    assert format_check_cmd in compact_script, (
        "UI build step must run Prettier format checks before tests/build."
    )
    assert lint_cmd in compact_script, "UI build step must run ESLint checks before tests/build."
    assert compact_script.index(format_check_cmd) < compact_script.index(lint_cmd), (
        "UI build step should run format:check before lint so style failures are surfaced first."
    )


def test_run_tests_workflow_does_not_define_windows_lifespan_regression_job() -> None:
    """Guards the workflow contract after removing the Windows lifespan regression job."""
    repo_root = Path(__file__).resolve().parents[1]
    workflow_file = repo_root / ".github" / "workflows" / "run_tests.yml"
    doc = yaml.safe_load(workflow_file.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), "run_tests.yml must parse to a workflow document object."

    jobs = doc.get("jobs")
    assert isinstance(jobs, dict), "run_tests.yml must define a jobs mapping."
    assert "api-lifespan-windows" not in jobs, (
        "run_tests.yml should not define job 'api-lifespan-windows' after workflow cleanup."
    )


def test_manual_trigger_workflow_is_single_job_only() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    workflow_file = repo_root / ".github" / "workflows" / "trigger_all_jobs.yml"
    doc = yaml.safe_load(workflow_file.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), "trigger_all_jobs.yml must parse to a workflow document object."

    on_block = _workflow_on_block(doc)
    workflow_dispatch = on_block.get("workflow_dispatch")
    assert isinstance(workflow_dispatch, dict), "trigger_all_jobs.yml must define workflow_dispatch."

    inputs = workflow_dispatch.get("inputs")
    assert isinstance(inputs, dict), "workflow_dispatch must define inputs."
    job_input = inputs.get("job")
    assert isinstance(job_input, dict), "workflow_dispatch inputs must include a job selector."

    assert job_input.get("required") is True, "Manual trigger job input must be required."
    assert job_input.get("type") == "choice", "Manual trigger job input must use choice type."

    options = job_input.get("options")
    assert isinstance(options, list) and options, "Manual trigger job input must define selectable options."
    assert "all" not in options, "Manual trigger workflow must not offer an all-jobs option."

    expected = {
        "bronze_market",
        "bronze_finance",
        "bronze_price_target",
        "bronze_earnings",
        "silver_market",
        "silver_finance",
        "silver_price_target",
        "silver_earnings",
        "gold_market",
        "gold_finance",
        "gold_price_target",
        "gold_earnings",
    }
    assert set(options) == expected, "Manual trigger options must enumerate the supported single-job set."

    text = workflow_file.read_text(encoding="utf-8")
    assert "job == 'all'" not in text, "Manual trigger workflow must not contain all-job conditional logic."
    assert "default: all" not in text, "Manual trigger workflow must not default to all."


def test_deploy_workflow_bronze_bootstrap_is_opt_in() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    workflow_file = repo_root / ".github" / "workflows" / "deploy.yml"
    doc = yaml.safe_load(workflow_file.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), "deploy.yml must parse to a workflow document object."

    on_block = _workflow_on_block(doc)
    workflow_dispatch = on_block.get("workflow_dispatch")
    assert isinstance(workflow_dispatch, dict), "deploy.yml must define workflow_dispatch."
    inputs = workflow_dispatch.get("inputs")
    assert isinstance(inputs, dict), "workflow_dispatch must define inputs."

    bootstrap_input = inputs.get("bootstrap_bronze_runs")
    assert isinstance(bootstrap_input, dict), "deploy.yml must declare bootstrap_bronze_runs input."
    assert bootstrap_input.get("type") == "boolean"
    assert bootstrap_input.get("required") is False
    assert bootstrap_input.get("default") is False, "bootstrap_bronze_runs must default to false."

    env = doc.get("env")
    assert isinstance(env, dict), "deploy.yml must define env mapping."
    bootstrap_env = env.get("BOOTSTRAP_BRONZE_RUNS")
    assert isinstance(bootstrap_env, str), "deploy.yml must define BOOTSTRAP_BRONZE_RUNS env expression."
    assert "bootstrap_bronze_runs" in bootstrap_env

    text = workflow_file.read_text(encoding="utf-8")
    assert text.count('if [ "${BOOTSTRAP_BRONZE_RUNS}" = "true" ]; then') == 4, (
        "All Bronze deploy blocks must gate immediate starts behind BOOTSTRAP_BRONZE_RUNS."
    )


def test_deploy_workflow_exports_acr_login_server_for_yaml_templates() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    workflow_file = repo_root / ".github" / "workflows" / "deploy.yml"
    text = workflow_file.read_text(encoding="utf-8")
    doc = yaml.safe_load(text)
    assert isinstance(doc, dict), "deploy.yml must parse to a workflow document object."

    assert 'acr_login_server="$(az acr show --name' in text, (
        "deploy.yml must resolve ACR login server from Azure before rendering app YAML templates."
    )
    assert 'echo "ACR_LOGIN_SERVER=$acr_login_server" >> "$GITHUB_ENV"' in text, (
        "deploy.yml must export ACR_LOGIN_SERVER to GITHUB_ENV for envsubst templates."
    )

    deploy_script = None
    for job_name, step_name, script in _iter_workflow_run_steps(doc):
        if job_name == "build-and-deploy" and step_name == "Deploy Unified App (API + UI Sidecar)":
            deploy_script = script
            break

    assert deploy_script, (
        "deploy.yml must define build-and-deploy step 'Deploy Unified App (API + UI Sidecar)'."
    )
    assert ': "${ACR_LOGIN_SERVER:?ACR_LOGIN_SERVER is required}"' in deploy_script, (
        "Unified app deploy step must fail fast when ACR_LOGIN_SERVER is missing."
    )


def test_supply_chain_security_workflow_enforces_pinned_audits() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    workflow_file = repo_root / ".github" / "workflows" / "supply_chain_security.yml"
    assert workflow_file.exists(), "Supply-chain security workflow must exist."

    text = workflow_file.read_text(encoding="utf-8")
    doc = yaml.safe_load(text)
    assert isinstance(doc, dict), "supply_chain_security.yml must parse to a workflow document object."

    assert doc.get("name") == "Supply Chain Security"

    on_block = _workflow_on_block(doc)
    assert "pull_request" in on_block, "Supply-chain workflow must run on pull_request."
    push_block = on_block.get("push")
    assert isinstance(push_block, dict), "Supply-chain workflow must define push trigger."
    branches = push_block.get("branches")
    assert isinstance(branches, list) and "main" in branches, (
        "Supply-chain workflow push trigger must include main branch."
    )
    assert "workflow_dispatch" in on_block, "Supply-chain workflow must support manual dispatch."

    permissions = doc.get("permissions")
    assert permissions == {"contents": "read"}, (
        "Supply-chain workflow permissions must be least privilege (contents: read)."
    )

    assert _PINNED_CHECKOUT_ACTION.search(text), (
        "Supply-chain workflow must pin actions/checkout to a commit SHA."
    )
    assert _PINNED_SETUP_PYTHON_ACTION.search(text), (
        "Supply-chain workflow must pin actions/setup-python to a commit SHA."
    )
    assert "node:20-bookworm-slim@sha256:" in text, (
        "Supply-chain workflow must pin the Node container image by digest."
    )
    assert "pip-audit --strict -r requirements.lock.txt" in text
    assert "pip-audit --strict -r requirements-dev.lock.txt" in text
    assert "pnpm audit --audit-level=high" in text


def test_dependabot_config_covers_actions_python_and_ui() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    dependabot_file = repo_root / ".github" / "dependabot.yml"
    assert dependabot_file.exists(), ".github/dependabot.yml must exist."

    doc = yaml.safe_load(dependabot_file.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), "dependabot.yml must parse to a mapping."
    assert doc.get("version") == 2, "dependabot.yml must use schema version 2."

    updates = doc.get("updates")
    assert isinstance(updates, list) and updates, "dependabot.yml must define update entries."

    by_ecosystem = {
        str(item.get("package-ecosystem")): item
        for item in updates
        if isinstance(item, dict) and item.get("package-ecosystem")
    }
    assert {"github-actions", "pip", "npm"}.issubset(set(by_ecosystem.keys())), (
        "dependabot.yml must cover GitHub Actions, Python, and UI npm dependencies."
    )

    assert by_ecosystem["github-actions"].get("directory") == "/"
    assert by_ecosystem["pip"].get("directory") == "/"
    assert by_ecosystem["npm"].get("directory") == "/ui"

    groups = doc.get("multi-ecosystem-groups")
    assert isinstance(groups, dict), "dependabot.yml must define multi-ecosystem-groups."
    all_dependencies = groups.get("all-dependencies")
    assert isinstance(all_dependencies, dict), (
        "dependabot.yml must define an all-dependencies multi-ecosystem group."
    )

    group_schedule = all_dependencies.get("schedule")
    assert isinstance(group_schedule, dict), "all-dependencies group must include a schedule."
    assert group_schedule.get("interval") == "weekly", "all-dependencies updates must run weekly."

    group_commit_message = all_dependencies.get("commit-message")
    assert isinstance(group_commit_message, dict), (
        "all-dependencies group must define a commit-message."
    )
    assert group_commit_message.get("prefix") == "deps(all)"

    for ecosystem in ("github-actions", "pip", "npm"):
        entry = by_ecosystem[ecosystem]
        assert entry.get("multi-ecosystem-group") == "all-dependencies", (
            f"{ecosystem} entry must be assigned to the all-dependencies group."
        )
        patterns = entry.get("patterns")
        assert isinstance(patterns, list) and "*" in patterns, (
            f"{ecosystem} entry must include wildcard patterns for grouped updates."
        )


def test_repo_level_agents_governance_file_exists() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    agents_file = repo_root / "AGENTS.md"
    assert agents_file.exists(), "Repo root must contain AGENTS.md governance instructions."

    text = agents_file.read_text(encoding="utf-8")
    assert "Available skills" in text, "AGENTS.md must enumerate available skills."
    assert "How to use skills" in text, "AGENTS.md must define usage instructions."
    assert "Trigger rules" in text, "AGENTS.md must define skill trigger rules."
