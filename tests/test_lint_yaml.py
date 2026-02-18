from pathlib import Path
import re

import pytest
import yaml

_GITHUB_EXPR_IN_QUOTED_QUERY = re.compile(r'--query\s+\\?"[^"\n]*\$\{\{[^"\n]*"')

def test_yaml_syntax():
    """Validates that all YAML files in the repository have valid syntax."""
    repo_root = Path(__file__).resolve().parents[1]
    yaml_files = list(repo_root.rglob("*.yml")) + list(repo_root.rglob("*.yaml"))
    
    for yaml_file in yaml_files:
        # Skip hidden directories like .git, .mypy_cache, etc.
        if any(part.startswith(".") and part != ".github" for part in yaml_file.parts):
            continue
            
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


def test_run_tests_workflow_has_windows_lifespan_regression_job() -> None:
    """Guards the cross-platform API lifespan gate for cancellation regressions."""
    repo_root = Path(__file__).resolve().parents[1]
    workflow_file = repo_root / ".github" / "workflows" / "run_tests.yml"
    doc = yaml.safe_load(workflow_file.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), "run_tests.yml must parse to a workflow document object."

    jobs = doc.get("jobs")
    assert isinstance(jobs, dict), "run_tests.yml must define a jobs mapping."
    job = jobs.get("api-lifespan-windows")
    assert isinstance(job, dict), "run_tests.yml must define job 'api-lifespan-windows'."

    assert str(job.get("runs-on", "")).strip().lower() == "windows-latest", (
        "api-lifespan-windows must run on windows-latest."
    )

    setup_step = None
    for step in job.get("steps") or []:
        if isinstance(step, dict) and str(step.get("name", "")).strip() == "Set up Python":
            setup_step = step
            break

    assert isinstance(setup_step, dict), "api-lifespan-windows must include a 'Set up Python' step."
    setup_with = setup_step.get("with")
    assert isinstance(setup_with, dict), "'Set up Python' step must define a 'with' mapping."
    assert str(setup_with.get("python-version", "")).strip().strip("'").strip('"') == "3.13", (
        "api-lifespan-windows must pin Python 3.13."
    )

    target_script = None
    for job_name, step_name, script in _iter_workflow_run_steps(doc):
        if job_name == "api-lifespan-windows" and step_name == "Run Windows lifespan regression tests":
            target_script = script
            break

    assert target_script, (
        "api-lifespan-windows must include run step 'Run Windows lifespan regression tests'."
    )
    compact_script = " ".join(target_script.split())
    assert "tests/api/test_lifespan_workers.py" in compact_script, (
        "Windows regression run must include tests/api/test_lifespan_workers.py."
    )
