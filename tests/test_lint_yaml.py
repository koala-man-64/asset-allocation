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
