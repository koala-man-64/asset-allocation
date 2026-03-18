from __future__ import annotations

import ast
import csv
import re
from pathlib import Path

from core.runtime_config import DEFAULT_ENV_OVERRIDE_KEYS


_ALLOWED_CLASSES = {
    "secret",
    "deploy_var",
    "runtime_config",
    "local_dev",
    "constant",
    "deprecated",
}
_ALLOWED_GITHUB_STORAGE = {"secret", "var", "none"}
_ALLOWED_SOURCES = {
    "secret_store",
    "deploy_config",
    "checked_in_deploy_defaults",
    "runtime_config_or_local_env",
    "local_env",
    "checked_in_constant",
    "platform_runtime",
    "deprecated",
}
_WORKFLOW_VAR_PATTERN = re.compile(r"\bvars\.([A-Z][A-Z0-9_]+)\b")
_WORKFLOW_SECRET_PATTERN = re.compile(r"\bsecrets\.([A-Z][A-Z0-9_]+)\b")
_JS_ENV_PATTERNS = (
    re.compile(r'process\.env\.([A-Z][A-Z0-9_]+)'),
    re.compile(r'import\.meta\.env\.([A-Z][A-Z0-9_]+)'),
)
_VITE_BUILTINS = {"DEV", "PROD", "SSR", "MODE", "BASE_URL"}
_ENV_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]+$")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _contract_rows() -> list[dict[str, str]]:
    path = _repo_root() / "docs" / "ops" / "env-contract.csv"
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _contract_map() -> dict[str, dict[str, str]]:
    rows = _contract_rows()
    return {row["name"]: row for row in rows}


def _contract_names_by_class(class_name: str) -> set[str]:
    return {
        row["name"]
        for row in _contract_rows()
        if row["class"] == class_name
    }


def _template_keys() -> set[str]:
    path = _repo_root() / ".env.template"
    keys: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        keys.add(line.split("=", 1)[0].strip())
    return keys


def _workflow_refs(pattern: re.Pattern[str]) -> set[str]:
    refs: set[str] = set()
    workflow_dir = _repo_root() / ".github" / "workflows"
    for path in workflow_dir.glob("*.yml"):
        refs.update(pattern.findall(path.read_text(encoding="utf-8")))
    return refs


def _code_env_refs() -> set[str]:
    root = _repo_root()
    refs: set[str] = set()
    targets = [
        root / "api",
        root / "core",
        root / "monitoring",
        root / "tasks",
        root / "ui" / "src",
        root / "ui" / "vite.config.ts",
        root / "ui" / "Dockerfile",
        root / "docker-compose.yml",
    ]
    for target in targets:
        paths = (
            [target]
            if target.is_file()
            else [p for p in target.rglob("*") if p.is_file() and "__pycache__" not in p.parts]
        )
        for path in paths:
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            if path.suffix == ".py":
                refs.update(_python_env_refs(text))
                continue
            for pattern in _JS_ENV_PATTERNS:
                refs.update(pattern.findall(text))
    return refs - _VITE_BUILTINS


def _string_literal(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _is_os_environ(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "environ"
        and isinstance(node.value, ast.Name)
        and node.value.id == "os"
    )


def _direct_python_env_refs(node: ast.AST) -> set[str]:
    refs: set[str] = set()

    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            if (
                isinstance(child.func, ast.Attribute)
                and child.func.attr == "get"
                and _is_os_environ(child.func.value)
                and child.args
            ):
                env_name = _string_literal(child.args[0])
                if env_name and _ENV_NAME_PATTERN.fullmatch(env_name):
                    refs.add(env_name)
            elif (
                isinstance(child.func, ast.Attribute)
                and child.func.attr == "getenv"
                and isinstance(child.func.value, ast.Name)
                and child.func.value.id == "os"
                and child.args
            ):
                env_name = _string_literal(child.args[0])
                if env_name and _ENV_NAME_PATTERN.fullmatch(env_name):
                    refs.add(env_name)
        elif isinstance(child, ast.Subscript) and _is_os_environ(child.value):
            env_name = _string_literal(child.slice)
            if env_name and _ENV_NAME_PATTERN.fullmatch(env_name):
                refs.add(env_name)

    return refs


def _helper_call_env_refs(node: ast.AST, helper_names: set[str]) -> set[str]:
    refs: set[str] = set()
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        helper_name = _call_name(child.func)
        if helper_name not in helper_names or not child.args:
            continue
        env_name = _string_literal(child.args[0])
        if env_name and _ENV_NAME_PATTERN.fullmatch(env_name):
            refs.add(env_name)
    return refs


def _python_helper_names(tree: ast.Module) -> set[str]:
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    helper_names = {
        name
        for name, node in functions.items()
        if _direct_python_env_refs(node)
    }

    changed = True
    while changed:
        changed = False
        for name, node in functions.items():
            if name in helper_names:
                continue
            called = {
                _call_name(child.func)
                for child in ast.walk(node)
                if isinstance(child, ast.Call)
            }
            if any(called_name in helper_names for called_name in called):
                helper_names.add(name)
                changed = True

    return helper_names


def _python_env_refs(text: str) -> set[str]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return set()

    helper_names = _python_helper_names(tree)
    refs = _direct_python_env_refs(tree)
    refs.update(_helper_call_env_refs(tree, helper_names))
    return refs


def test_env_contract_rows_are_unique_and_well_formed() -> None:
    rows = _contract_rows()
    names = [row["name"] for row in rows]
    assert len(names) == len(set(names)), "env-contract.csv must not contain duplicate names"

    for row in rows:
        assert row["class"] in _ALLOWED_CLASSES, f"{row['name']}: unexpected class"
        assert row["github_storage"] in _ALLOWED_GITHUB_STORAGE, f"{row['name']}: unexpected github_storage"
        assert row["source_of_truth"] in _ALLOWED_SOURCES, f"{row['name']}: unexpected source_of_truth"
        assert row["template"] in {"true", "false"}, f"{row['name']}: template must be true|false"


def test_env_contract_exactly_matches_env_template_surface() -> None:
    contract_template_keys = {
        row["name"]
        for row in _contract_rows()
        if row["template"] == "true"
    }
    assert contract_template_keys == _template_keys()


def test_workflow_var_and_secret_refs_follow_contract() -> None:
    contract = _contract_map()
    ignored = {"GITHUB_TOKEN"}

    for name in _workflow_refs(_WORKFLOW_VAR_PATTERN) - ignored:
        assert name in contract, f"Workflow var reference is undocumented: {name}"
        assert contract[name]["github_storage"] == "var", (
            f"Workflow var reference must be classified as github_storage=var: {name}"
        )

    for name in _workflow_refs(_WORKFLOW_SECRET_PATTERN) - ignored:
        assert name in contract, f"Workflow secret reference is undocumented: {name}"
        assert contract[name]["github_storage"] == "secret", (
            f"Workflow secret reference must be classified as github_storage=secret: {name}"
        )


def test_runtime_config_keys_are_not_consumed_from_github_vars() -> None:
    deploy_workflow = (_repo_root() / ".github" / "workflows" / "deploy.yml").read_text(encoding="utf-8")
    sync_script = (_repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8")

    assert "ConfigPatterns" not in sync_script
    for key in sorted(DEFAULT_ENV_OVERRIDE_KEYS):
        assert f"vars.{key}" not in deploy_workflow, f"deploy.yml must not consume runtime_config key via vars: {key}"


def test_runtime_config_allowlist_matches_env_contract() -> None:
    assert DEFAULT_ENV_OVERRIDE_KEYS == _contract_names_by_class("runtime_config")


def test_contract_documents_runtime_code_env_refs() -> None:
    contract_names = set(_contract_map())
    undocumented = sorted(_code_env_refs() - contract_names)
    assert undocumented == [], f"Runtime code references undocumented env vars: {undocumented}"


def test_non_secret_identifiers_are_not_sourced_from_github_secrets() -> None:
    workflow_text = (_repo_root() / ".github" / "workflows" / "deploy.yml").read_text(encoding="utf-8")
    for name in [
        "AZURE_CLIENT_ID",
        "AZURE_TENANT_ID",
        "AZURE_SUBSCRIPTION_ID",
        "AZURE_STORAGE_ACCOUNT_NAME",
        "BACKFILL_START_DATE",
    ]:
        assert f"secrets.{name}" not in workflow_text, f"{name} must not be sourced from GitHub Secrets"


def test_sync_script_uses_checked_in_env_contract() -> None:
    sync_script = (_repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8")
    assert "env-contract.csv" in sync_script
    assert "Load-EnvContract" in sync_script
    assert "ConfigPatterns" not in sync_script
