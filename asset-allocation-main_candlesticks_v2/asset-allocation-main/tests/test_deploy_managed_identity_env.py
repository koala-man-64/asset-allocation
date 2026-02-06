from __future__ import annotations

from pathlib import Path

import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _assert_azure_client_id_env_present(doc: dict, *, source: str) -> None:
    template = (doc.get("properties") or {}).get("template") or {}
    containers = template.get("containers") or []
    assert containers, f"{source}: expected at least one container definition"

    for container in containers:
        env_list = container.get("env") or []
        for entry in env_list:
            if entry.get("name") == "AZURE_CLIENT_ID":
                assert entry.get("value") == "${ACR_PULL_IDENTITY_CLIENT_ID}", (
                    f"{source}: AZURE_CLIENT_ID env must be wired to ACR_PULL_IDENTITY_CLIENT_ID"
                )
                return

    raise AssertionError(f"{source}: missing env var AZURE_CLIENT_ID")


def test_all_jobs_wire_user_assigned_identity_client_id() -> None:
    repo_root = _repo_root()
    for path in sorted((repo_root / "deploy").glob("job_*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(doc, dict), f"{path}: expected YAML mapping"
        _assert_azure_client_id_env_present(doc, source=str(path))


def test_api_manifest_wires_user_assigned_identity_client_id() -> None:
    repo_root = _repo_root()
    path = repo_root / "deploy" / "app_api.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), f"{path}: expected YAML mapping"
    _assert_azure_client_id_env_present(doc, source=str(path))


def test_deploy_workflow_exports_acr_pull_identity_client_id() -> None:
    repo_root = _repo_root()
    deploy_workflow = repo_root / ".github" / "workflows" / "deploy.yml"
    text = deploy_workflow.read_text(encoding="utf-8")
    assert "ACR_PULL_IDENTITY_CLIENT_ID" in text, "deploy workflow must export ACR_PULL_IDENTITY_CLIENT_ID"

