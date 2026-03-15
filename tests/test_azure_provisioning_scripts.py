from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_interactive_azure_orchestrator_wraps_existing_scripts() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "provision_azure_interactive.ps1"
    text = script.read_text(encoding="utf-8")

    assert "validate_azure_permissions.ps1" in text, (
        "interactive orchestrator must expose the existing Azure permission validation step"
    )
    assert "provision_azure.ps1" in text, (
        "interactive orchestrator must route shared infra through provision_azure.ps1"
    )
    assert "-ProvisionPostgres:$false" in text, (
        "interactive orchestrator must disable embedded Postgres when delegating shared infra"
    )
    assert "provision_azure_postgres.ps1" in text, (
        "interactive orchestrator must route Postgres through the dedicated Postgres provisioner"
    )
    assert "configure_cost_guardrails.ps1" in text, (
        "interactive orchestrator must expose the cost guardrails deployment step"
    )
    assert "validate_acr_pull.ps1" in text, (
        "interactive orchestrator must expose the post-provision ACR validation step"
    )


def test_interactive_azure_orchestrator_uses_child_powershell_processes() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "provision_azure_interactive.ps1"
    text = script.read_text(encoding="utf-8")

    assert "Resolve-PowerShellExe" in text, (
        "interactive orchestrator must resolve a child PowerShell executable"
    )
    assert "-ExecutionPolicy Bypass -File $ScriptPath @Arguments" in text, (
        "interactive orchestrator must launch child scripts via a separate PowerShell process"
    )
    assert "Continue to the next step?" in text, (
        "interactive orchestrator must allow the operator to continue after a failed child step"
    )
