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


def _assert_job_manifest_uses_managed_identity_for_acr_pull(doc: dict, *, source: str) -> None:
    identity = doc.get("identity") or {}
    assert identity.get("type") == "UserAssigned", (
        f"{source}: expected top-level UserAssigned identity"
    )
    user_assigned = identity.get("userAssignedIdentities") or {}
    assert "${ACR_PULL_IDENTITY_RESOURCE_ID}" in user_assigned, (
        f"{source}: expected ACR pull identity placeholder in userAssignedIdentities"
    )

    configuration = (doc.get("properties") or {}).get("configuration") or {}
    registries = configuration.get("registries") or []
    assert registries, f"{source}: expected at least one registry entry"
    assert any(
        entry.get("identity") == "${ACR_PULL_IDENTITY_RESOURCE_ID}" for entry in registries
    ), f"{source}: expected registry identity to use ACR_PULL_IDENTITY_RESOURCE_ID"


def test_all_jobs_wire_user_assigned_identity_client_id() -> None:
    repo_root = _repo_root()
    for path in sorted((repo_root / "deploy").glob("job_*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(doc, dict), f"{path}: expected YAML mapping"
        _assert_azure_client_id_env_present(doc, source=str(path))


def test_all_jobs_use_manifest_managed_identity_for_acr_pull() -> None:
    repo_root = _repo_root()
    for path in sorted((repo_root / "deploy").glob("job_*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(doc, dict), f"{path}: expected YAML mapping"
        _assert_job_manifest_uses_managed_identity_for_acr_pull(doc, source=str(path))


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
    assert "BACKTEST_JOB" in text, "deploy workflow must define the backtest job name"
    assert "GOLD_REGIME_JOB" in text, "deploy workflow must define the gold regime job name"


def test_deploy_workflow_updates_jobs_from_yaml_without_pre_mutating_job_identity() -> None:
    repo_root = _repo_root()
    deploy_workflow = repo_root / ".github" / "workflows" / "deploy.yml"
    text = deploy_workflow.read_text(encoding="utf-8")

    assert "az containerapp job identity assign" not in text, (
        "deploy workflow should not mutate job identity before YAML update"
    )
    assert "az containerapp job registry set" not in text, (
        "deploy workflow should not mutate job registry before YAML update"
    )
    assert "Updating job from YAML (image + identity + registry)..." in text, (
        "deploy workflow should update jobs using the rendered manifest"
    )


def test_deploy_workflow_only_updates_preprovisioned_resources() -> None:
    repo_root = _repo_root()
    deploy_workflow = repo_root / ".github" / "workflows" / "deploy.yml"
    text = deploy_workflow.read_text(encoding="utf-8")

    assert "Validate Pre-Provisioned Deploy Targets" in text, (
        "deploy workflow must validate infrastructure was provisioned outside GitHub Actions"
    )
    assert "Provision it outside GitHub Actions before running deploy." in text, (
        "deploy workflow must fail fast when prerequisite infrastructure is missing"
    )
    assert "Deploy workflow only updates existing apps. Provision it outside GitHub Actions." in text, (
        "deploy workflow must treat missing Container Apps as an external provisioning issue"
    )
    assert "Deploy workflow only updates existing jobs. Provision it outside GitHub Actions." in text, (
        "deploy workflow must treat missing Container App jobs as an external provisioning issue"
    )
    assert "az storage container create" not in text, (
        "deploy workflow must not provision storage containers"
    )
    assert "az containerapp create" not in text, (
        "deploy workflow must not create Container Apps"
    )
    assert "az containerapp job create" not in text, (
        "deploy workflow must not create Container App jobs"
    )
    assert "Apply Repo-Owned Postgres Migrations" not in text, (
        "deploy workflow must not apply repo-owned Postgres migrations"
    )
    assert "apply_postgres_migrations.ps1" not in text, (
        "deploy workflow must not invoke the migration script"
    )


def test_api_manifest_allowlists_backtest_job() -> None:
    repo_root = _repo_root()
    path = repo_root / "deploy" / "app_api.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), f"{path}: expected YAML mapping"
    containers = ((doc.get("properties") or {}).get("template") or {}).get("containers") or []
    api_container = next(
        (container for container in containers if container.get("name") == "asset-allocation-api"),
        None,
    )
    assert api_container, f"{path}: expected asset-allocation-api container"
    env_vars = {entry.get("name"): entry.get("value") for entry in api_container.get("env") or []}
    assert "backtests-job" in str(env_vars.get("SYSTEM_HEALTH_ARM_JOBS") or ""), (
        "app_api manifest must allowlist the backtest ACA job"
    )
    assert env_vars.get("BACKTEST_ACA_JOB_NAME") == "backtests-job", (
        "app_api manifest must export BACKTEST_ACA_JOB_NAME"
    )
    assert "gold-regime-job" in str(env_vars.get("SYSTEM_HEALTH_ARM_JOBS") or ""), (
        "app_api manifest must allowlist the gold regime ACA job"
    )
    assert env_vars.get("REGIME_ACA_JOB_NAME") == "gold-regime-job", (
        "app_api manifest must export REGIME_ACA_JOB_NAME"
    )


def test_gold_regime_job_runs_daily_at_4pm_est() -> None:
    repo_root = _repo_root()
    path = repo_root / "deploy" / "job_gold_regime_data.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), f"{path}: expected YAML mapping"

    configuration = (doc.get("properties") or {}).get("configuration") or {}
    assert configuration.get("triggerType") == "Schedule", (
        "gold regime job must be scheduled"
    )
    schedule = configuration.get("scheduleTriggerConfig") or {}
    assert schedule.get("cronExpression") == "0 21 * * *", (
        "gold regime job must run daily at 21:00 UTC (4:00 PM EST)"
    )


def test_setup_env_seeds_job_defaults_for_github_sync() -> None:
    repo_root = _repo_root()
    setup_env = repo_root / "scripts" / "setup-env.ps1"
    text = setup_env.read_text(encoding="utf-8")

    assert "gold-regime-job" in text, "setup-env must seed the gold regime job name"
    assert '$IsGitHubSyncTarget = $EnvFileName -ieq ".env.web"' in text, (
        "setup-env must detect .env.web targets for GitHub sync defaults"
    )
    assert 'Prompt-Var "ASSET_ALLOCATION_API_BASE_URL" $DefaultAssetAllocationApiBaseUrl' in text, (
        "setup-env must use GitHub-safe API base URL defaults for .env.web"
    )
    assert 'Prompt-Var "VITE_API_PROXY_TARGET" $DefaultViteApiProxyTarget' in text, (
        "setup-env must use GitHub-safe UI proxy defaults for .env.web"
    )
    assert 'Prompt-Var "BACKTEST_ACA_JOB_NAME" "backtests-job"' in text, (
        "setup-env must default BACKTEST_ACA_JOB_NAME for GitHub sync"
    )
    assert 'Prompt-Var "REGIME_ACA_JOB_NAME" "gold-regime-job"' in text, (
        "setup-env must default REGIME_ACA_JOB_NAME for GitHub sync"
    )


def test_sync_all_to_github_treats_aca_job_names_as_variables() -> None:
    repo_root = _repo_root()
    sync_script = repo_root / "scripts" / "sync-all-to-github.ps1"
    text = sync_script.read_text(encoding="utf-8")

    assert "^BACKTEST_ACA_JOB_NAME$" in text, (
        "sync-all-to-github must classify BACKTEST_ACA_JOB_NAME as a GitHub variable"
    )
    assert "^REGIME_ACA_JOB_NAME$" in text, (
        "sync-all-to-github must classify REGIME_ACA_JOB_NAME as a GitHub variable"
    )


def test_env_template_includes_regime_job_defaults() -> None:
    repo_root = _repo_root()
    env_template = repo_root / ".env.template"
    text = env_template.read_text(encoding="utf-8")

    assert "gold-regime-job" in text, ".env.template must include the gold regime job"
    assert "BACKTEST_ACA_JOB_NAME=backtests-job" in text, (
        ".env.template must define BACKTEST_ACA_JOB_NAME"
    )
    assert "REGIME_ACA_JOB_NAME=gold-regime-job" in text, (
        ".env.template must define REGIME_ACA_JOB_NAME"
    )


def test_reset_postgres_script_uses_psql_reset_and_repo_migrations() -> None:
    repo_root = _repo_root()
    reset_script = repo_root / "scripts" / "reset_postgres_from_scratch.ps1"
    text = reset_script.read_text(encoding="utf-8")

    assert "reset_postgres.py" not in text, (
        "reset_postgres_from_scratch should not depend on a local Python helper"
    )
    assert 'Invoke-Psql -Args @($Dsn, "-v", "ON_ERROR_STOP=1", "-c", $resetSql)' in text, (
        "reset_postgres_from_scratch must perform the destructive reset through psql"
    )
    assert '& $migrationScript -Dsn $Dsn -MigrationsDir $resolvedDir -UseDockerPsql:$UseDockerPsql' in text, (
        "reset_postgres_from_scratch must reapply repo-owned migrations through the shared migration script"
    )
    assert "-UseDockerPsql is ignored" not in text, (
        "reset_postgres_from_scratch must honor the UseDockerPsql switch"
    )
