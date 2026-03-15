from __future__ import annotations

import csv
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


def test_deploy_workflow_deploys_jobs_from_yaml_without_pre_mutating_job_identity() -> None:
    repo_root = _repo_root()
    deploy_workflow = repo_root / ".github" / "workflows" / "deploy.yml"
    deploy_job_helper = repo_root / "scripts" / "deploy_containerapp_job.sh"
    workflow_text = deploy_workflow.read_text(encoding="utf-8")
    helper_text = deploy_job_helper.read_text(encoding="utf-8")

    assert "az containerapp job identity assign" not in workflow_text, (
        "deploy workflow should not mutate job identity before YAML update"
    )
    assert "az containerapp job registry set" not in workflow_text, (
        "deploy workflow should not mutate job registry before YAML update"
    )
    assert workflow_text.count("bash scripts/deploy_containerapp_job.sh") == 14, (
        "deploy workflow must route every managed Container App job through the shared YAML deploy helper"
    )
    assert "Updating job from YAML (image + identity + registry)..." in helper_text, (
        "job deploy helper should update existing jobs using the rendered manifest"
    )


def test_deploy_workflow_only_requires_preprovisioned_shared_resources() -> None:
    repo_root = _repo_root()
    deploy_workflow = repo_root / ".github" / "workflows" / "deploy.yml"
    deploy_job_helper = repo_root / "scripts" / "deploy_containerapp_job.sh"
    workflow_text = deploy_workflow.read_text(encoding="utf-8")
    helper_text = deploy_job_helper.read_text(encoding="utf-8")

    assert "Validate Pre-Provisioned Deploy Targets" in workflow_text, (
        "deploy workflow must validate infrastructure was provisioned outside GitHub Actions"
    )
    assert "Provision it outside GitHub Actions before running deploy." in workflow_text, (
        "deploy workflow must fail fast when prerequisite infrastructure is missing"
    )
    assert "Container App Job '" not in workflow_text, (
        "deploy workflow should no longer fail fast when a managed Container App job is absent"
    )
    assert "Deploy workflow only updates existing jobs. Provision it outside GitHub Actions." not in workflow_text, (
        "deploy workflow should not treat missing Container App jobs as an external provisioning issue"
    )
    assert "az storage container create" not in workflow_text, (
        "deploy workflow must not provision storage containers"
    )
    assert "az containerapp job create" in helper_text, (
        "job deploy helper must create missing Container App jobs from YAML"
    )
    assert 'az containerapp job create \\\n    --name "$job_name" \\\n    --resource-group "$RESOURCE_GROUP" \\\n    --yaml "$tmp_file" \\\n    --only-show-errors' in helper_text, (
        "job deploy helper must pass the explicit job name when creating from YAML"
    )
    assert "Creating job from YAML (image + identity + registry)..." in helper_text, (
        "job deploy helper must create missing jobs using the rendered manifest"
    )
    assert "Apply Repo-Owned Postgres Migrations" not in workflow_text, (
        "deploy workflow must not apply repo-owned Postgres migrations"
    )
    assert "apply_postgres_migrations.ps1" not in workflow_text, (
        "deploy workflow must not invoke the migration script"
    )


def test_deploy_workflow_creates_missing_api_app_from_yaml() -> None:
    repo_root = _repo_root()
    deploy_workflow = repo_root / ".github" / "workflows" / "deploy.yml"
    text = deploy_workflow.read_text(encoding="utf-8")

    assert "Creating Container App from rendered YAML..." in text, (
        "deploy workflow must create the unified Container App when it is missing"
    )
    assert "az containerapp create" in text, (
        "deploy workflow must create the unified Container App from YAML"
    )
    assert 'az containerapp create \\\n              --name ${{ env.API_APP_NAME }} \\\n              --resource-group ${{ env.RESOURCE_GROUP }} \\\n              --yaml "$tmp" \\\n              --only-show-errors' in text, (
        "deploy workflow must pass the explicit Container App name when creating from YAML"
    )
    assert '--yaml "$tmp"' in text, (
        "deploy workflow must create the unified Container App from the rendered manifest"
    )
    assert "Deploy workflow only updates existing apps. Provision it outside GitHub Actions." not in text, (
        "deploy workflow should not fail fast when the unified Container App is absent"
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


def test_env_contract_tracks_aca_job_names_as_checked_in_defaults() -> None:
    repo_root = _repo_root()
    contract = repo_root / "docs" / "ops" / "env-contract.csv"
    with contract.open(encoding="utf-8", newline="") as handle:
        rows = {row["name"]: row for row in csv.DictReader(handle)}

    assert rows["BACKTEST_ACA_JOB_NAME"]["class"] == "deploy_var"
    assert rows["BACKTEST_ACA_JOB_NAME"]["github_storage"] == "none"
    assert rows["REGIME_ACA_JOB_NAME"]["class"] == "deploy_var"
    assert rows["REGIME_ACA_JOB_NAME"]["github_storage"] == "none"


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
