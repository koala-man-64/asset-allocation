# Project & Workflow Audit Report

## 1. Executive Summary
Overall posture is **Medium risk** for delivery: CI and deploy workflows are generally well-structured (pinned actions/images, explicit secrets validation), and this change set aligned CI/deploy manifests with newly-required runtime env vars. The highest residual risks are configuration drift across non-GitHub deployment paths and naming/creation drift for Azure Storage containers. The repository also contains some potentially-unused/vestigial packaging configuration that can be cleaned up without functional impact.

## 2. Scope & Assumptions
- **In scope:** repo config and workflows related to the “remove defaults” delivery (`.github/workflows/*`, `deploy/*`, `.env.template`, runtime env parsing patterns).
- **Excluded:** live Azure runtime verification (no environment access), deep static analysis for dead code (no dedicated tool like vulture configured).
- **Assumptions:** Container Apps deploys are executed via `.github/workflows/deploy.yml` using `envsubst`.

## 3. Inventory Snapshot
- **Languages/runtimes:** Python 3.10+, FastAPI (backtest service), GitHub Actions, Azure Container Apps Jobs.
- **Workflows:**
  - `.github/workflows/run_tests.yml` — builds UI + runs pytest.
  - `.github/workflows/deploy.yml` — envsubst → deploy Container Apps + Jobs.
  - `.github/workflows/trigger_all_jobs.yml` — manual job trigger.
  - `.github/workflows/lint_workflows.yml` — workflow linting.
- **Policy/instruction files:** `CONTRIBUTING.md`, `SECURITY.md`, `.gitignore` (env/secrets hygiene).
- **Audit artifact:** `docs/signoffs/audit_snapshot_defaults_removal.json`.

## 4. Findings (Triaged)

### 4.1 Critical (Must Fix)
- None identified in the scoped change set.

### 4.2 Major
- **[Deploy/CI drift risk due to stricter required env vars]**
  - **Evidence:** runtime now requires explicit env vars (e.g., `BACKTEST_CSP`, `SYSTEM_HEALTH_TTL_SECONDS`, `LOG_LEVEL`, `AZURE_CONTAINER_GOLD`); manifests/workflows were updated in `deploy/job_*.yaml`, `deploy/app_backtest_api.yaml`, `.github/workflows/run_tests.yml`, `.github/workflows/deploy.yml`.
  - **Why it matters:** any deployment path not using these updated manifests can fail hard at startup/runtime with “<ENV> is required”.
  - **Recommendation:** ensure all deployment paths use the repo manifests, or mirror required env vars in other IaC/scripts; keep env var contract documented (`.env.template`).
  - **Acceptance Criteria:** all deployed jobs/apps start successfully with no “is required” errors; CI remains green.
  - **Owner Suggestion:** DevOps Agent / Delivery Engineer Agent.

- **[Storage container creation may not match declared container mappings]**
  - **Evidence:** `deploy.yml` sets `AZURE_CONTAINER_MARKET=market-data`, `AZURE_CONTAINER_FINANCE=finance-data`, etc; provisioning/deploy steps create only `bronze`, `silver`, `gold`, `platinum` (see `deploy/provision_azure.ps1` default `$StorageContainers` and `.github/workflows/deploy.yml` “Ensure Storage Containers Exist” step).
  - **Why it matters:** if runtime expects per-domain containers, jobs may fail at runtime due to missing containers.
  - **Recommendation:** align container creation to the container naming contract used by deploy env vars; either create all referenced containers or standardize on medallion containers only.
  - **Acceptance Criteria:** storage containers referenced by all `AZURE_CONTAINER_*` env vars exist in the target account; jobs can list/write blobs without container-not-found errors.
  - **Owner Suggestion:** DevOps Agent.

### 4.3 Minor
- **[Potentially unused/vestigial package config]**
  - **Evidence:** `pyproject.toml` includes `asset_allocation*` packages and package-data for `"asset_allocation.ui"` while `asset_allocation/` contains only `asset_allocation/__init__.py`.
  - **Why it matters:** increases maintenance confusion; can mislead contributors about the canonical package layout.
  - **Recommendation:** either remove unused package-data entries or restore the intended package structure.
  - **Acceptance Criteria:** packaging config matches on-disk packages; `pip install -e .` produces expected import roots.
  - **Owner Suggestion:** Code Hygiene Agent / Delivery Engineer Agent.

## 5. Roadmap (Phased)
- **Quick wins (0-2 days)**
  - Align storage container creation with `AZURE_CONTAINER_*` values.
  - Document required env var contract (already surfaced via `.env.template`).
- **Near-term (1-2 weeks)**
  - Add a lightweight dead-code scan tool (optional) and/or remove vestigial packaging entries.
- **Later (backlog)**
  - Decide whether “no defaults” must include all function parameter defaults and algorithmic constants; scope as a breaking refactor if required.

## 6. Release/Delivery Gates
- Tests: **Pass** (`python3 -m pytest -q` → 141 passed).
- CI workflow safety: **Pass** (pinned actions/images; secrets validated; deploy checks improved).
- Deploy readiness: **Pass with caveat** (must ensure all environments set required env vars and referenced storage containers exist).

## 7. Evidence Log
- Audit snapshot: `python3 /home/rdprokes/.codex/skills/project-workflow-auditor-agent/scripts/audit_snapshot.py --repo . --out docs/signoffs/audit_snapshot_defaults_removal.json`
- Tests: `python3 -m pytest -q` → **141 passed, 3 warnings**
