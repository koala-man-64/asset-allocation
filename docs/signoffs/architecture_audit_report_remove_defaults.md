# Architecture & Code Audit Report

## 1. Executive Summary
This change set improves cloud-runtime reliability and configuration correctness for Azure Container Apps Jobs and the backtest service by removing hidden configuration fallbacks and enforcing explicit env-var contracts. The highest-risk failure modes (silent misconfiguration, overlapping job execution due to expired leases, and “green” job runs with partial failures) were addressed. Remaining architectural risks are primarily around configuration drift across environments and ambiguity in the scope of “remove all defaults”.

## 2. System Map (High-Level)
- **Azure Container Apps Jobs** run Python modules under `scripts/*` for Bronze/Silver/Gold ingestion and transformations, using Azure Blob Storage for persistence and Azure Blob Leases for distributed locking.
- **Backtest service** (`backtest/service/app.py`) serves API endpoints and UI assets, and aggregates operational status from storage + optional ARM probes via `monitoring/system_health.py`.
- **CI/CD**
  - `.github/workflows/run_tests.yml` runs UI build/tests and Python pytest.
  - `.github/workflows/deploy.yml` uses `envsubst` to deploy Container Apps + Jobs from `deploy/*.yaml`.

## 3. Findings (Triaged)

### 3.1 Critical (Must Fix)
- None remaining for the scoped changes (tests are green and deploy/CI is updated to meet new env requirements).

### 3.2 Major
- **[Config explicitness increases operational coupling]**
  - **Evidence:** stricter runtime requirements (`BACKTEST_CSP`, `SYSTEM_HEALTH_*`, `LOG_LEVEL`, `DISABLE_DOTENV`, `AZURE_CONTAINER_GOLD`) enforced in code (`backtest/service/*`, `monitoring/system_health.py`, `scripts/common/logging_config.py`, `scripts/common/config_shared.py`).
  - **Why it matters:** environments not updated in lockstep will fail fast.
  - **Recommendation:** treat env-var contract as a versioned interface; centralize required vars and ensure all deploy paths apply the same manifests.
  - **Acceptance Criteria:** all deployed jobs/apps start with no missing-env failures; env contract documented in `.env.template`.
  - **Owner Suggestion:** DevOps Agent / Delivery Engineer Agent.

- **[Storage container naming/creation drift]**
  - **Evidence:** deploy env var mappings in `.github/workflows/deploy.yml` vs container creation steps in deploy/provision scripts.
  - **Why it matters:** runtime may point at containers that do not exist, causing operational failures.
  - **Recommendation:** align container creation with the actual `AZURE_CONTAINER_*` contract used at runtime.
  - **Acceptance Criteria:** all referenced containers exist in the target account; job can read/write.
  - **Owner Suggestion:** DevOps Agent.

### 3.3 Minor
- **[Ambiguity: “remove defaults” beyond env/config]**
  - **Evidence:** many function defaults and algorithm constants remain across the codebase; only env/config fallbacks were removed in this work item.
  - **Recommendation:** clarify the requirement and, if needed, scope a breaking refactor as a separate work item.
  - **Acceptance Criteria:** explicit written decision on scope, with an implementation plan if expanded.
  - **Owner Suggestion:** Architecture Review Agent / Orchestrator.

## 4. Architectural Recommendations
- Continue treating “configuration” as an explicit contract:
  - centralize required env vars per runtime (jobs vs backtest app),
  - validate at startup (fail fast) for critical safety knobs.
- For job reliability:
  - prefer lease renewal for long-running jobs and explicit “lock busy” semantics.
  - ensure every job reports failure via non-zero exit codes when partial work fails.
- For operational readiness:
  - ensure logs are structured and level-configurable across jobs (`LOG_FORMAT`, `LOG_LEVEL`).
  - add canary job run verification in non-prod after changes to env contracts.

## 5. Operational Readiness & Observability
- Required signals:
  - Job completion status (Succeeded/Failed) per job execution.
  - Missing-env failures surfaced early (container startup failures or Python `ValueError`).
  - Lock renewal failures treated as critical (prevents data corruption).
- Release readiness risks tied to telemetry:
  - If `BACKTEST_CSP` or system-health env vars are missing, the backtest service should fail fast rather than run without security headers or correct caching behavior.

## 6. Refactoring Examples (Targeted)
- Distributed lock renewal introduced to prevent lease expiry and overlapping runs (`scripts/common/core.py`).
- Removed env fallbacks that masked misconfiguration (multiple modules; see `docs/signoffs/changes_defaults_removal.patch`).

## 7. Evidence & Telemetry
- Tests: `python3 -m pytest -q` → **141 passed, 3 warnings**
- Repo/workflow snapshot: `docs/signoffs/audit_snapshot_defaults_removal.json`
