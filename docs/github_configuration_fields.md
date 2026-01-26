# Architecture & Code Audit Report

**Scope:** GitHub Actions configuration inputs (GitHub Secrets / Variables) required or implied by this repository.

## 1. Executive Summary
GitHub Actions workflows in this repo depend on a set of GitHub Secrets for Azure OIDC login, storage access, external data providers, Postgres DSNs, and Backtest API authentication. Some runtime-critical fields are referenced in deployment templates but are not consistently validated in workflow preflight checks. For the UI “Run job” action (Backtest API → Azure Container Apps Jobs) to persist across redeploys, ARM/job monitoring configuration should be promoted into GitHub-managed config and wired into the deploy manifest.

## 2. System Map (High-Level)
- **CI/CD workflows:** `.github/workflows/deploy.yml`, `.github/workflows/run_tests.yml`, `.github/workflows/trigger_all_jobs.yml`
- **Deploy mechanism:** `envsubst`-rendered ACA YAML templates (e.g., `deploy/app_backtest_api.yaml`, `deploy/job_*.yaml`) applied via `az containerapp ... --yaml ...`
- **Runtime config path:** GitHub Secrets → workflow `env:` → `envsubst` → Azure Container Apps env vars/secrets
- **Job triggering feature:** Backtest API endpoint reads ARM/job env vars (see `api/endpoints/system.py` `POST /api/system/jobs/{job_name}/run`)

## 3. Findings (Triaged)

### 3.1 Critical (Must Fix)
- **Deploy can break if `BACKTEST_AUTH_MODE` is unset**
  - **Evidence:** `api/service/settings.py` requires `BACKTEST_AUTH_MODE`; `deploy/app_backtest_api.yaml` templates `${BACKTEST_AUTH_MODE}`; `.github/workflows/deploy.yml` sources it from `secrets.BACKTEST_AUTH_MODE`, but the deploy “Validate required secrets” preflight does not enforce it.
  - **Why it matters:** A missing/empty secret can produce a container that fails to start after redeploy.
  - **Recommendation:** Treat `BACKTEST_AUTH_MODE` as required and validate it in deploy preflight.
  - **Acceptance Criteria:** Deploy workflow fails fast if missing; Backtest API starts after redeploy.
  - **Owner Suggestion:** DevOps Agent / Delivery Engineer Agent

### 3.2 Major
- **Job-trigger + ARM health config won’t persist unless GitHub-managed**
  - **Evidence:** Job trigger reads `SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID`, `SYSTEM_HEALTH_ARM_RESOURCE_GROUP`, `SYSTEM_HEALTH_ARM_JOBS`; these are not present in `deploy/app_backtest_api.yaml`.
  - **Why it matters:** Manually set Azure env vars can be overwritten by future redeploys.
  - **Recommendation:** Add these fields to GitHub-managed config and wire them into the deploy template.
  - **Acceptance Criteria:** After redeploy, `/api/system/jobs/{job}/run` still works and job executions still show up in `/api/system/health`.
  - **Owner Suggestion:** DevOps Agent / Delivery Engineer Agent

- **Secret inventory is fragmented across workflows**
  - **Evidence:** Multiple “Validate required secrets” blocks exist (`deploy.yml`, `run_tests.yml`) with different coverage.
  - **Why it matters:** Higher risk of misconfiguration across environments/repos.
  - **Recommendation:** Consolidate documentation and align validations to one authoritative list.
  - **Acceptance Criteria:** A single documented list and consistent workflow preflight checks.
  - **Owner Suggestion:** DevOps Agent

### 3.3 Minor
- **Docs mention `AZURE_CREDENTIALS`, but workflows use OIDC triplet**
  - **Evidence:** `docs/azure_resource_audit.md` vs workflow usage of `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`.
  - **Why it matters:** Confusing setup guidance.
  - **Recommendation:** Update docs to match current workflow requirements.
  - **Acceptance Criteria:** Docs list matches workflows.
  - **Owner Suggestion:** Delivery Engineer Agent

## 4. Architectural Recommendations

### 4.1 GitHub Secrets referenced by workflows (configure in GitHub)
These fields are directly referenced via `${{ secrets.<NAME> }}` in `.github/workflows/*`.

- **Azure OIDC (deploy + manual job trigger):**
  - `AZURE_CLIENT_ID`
  - `AZURE_TENANT_ID`
  - `AZURE_SUBSCRIPTION_ID`
- **Azure Storage (deploy/tests/jobs):**
  - `AZURE_STORAGE_CONNECTION_STRING`
  - `AZURE_STORAGE_ACCOUNT_NAME`
- **External data providers (deploy/jobs):**
  - `YAHOO_USERNAME`
  - `YAHOO_PASSWORD`
  - `NASDAQ_API_KEY`
- **Databases (deploy/jobs):**
  - `RANKING_POSTGRES_DSN`
  - `BACKTEST_POSTGRES_DSN`
- **Backtest API security (deploy):**
  - `BACKTEST_API_KEY`
  - `BACKTEST_CSP`
  - `BACKTEST_AUTH_MODE`
- **Backtest API OIDC config (deploy; required when `BACKTEST_AUTH_MODE` enables OIDC):**
  - `BACKTEST_OIDC_ISSUER`
  - `BACKTEST_OIDC_AUDIENCE`
  - `BACKTEST_OIDC_JWKS_URL`
  - `BACKTEST_OIDC_REQUIRED_SCOPES`
  - `BACKTEST_OIDC_REQUIRED_ROLES`
- **UI runtime auth config served by backtest-api (deploy; optional):**
  - `BACKTEST_UI_AUTH_MODE`
  - `BACKTEST_UI_OIDC_CLIENT_ID`
  - `BACKTEST_UI_OIDC_AUTHORITY`
  - `BACKTEST_UI_OIDC_SCOPES`
  - `BACKTEST_UI_API_BASE_URL`

### 4.2 GitHub-managed fields recommended to persist “Run job” + job log tails across redeploys
These are required/used by the Backtest API and/or system health collection logic; ensure they are set in GitHub-managed config so deploy templates can render them.

- **Required for UI “Run job” allowlist + ARM start calls:**
  - `SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID`
  - `SYSTEM_HEALTH_ARM_RESOURCE_GROUP`
  - `SYSTEM_HEALTH_ARM_JOBS` (comma-separated job names allowed to start)
- **Optional (but required if you want `/api/system/health` ARM probe behavior configured explicitly):**
  - `SYSTEM_HEALTH_ARM_CONTAINERAPPS` (comma-separated names)
  - `SYSTEM_HEALTH_ARM_API_VERSION`
  - `SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS`
  - `SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB`

- **Required for `/api/system/jobs/{job_name}/logs` (Log Analytics):**
  - `SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED`
  - `SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID`
  - `SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS`

## 5. Operational Readiness & Observability
- Prefer **GitHub Variables** for non-sensitive config (e.g., subscription/resource group, job allowlists) and **GitHub Secrets** for secrets (connection strings, API keys, passwords).
- Ensure the `backtest-api` managed identity has RBAC to start Container Apps Jobs (`Microsoft.App/jobs/start/action`); otherwise `/api/system/jobs/{job}/run` will return errors even if configuration is present.

## 6. Refactoring Examples (Targeted)
- Add missing deploy preflight validation for `BACKTEST_AUTH_MODE` in `.github/workflows/deploy.yml` alongside existing secret checks.
- Add `SYSTEM_HEALTH_ARM_*` fields to `deploy/app_backtest_api.yaml` and source them from GitHub-managed config.

## 7. Evidence & Telemetry
- Files reviewed:
  - `.github/workflows/deploy.yml`
  - `.github/workflows/run_tests.yml`
  - `.github/workflows/trigger_all_jobs.yml`
  - `deploy/app_backtest_api.yaml`
  - `deploy/job_*.yaml`
  - `services/backtest_api/app.py`
  - `api/service/settings.py`
  - `monitoring/system_health.py`
  - `.env.template`

