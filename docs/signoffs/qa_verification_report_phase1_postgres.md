# QA Verification Report

## 1. Executive Summary
- **Overall confidence level:** Medium
- **Scope:** Phase 1 only (Postgres provisioning + migrations baseline + deploy/workflow secret wiring + backtest replica cap).
- **Top risks remaining:** Postgres DSN secret availability at deploy time; public networking exposure (until private networking is added); and existing PR CI workflows that run with cloud secrets (unrelated but relevant to overall posture).

## 2. Test Matrix (Functionality Coverage)
| Feature/Flow | Risk | Test Type | Local | Dev | Prod | Status | Notes |
|---|---:|---|---|---|---|---|---|
| Provision Postgres Flexible Server | High | Manual/scripted | Planned | Recommended | N/A | Planned | Requires Azure subscription access |
| Apply migrations (idempotent) | High | Manual/scripted | Planned | Recommended | N/A | Planned | Validate `schema_migrations` |
| Deploy workflow secret wiring | High | CI review + deploy smoke | Reviewed | Planned | Safe-only | Partial | Ensure DSNs only in deploy workflow |
| Backtest API replica safety | High | Config + runtime smoke | Reviewed | Planned | Safe-only | Partial | `maxReplicas: 1` enforced in YAML |

## 3. Test Cases (Prioritized)
- **TC1: YAML validity**
  - Steps: parse YAML manifests/workflows.
  - Expected: YAML parses without errors.
  - Evidence: executed locally (see Evidence section).

- **TC2: PowerShell script syntax**
  - Steps: parse PowerShell scripts via `System.Management.Automation.Language.Parser`.
  - Expected: no parse errors.
  - Evidence: executed locally (see Evidence section).

- **TC3: Provisioning + migrations (staging)**
  - Preconditions: Azure subscription + permissions; Azure CLI authenticated.
  - Steps:
    1) `pwsh deploy/provision_azure_postgres.ps1 -SubscriptionId ... -ServerName ... -AllowAzureServices -ApplyMigrations -CreateAppUsers -UseDockerPsql`
    2) Apply migrations with `pwsh deploy/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>" -UseDockerPsql`
    3) Re-run migration script to confirm idempotency.
  - Expected:
    - DB exists and is reachable.
    - `public.schema_migrations` contains applied versions.
    - Second run applies nothing and exits cleanly.
  - Notes:
    - If Azure returns “location is restricted”, re-run with `-Location eastus2` (or set `-LocationFallback`).
    - Burstable SKUs (e.g., `standard_b1ms`) require `--tier Burstable` (the script auto-selects tier based on SKU).
    - If re-running with `-CreateAppUsers`, confirm downstream systems update to the rotated passwords (or pass the existing passwords explicitly).
    - If the server exists but the database does not, `db show` may return `ResourceNotFound` and the script should then create the database.

- **TC4: Deploy smoke (staging)**
  - Preconditions: GitHub secrets `RANKING_POSTGRES_DSN` and `BACKTEST_POSTGRES_DSN` present.
  - Steps: run `.github/workflows/deploy.yml` on `main` or via manual dispatch.
  - Expected:
    - No logs print DSN values.
    - Rendered temp YAML is deleted.
    - Container Apps resources deploy successfully.

## 4. Automated Tests Added/Updated (If applicable)
- None (Phase 1 is infra + manifests + docs).

## 5. Environment Verification
### Local (Required)
- YAML parse commands:
  - `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/deploy.yml'))"`
  - `python3 -c "import yaml; yaml.safe_load(open('deploy/app_backtest_api.yaml'))"`
  - `python3 -c "import yaml; yaml.safe_load(open('deploy/job_platinum_ranking.yaml'))"`
- PowerShell parse command:
  - `powershell.exe -NoProfile -Command "...Parser]::ParseFile(...)"` (see Evidence)

### Dev (Optional)
- Provision + migrate Postgres in a dev RG and run TC3/TC4.

### Prod (Optional, Safe-Only)
- Post-deploy safe checks:
  - Backtest API `/healthz` and `/readyz`
  - Verify Container Apps shows secret refs configured (names only).

## 6. CI/CD Verification (If applicable)
- Verified Postgres DSN secrets are only referenced in `.github/workflows/deploy.yml` (not in PR workflows).
- Note: `.github/workflows/run_tests.yml` runs on `pull_request` and uses cloud secrets; treat this as acceptable only if PR contributors are trusted.

## 7. Release Readiness Gate
- **Gate decision:** Pass for merging Phase 1 changes.
- **Conditions to proceed to Phase 2/3:** Complete TC3/TC4 in a staging/dev environment and confirm secrets are not exposed in logs.

## 8. Evidence & Telemetry
- YAML parse: OK (local)
- PowerShell parse: OK (local)

## 9. Gaps & Recommendations
- Add a staging runbook checklist to confirm DSN secrets exist before deploy.
- Consider splitting PR CI workflows to avoid running with cloud secrets on untrusted PRs.

## 10. Handoffs (Only if needed)
- `Handoff: DevOps Agent` — tighten PR secret exposure posture; confirm deploy-only secrets.
- `Handoff: Delivery Engineer Agent` — add Phase 2 migrations for tables/indexes only when writers/readers land.
