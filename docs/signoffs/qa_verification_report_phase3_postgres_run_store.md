# QA Verification Report

## 1. Executive Summary
- **Overall confidence level:** Medium
- **Scope:** Phase 3 only — Postgres-backed backtest run store (`backtest.runs`) + service wiring/config; artifacts remain in ADLS/Blob.
- **Top risks remaining:** Migration/permissions not validated by automated tests; Postgres connectivity must be validated in a deployed environment; multi-replica execution remains unsafe (must keep `maxReplicas: 1`).

## 2. Test Matrix (Functionality Coverage)
| Feature/Flow | Risk | Test Type | Local | Dev | Prod | Status | Notes |
|---|---:|---|---|---|---|---|---|
| `BACKTEST_RUN_STORE_MODE=postgres` parsing + DSN requirement | Medium | Unit | ✅ | N/A | N/A | Pass | Validates fail-fast config |
| Backtest API startup selects Postgres store | Medium | Unit | ✅ | N/A | N/A | Pass | Uses fake store to avoid network |
| Backtest service core flows (sqlite + ADLS artifacts) | Medium | Integration-style | ✅ | N/A | N/A | Pass | Regression coverage for existing behaviors |
| Postgres migration 0004 applied | High | Manual | Planned | Recommended | Safe-only | Pending | Requires Postgres admin DSN |
| Submit run → row inserted/updated in Postgres | High | Manual smoke | Planned | Recommended | Safe-only | Pending | Validate create/update/list semantics |
| Restart reconciliation (queued/running → failed) | Medium | Manual | Planned | Recommended | N/A | Pending | Validate startup behavior |
| Rollback to `adls` run store | Low | Manual | Planned | Recommended | N/A | Pending | Ensure service remains operable |

## 3. Test Cases (Prioritized)
- **TC1: Unit — settings validation**
  - Steps: `PYTHONPATH=$PWD pytest -q tests/backtest/test_postgres_run_store_mode.py`
  - Expected: test validates `BACKTEST_POSTGRES_DSN` required when mode is postgres.
  - Status: Pass

- **TC2: Regression — backtest service API behavior**
  - Steps: `PYTHONPATH=$PWD pytest -q tests/backtest/test_phase3_service_api.py`
  - Expected: service submits/runs a backtest and serves artifacts; ADLS upload paths work under fakes.
  - Status: Pass

- **TC3: Migration apply (dev/staging)**
  - Steps: `pwsh deploy/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>" -UseDockerPsql`
  - Expected:
    - `backtest.runs` exists.
    - Re-running migrations is idempotent and exits cleanly.
  - Status: Pending (environment required)

- **TC4: Postgres-mode smoke (dev/staging)**
  - Preconditions:
    - `BACKTEST_RUN_STORE_MODE=postgres`
    - `BACKTEST_POSTGRES_DSN` set (service role recommended)
    - Migration 0004 applied
  - Steps:
    1) Deploy backtest API.
    2) Submit a run.
    3) Query Postgres:
       - `SELECT run_id, status, submitted_at, started_at, completed_at FROM backtest.runs ORDER BY submitted_at DESC LIMIT 25;`
  - Expected:
    - A new row exists for the submitted run.
    - Status transitions reflect execution (`queued` → `running` → `completed|failed`).
  - Status: Pending

- **TC5: Restart reconciliation (dev/staging)**
  - Steps:
    1) Submit a run, restart the service while it is `queued`/`running`.
    2) Confirm the row is updated to `failed` with `error` populated.
  - Expected: queued/running runs do not remain stuck after restarts.
  - Status: Pending

## 4. Automated Tests Added/Updated (If applicable)
- Added: `tests/backtest/test_postgres_run_store_mode.py` (mode parsing + app store selection).

## 5. Environment Verification
### Local (Required)
- `PYTHONPATH=$PWD pytest -q tests/backtest/test_postgres_run_store_mode.py` → **2 passed**
- `PYTHONPATH=$PWD pytest -q tests/backtest/test_phase3_service_api.py` → **8 passed**

### Dev (Recommended)
- Run TC3–TC5 once Postgres is reachable and migrations are applied.

### Prod (Optional, Safe-Only)
- Safe-only checks after dev passes:
  - `GET /healthz` and `GET /readyz`
  - Submit a single canary run to a test-only ADLS prefix and confirm `backtest.runs` updates (no destructive tests beyond normal operation).

## 6. CI/CD Verification (If applicable)
- Deploy workflow injects `BACKTEST_POSTGRES_DSN` only in the deploy step environment (`.github/workflows/deploy.yml:267`) and renders Container Apps YAML via `envsubst` (temporary file is removed) (`.github/workflows/deploy.yml:301`).
- No `pull_request_target` workflows detected (`rg -n "pull_request_target" .github/workflows` → none).

## 7. Release Readiness Gate
- **Gate decision:** Pass with conditions
- **Conditions to enable Postgres run store in a deployed environment:**
  - Apply migration `deploy/sql/postgres/migrations/0004_backtest_runs.sql`.
  - Set `BACKTEST_POSTGRES_DSN` secret for the backtest API.
  - Keep `maxReplicas: 1` while runs execute in-process.

## 8. Evidence & Telemetry
- Local tests:
  - `PYTHONPATH=$PWD pytest -q tests/backtest/test_postgres_run_store_mode.py` → **2 passed**
  - `PYTHONPATH=$PWD pytest -q tests/backtest/test_phase3_service_api.py` → **8 passed**

## 9. Gaps & Recommendations
- Add a dev/staging smoke runbook step (or a separate dev-only CI job) that runs the backtest API against a Postgres test instance/container to validate SQL behavior end-to-end.
- Add monitoring for run-store errors and Postgres connection failures before enabling broader usage.

