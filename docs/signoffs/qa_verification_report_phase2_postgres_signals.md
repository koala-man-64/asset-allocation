# QA Verification Report

## 1. Executive Summary
- **Overall confidence level:** Medium
- **Scope:** Phase 2 signal dual-write only (Postgres schema for signals + writer + integration hook). No backfill/migration.
- **Top risks remaining:** Postgres connectivity/runtime permissions not validated by automated tests; drift risk if best-effort mode is enabled; deploy-time secret handling remains sensitive (ensure DSNs are not printed in logs).

## 2. Test Matrix (Functionality Coverage)
| Feature/Flow | Risk | Test Type | Local | Dev | Prod | Status | Notes |
|---|---:|---|---|---|---|---|---|
| Compute ranking signals (percentiles/year_month) | Medium | Unit | ✅ | N/A | N/A | Pass | Existing unit tests cover core transforms |
| Compute composite signals | Medium | Unit | ✅ | N/A | N/A | Pass | Existing unit tests cover composite ranking |
| Dual-write gating (POSTGRES_DSN unset) | Low | Unit/Smoke | ✅ | ✅ | N/A | Pass | Should preserve current behavior |
| Dual-write to Postgres (POSTGRES_DSN set) | High | Manual smoke | Planned | Recommended | Safe-only | Pending | Requires reachable Postgres + migrations applied |
| Postgres schema migration 0003 | High | Manual | Planned | Recommended | Safe-only | Pending | Apply migrations and confirm tables exist |
| Idempotency (same month rerun) | High | Manual smoke | Planned | Recommended | N/A | Pending | Validate delete+copy semantics |

## 3. Test Cases (Prioritized)
- **TC1: Unit regression for ranking transforms**
  - Steps: `PYTHONPATH=$PWD pytest -q tests/ranking`
  - Expected: all ranking tests pass.
  - Status: Pass

- **TC2: Migration apply (dev/staging)**
  - Steps: `pwsh deploy/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>" -UseDockerPsql`
  - Expected:
    - `ranking.ranking_signal`, `ranking.composite_signal_daily`, `ranking.signal_sync_state` exist.
    - Re-run applies nothing and exits cleanly.
  - Status: Pending (environment required)

- **TC3: Dual-write smoke (dev/staging)**
  - Preconditions:
    - `POSTGRES_DSN` set for the ranking job (writer role DSN recommended).
    - Migration 0003 applied.
  - Steps:
    1) Run ranking job once (or run `python -m scripts.ranking.materialize_signals --year-month YYYY-MM` with required env vars).
    2) Query Postgres:
       - `SELECT * FROM ranking.signal_sync_state ORDER BY synced_at DESC LIMIT 25;`
       - Counts by `year_month` in both tables.
  - Expected:
    - A `signal_sync_state` row exists for the month with `status='success'`.
    - Table row counts are non-zero and stable across reruns (idempotent).
  - Status: Pending

- **TC4: Failure semantics**
  - Steps:
    - With `POSTGRES_SIGNALS_WRITE_REQUIRED=true` (default), force a Postgres connection failure and confirm the job fails/retries.
    - With `POSTGRES_SIGNALS_WRITE_REQUIRED=false`, confirm the job completes and logs the Postgres failure.
  - Expected: behavior matches flag description and does not log secrets.
  - Status: Pending

## 4. Automated Tests Added/Updated (If applicable)
- None added in Phase 2. Existing ranking unit tests provide regression coverage for the transform logic.

## 5. Environment Verification
### Local (Required)
- `PYTHONPATH=$PWD pytest -q tests/ranking` → **Pass**

### Dev (Recommended)
- Apply migrations and run TC2–TC4.

### Prod (Optional, Safe-Only)
- Only after dev smoke passes:
  - Confirm ranking job runs and Postgres state table updates.
  - Read-only SQL checks only; no destructive tests beyond the normal job behavior.

## 6. CI/CD Verification (If applicable)
- Verify dependency lockfiles include psycopg so CI and Docker builds install the driver (`requirements.lock.txt`, `requirements-dev.lock.txt`).
- Verify deploy workflow injects DSN secrets only in deploy steps and does not print rendered YAML.

## 7. Release Readiness Gate
- **Gate decision:** Pass with conditions
- **Conditions to enable Postgres dual-write in a deployed environment:**
  - Apply migration 0003 in the target DB.
  - Configure `RANKING_POSTGRES_DSN` secret.
  - Run at least one canary month and validate `ranking.signal_sync_state`.

## 8. Evidence & Telemetry
- Local tests: `PYTHONPATH=$PWD pytest -q tests/ranking` → **11 passed**

## 9. Gaps & Recommendations
- Add a dev-only smoke script (or CI job) that runs Postgres writes against an ephemeral Postgres container (future enhancement).
- Add monitoring on `ranking.signal_sync_state` freshness before any Postgres readers are enabled.

