# Implementation Report

## 1. Execution Summary
- Implemented Phase 3 backtest **run-state persistence in Postgres** while keeping **artifacts in ADLS/Blob**.
- Added a repo-owned Postgres migration for the backtest run table and a Postgres-backed RunStore implementation used by the FastAPI backtest service when configured.
- Updated service configuration parsing and Container Apps manifest to enable `BACKTEST_RUN_STORE_MODE=postgres`.

**Out of scope**
- Migrating/backfilling existing ADLS/SQLite runs into Postgres.
- Enabling multi-replica backtest execution (service remains single-replica while runs execute in-process).
- Postgres-backed signal reads (future phase).

## 2. Architectural Alignment Matrix
- **Requirement:** “Postgres is the serving/state layer for backtest run metadata; artifacts remain in ADLS.”
  - **Implementation:** `backtest.runs` migration + `PostgresRunStore` for run state; ADLS artifact upload remains unchanged via `BACKTEST_ADLS_RUNS_DIR` (`deploy/sql/postgres/migrations/0004_backtest_runs.sql:3`, `asset_allocation/backtest/service/postgres_run_store.py:34`, `asset_allocation/backtest/service/app.py:105`).
  - **Status:** Complete.

- **Requirement:** “Explicit, env-driven configuration; fail fast if misconfigured.”
  - **Implementation:** `BACKTEST_RUN_STORE_MODE` accepts `postgres` and requires `BACKTEST_POSTGRES_DSN` (`asset_allocation/backtest/service/settings.py:107`, `asset_allocation/backtest/service/settings.py:223`).
  - **Status:** Complete.

- **Requirement:** “Cloud readiness should validate the configured backing store.”
  - **Implementation:** `/readyz` calls `store.ping()` when available (Postgres + ADLS) (`asset_allocation/backtest/service/app.py:207`).
  - **Status:** Complete.

## 3. Change Set
**Added**
- `deploy/sql/postgres/migrations/0004_backtest_runs.sql`
- `asset_allocation/backtest/service/postgres_run_store.py`
- `docs/postgres_phase3.md`
- `tests/backtest/test_postgres_run_store_mode.py`
- `docs/signoffs/implementation_report_phase3_postgres_run_store.md`

**Modified**
- `asset_allocation/backtest/service/settings.py` (add `postgres` mode + DSN requirement)
- `asset_allocation/backtest/service/app.py` (select `PostgresRunStore`; readiness ping)
- `deploy/app_backtest_api.yaml` (set `BACKTEST_RUN_STORE_MODE=postgres`)
- `docs/backtest_service.md` (document postgres mode + DSN env var)

**Key Interfaces**
- **Env vars**
  - `BACKTEST_RUN_STORE_MODE=postgres` — enables Postgres-backed run store.
  - `BACKTEST_POSTGRES_DSN` — required when mode is postgres (service DB DSN).
  - `BACKTEST_ADLS_RUNS_DIR` — still used as the default artifact upload prefix when `output.adls_dir` is not provided.
- **Postgres tables**
  - `backtest.runs`

## 4. Code Implementation
- Store selection on startup:
  - `asset_allocation/backtest/service/app.py:105` chooses `PostgresRunStore` when `run_store_mode == "postgres"`.
- Postgres run store implementation:
  - `asset_allocation/backtest/service/postgres_run_store.py:34` implements `init_db`, `ping`, `create_run`, `update_run`, `get_run`, `list_runs`, and startup reconciliation.
- Schema migration:
  - `deploy/sql/postgres/migrations/0004_backtest_runs.sql:3` creates `backtest.runs` with status constraint + indexes + role grants.

## 5. Observability & Operational Readiness
- **Readiness:** `/readyz` validates the backing store via `ping()` when present (Postgres/ADLS) (`asset_allocation/backtest/service/app.py:207`).
- **Startup safety:** queued/running runs are reconciled to failed on startup to avoid stale state (`asset_allocation/backtest/service/postgres_run_store.py:66`).
- **Secrets:** DSNs are not logged (DSN is never printed by the store/app).
- **Rollback:** switch `BACKTEST_RUN_STORE_MODE` back to `adls` or `sqlite` (artifacts remain in ADLS).

## 6. Cloud-Native Configuration (If applicable)
- Backtest API Container Apps manifest enables Postgres run store:
  - `deploy/app_backtest_api.yaml:73` sets `BACKTEST_RUN_STORE_MODE=postgres`
  - `deploy/app_backtest_api.yaml:37` wires `BACKTEST_POSTGRES_DSN` as a secretRef
  - Single-replica constraint remains in place (`deploy/app_backtest_api.yaml:83`).

## 7. Verification Steps
- Apply migrations (dev/staging):
  - `pwsh deploy/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>" -UseDockerPsql`
- Local tests:
  - `PYTHONPATH=$PWD pytest -q tests/backtest/test_postgres_run_store_mode.py`
  - `PYTHONPATH=$PWD pytest -q tests/backtest/test_phase3_service_api.py`
- Dev smoke (recommended):
  - Deploy with `BACKTEST_RUN_STORE_MODE=postgres`, submit a run, and validate:
    - `SELECT run_id, status, submitted_at FROM backtest.runs ORDER BY submitted_at DESC LIMIT 25;`

## 8. Risks & Follow-ups
- If backtest API is ever scaled beyond one replica, in-process execution can duplicate work; add distributed leases before allowing multi-replica execution.
- If run volume grows materially, consider adding a connection pool for PostgresRunStore to reduce connection churn.

## 9. Evidence & Telemetry
- `PYTHONPATH=$PWD pytest -q tests/backtest/test_postgres_run_store_mode.py` → **2 passed**
- `PYTHONPATH=$PWD pytest -q tests/backtest/test_phase3_service_api.py` → **8 passed**

