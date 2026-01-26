# Postgres Serving Split — Phase 3 (Backtest Run Store)

Phase 3 moves **backtest run-state** (queued/running/completed/failed + metadata) into Azure Postgres while keeping **artifacts in ADLS/Blob**.

**No run-history migration/backfill is included in Phase 3.** Existing ADLS/SQLite runs remain where they are; new runs are persisted to Postgres when enabled.

## What Phase 3 adds

- Postgres table for backtest runs (repo-owned migration):
  - `backtest.runs`
  - Migration: `deploy/sql/postgres/migrations/0004_backtest_runs.sql`
- Backtest service Postgres store implementation:
  - `api/service/postgres_run_store.py`
- Backtest API wiring:
  - `api/service/settings.py` adds `BACKTEST_RUN_STORE_MODE=postgres` and requires `POSTGRES_DSN`
  - `api/service/app.py` selects `PostgresRunStore` when configured

## Required configuration

- `BACKTEST_RUN_STORE_MODE=postgres`
- `POSTGRES_DSN` (secret) — DSN used by the backtest API to read/write run state.

Recommended DSN format:
- `postgresql://api_service:<password>@<server>.postgres.database.azure.com:5432/asset_allocation?sslmode=require`

Legacy (older role name; still supported in migrations/provisioning):
- `postgresql://backtest_service:<password>@<server>.postgres.database.azure.com:5432/asset_allocation?sslmode=require`

## Artifact storage (unchanged)

- Artifacts remain in ADLS/Blob.
- `BACKTEST_ADLS_RUNS_DIR` is still used as the default upload location when a submitted config does not set `output.adls_dir`.

## Apply migrations

Apply repo-owned migrations (includes Phase 3 table):

```powershell
pwsh deploy/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>" -UseDockerPsql
```

## Verify

1) Run the backtest API with `BACKTEST_RUN_STORE_MODE=postgres` and a valid `POSTGRES_DSN`.
2) Submit a backtest and confirm rows appear:

```sql
SELECT run_id, status, submitted_at, started_at, completed_at, run_name, error
FROM backtest.runs
ORDER BY submitted_at DESC
LIMIT 25;
```

## Rollback

- Set `BACKTEST_RUN_STORE_MODE=adls` (or `sqlite`) and redeploy.
- Artifacts remain in ADLS and are unaffected by the run-store backend.

