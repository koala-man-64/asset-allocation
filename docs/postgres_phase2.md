# Postgres Serving Split — Phase 2 (Signals Dual-Write)

Phase 2 enables **dual-write of derived ranking signals** into Azure Postgres while keeping **ADLS/Delta canonical**.

**No backfill/data migration is included in Phase 2**. Postgres will only contain signal months produced after enablement (until a later backfill phase is executed).

## What Phase 2 adds

- Postgres tables for signals (repo-owned migration):
  - `ranking.ranking_signal`
  - `ranking.composite_signal_daily`
  - `ranking.signal_sync_state`
  - Migration: `deploy/sql/postgres/migrations/0003_ranking_signals.sql`
- Dual-write from the existing ranking job signal materialization step:
  - `scripts/ranking/signals.py` calls `scripts/ranking/postgres_signals.py` after Delta writes succeed.
- Postgres driver dependency:
  - `psycopg==3.2.3`
  - `psycopg-binary==3.2.3`

## Required configuration

### Job env var

- `POSTGRES_DSN` (secret) — DSN used by the ranking job to write signals to Postgres.
  - In Azure Container Apps Job manifests this is already wired via `deploy/job_platinum_ranking.yaml`.

Recommended DSN format:
- `postgresql://ranking_writer:<password>@<server>.postgres.database.azure.com:5432/asset_allocation?sslmode=require`

### Optional feature flags

- `POSTGRES_SIGNALS_WRITE_REQUIRED` (default: `true`)
  - When `true`, a Postgres write failure fails the job (so ACA retries).
  - When `false`, Postgres write failures are logged but do not fail the job (Delta remains canonical).
- `POSTGRES_SIGNALS_VERIFY_COUNTS` (default: `false`)
  - When `true`, the writer verifies row counts after insertion for the given month and fails on mismatch.

## Apply migrations

Apply repo-owned migrations (includes Phase 2 tables):

```powershell
pwsh deploy/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>" -UseDockerPsql
```

## Verify dual-write

1) Run the ranking job (scheduled or manual) so it materializes at least one `year_month`.

2) Validate Postgres tables:

```sql
SELECT year_month, COUNT(*) AS rows
FROM ranking.ranking_signal
GROUP BY year_month
ORDER BY year_month DESC
LIMIT 12;

SELECT year_month, COUNT(*) AS rows
FROM ranking.composite_signal_daily
GROUP BY year_month
ORDER BY year_month DESC
LIMIT 12;

SELECT *
FROM ranking.signal_sync_state
ORDER BY synced_at DESC
LIMIT 25;
```

## Rollback

- Fastest: remove/unset `POSTGRES_DSN` (disables Postgres writes).
- Or set `POSTGRES_SIGNALS_WRITE_REQUIRED=false` (keeps best-effort writes but avoids job failures).

