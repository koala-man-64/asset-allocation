# Gold Postgres Sync

Gold Delta remains the source of truth. The gold jobs now replicate successful bucket outputs into Postgres `gold` tables after each Delta write and before bucket watermarks advance.

## Objects

- Migrations:
  - `[deploy/sql/postgres/migrations/0019_gold_postgres_sync.sql](/mnt/c/Users/rdpro/Projects/AssetAllocation/deploy/sql/postgres/migrations/0019_gold_postgres_sync.sql)`
  - `[deploy/sql/postgres/migrations/0024_add_gold_earnings_calendar_columns.sql](/mnt/c/Users/rdpro/Projects/AssetAllocation/deploy/sql/postgres/migrations/0024_add_gold_earnings_calendar_columns.sql)`
  - `[deploy/sql/postgres/migrations/0027_add_gold_market_structure_features.sql](/mnt/c/Users/rdpro/Projects/AssetAllocation/deploy/sql/postgres/migrations/0027_add_gold_market_structure_features.sql)`
- Shared sync helper: `[tasks/common/postgres_gold_sync.py](/mnt/c/Users/rdpro/Projects/AssetAllocation/tasks/common/postgres_gold_sync.py)`
- Serving tables:
  - `gold.market_data`
  - `gold.finance_data`
  - `gold.earnings_data`
  - `gold.price_target_data`
- Control table: `core.gold_sync_state`
- By-date views:
  - `gold.market_data_by_date`
  - `gold.finance_data_by_date`
  - `gold.earnings_data_by_date`
  - `gold.price_target_data_by_date`

## Runtime Rules

- If `POSTGRES_DSN` is not set, the gold jobs keep their prior Delta-only behavior.
- If `POSTGRES_DSN` is set, a bucket is skipped only when both conditions are true:
  - the Delta watermark is current for the latest silver commit
  - `core.gold_sync_state` shows a successful Postgres sync for that same bucket and source commit
- On a changed bucket, the job:
  - overwrites the Delta bucket
  - deletes Postgres rows for all symbols previously or currently assigned to that bucket
  - bulk inserts the current bucket rows into the matching Postgres table
  - upserts `core.gold_sync_state`
  - advances the bucket watermark only after Postgres sync succeeds

## Bootstrap

Run Postgres migrations first, clear the Gold Delta layer, then rerun the gold jobs:

```powershell
pwsh ./scripts/apply_postgres_migrations.ps1
```

The first successful run seeds `core.gold_sync_state`. After that, unchanged buckets resume normal incremental skipping.

`gold.earnings_data` now includes future-aware calendar columns in addition to the historical surprise metrics:
- `next_earnings_date`
- `days_until_next_earnings`
- `next_earnings_estimate`
- `next_earnings_time_of_day`
- `next_earnings_fiscal_date_ending`
- `has_upcoming_earnings`
- `is_scheduled_earnings_day`

`gold.market_data` now also includes market-structure features derived from daily OHLCV history:
- Donchian channel highs/lows, ATR-normalized distance, and breakout flags for 20-day and 55-day windows
- Confirmed-pivot nearest support/resistance zone scalars (`sr_*`)
- Fibonacci retracement levels and active-swing context (`fib_*`)

## Rebuild / Recovery

- Full rebuild: apply migrations, clear the Gold Delta layer, rerun the gold jobs.
- Single-domain rebuild: clear the matching Gold bucket path, rerun the matching gold job.
- If a job wrote Delta but failed Postgres sync, rerun the same job. Watermarks stay blocked, so the bucket will be retried.

## Verification

Check sync status:

```sql
SELECT domain, bucket, status, source_commit, row_count, symbol_count, synced_at
FROM core.gold_sync_state
ORDER BY domain, bucket;
```

Check serving-table access patterns:

```sql
SELECT *
FROM gold.market_data
WHERE symbol = 'AAPL'
ORDER BY date DESC
LIMIT 20;

SELECT *
FROM gold.market_data_by_date
WHERE date = DATE '2026-01-02'
ORDER BY symbol;
```
