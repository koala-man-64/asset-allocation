# Alpha Vantage Market Ingestion (Phase 1)

Phase 1 switches **market daily bars** ingestion to **Alpha Vantage**. The Bronze market job fetches daily data from Alpha Vantage and writes to `market-data/{SYMBOL}.csv`.

## What Changed
- `tasks/market_data/bronze_market_data.py` no longer uses the legacy browser-based ingestion.
- Data source: Alpha Vantage `TIME_SERIES_DAILY` (CSV) per symbol.

## Required Environment Variables
Tip: run `pwsh scripts/setup-env.ps1` to generate a complete `.env` from `.env.template`.

- `ALPHA_VANTAGE_API_KEY` (required)
- `ALPHA_VANTAGE_RATE_LIMIT_PER_MIN` (default: `300`)
- `ALPHA_VANTAGE_TIMEOUT_SECONDS` (default: `15`)
- `ALPHA_VANTAGE_MAX_WORKERS` (default: `32`)

Azure storage variables are still required for the job to write Bronze:
- `AZURE_STORAGE_CONNECTION_STRING` (or `AZURE_STORAGE_ACCOUNT_NAME` + identity)
- `AZURE_CONTAINER_BRONZE` (default: `bronze`)

## Run (Local)
```bash
python -m tasks.market_data.bronze_market_data
```

## Output
- Bronze blobs: `market-data/{SYMBOL}.csv`
- Canonical CSV schema:
  - `Date,Open,High,Low,Close,Volume`

## Resumability / Freshness
The job skips symbols whose existing Bronze blob `last_modified` is already “today” in UTC. Re-running the job on the same day should be mostly skips.

## Troubleshooting
- **Lots of throttling failures**
  - Reduce `ALPHA_VANTAGE_MAX_WORKERS` (network bursts can still amplify throttle likelihood even with a limiter).
  - Confirm `ALPHA_VANTAGE_RATE_LIMIT_PER_MIN` matches your current tier (300 rpm assumed).
- **Many symbols blacklisted**
  - Alpha Vantage symbol coverage can differ from other data providers. Inspect `market-data/blacklist.csv` in Bronze and consider symbol mapping rules as a follow-up.

## Rollback
Redeploy the previous container image (prior to the Phase 1 change) to restore the prior market ingestion implementation.
