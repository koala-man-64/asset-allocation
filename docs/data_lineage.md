# Data Lineage (Bronze → Silver → Gold → Platinum)

This document describes how the data pipelines in `tasks/` flow through the Bronze/Silver/Gold/Platinum layers, how those layers map to storage paths, and which trading signals/strategies depend on which domains.

## Domains

- **Market**: OHLCV + derived market features (`tasks/market_data/*`)
- **Finance**: financial statements + ratios (`tasks/finance_data/*`)
- **Earnings**: earnings calendar + surprise metrics (`tasks/earnings_data/*`)
- **Price Target**: analyst targets + dispersion metrics (`tasks/price_target_data/*`)

## Storage Layers and Canonical Paths

### Bronze (raw landing)

- Container: `AZURE_CONTAINER_BRONZE`
- Job examples:
  - `tasks/market_data/bronze_market_data.py` writes `market-data/<ticker>.csv` and `market-data/whitelist.csv`
  - `tasks/finance_data/bronze_finance_data.py` writes `finance-data/*` and `finance-data/whitelist.csv`
  - `tasks/earnings_data/bronze_earnings_data.py` writes `earnings-data/*` and `earnings-data/whitelist.csv`
  - `tasks/price_target_data/bronze_price_target_data.py` writes `price-target-data/*` and `price-target-data/whitelist.csv`

### Silver (cleaned / standardized)

- Container: `AZURE_CONTAINER_SILVER`
- Layout mode:
  - `SILVER_LAYOUT_MODE=alpha26` (required): first-letter bucket Delta tables (`A..Z`).
- Bucket tables:
  - Market: `market-data/buckets/<A..Z>`
  - Earnings: `earnings-data/buckets/<A..Z>`
  - Price Target: `price-target-data/buckets/<A..Z>`
  - Finance: `finance-data/<balance_sheet|income_statement|cash_flow|valuation>/buckets/<A..Z>` (4x26)

### Gold (feature store)

- Container: `AZURE_CONTAINER_GOLD`
- Layout mode:
  - `GOLD_LAYOUT_MODE=alpha26` (required): first-letter bucket feature tables (`A..Z`).
- Feature engineering jobs write bucket tables:
  - Market: `market/buckets/<A..Z>`
  - Earnings: `earnings/buckets/<A..Z>`
  - Finance: `finance/buckets/<A..Z>`
  - Price target: `targets/buckets/<A..Z>`

### Platinum (reserved)

- Container: `AZURE_CONTAINER_PLATINUM`
- Reserved for curated/derived datasets that sit above Gold.
- No Platinum pipelines are currently defined in this repo.

## Downstream Impact

The System Status UI consumes `GET /api/system/lineage` to display domain impacts. This repo currently reports no trading-signal impacts.

## Refresh Behavior & Controls

### Silver ingestion
- Silver jobs skip unchanged Bronze blobs using watermarks stored in the `common` container:
  - Path: `system/watermarks/bronze_*` (JSON map keyed by blob name).
- Silver jobs also persist a per-job last-success checkpoint and pre-filter candidate blobs
  to items changed since that checkpoint:
  - Path: `system/watermarks/runs/silver_*_data.json`.
- Market/Earnings default to **latest-only** ingestion:
  - `SILVER_LATEST_ONLY` (global)
  - `SILVER_MARKET_LATEST_ONLY`, `SILVER_EARNINGS_LATEST_ONLY` (domain overrides)
- Silver precision policy (future writes only):
  - Rounding mode: `ROUND_HALF_UP`.
  - Price-valued fields are rounded to 2 decimals.
  - Explicitly derived silver fields are rounded to 4 decimals.
  - Domain policy:
    - Market: `open`, `high`, `low`, `close` at 2 decimals.
    - Finance valuation table (`quarterly_valuation_measures`): `market_cap`, `pe_ratio`, `forward_pe`, `ev_ebitda`, `ev_revenue`, `shares_outstanding` at 4 decimals.
    - Earnings: no explicit precision targets in current policy.
    - Price target: `tp_mean_est`, `tp_high_est`, `tp_low_est` at 2 decimals; `tp_std_dev_est` at 4 decimals.

### Gold feature engineering
- Gold jobs skip unchanged tickers using Silver commit watermarks:
  - Path: `system/watermarks/gold_*` (JSON map keyed by ticker).
- Watermark keys are bucket-based (`bucket::<A..Z>`).
- If watermarks are unavailable, Gold fails fast and logs an error.

### Alpha26 symbol indexes
- Bronze/Silver/Gold runs publish symbol index artifacts in `AZURE_CONTAINER_COMMON`:
  - Bronze: `system/bronze-index/<domain>/latest.parquet`
  - Silver: `system/silver-index/<domain>/latest.parquet`
  - Gold: `system/gold-index/<domain>/latest.parquet`
- These indexes are used for symbol discovery/monitoring in bucketed layouts.

### System health markers
- Successful Bronze/Silver/Gold jobs emit system-health markers under
  `system/health_markers/<layer>/<domain>.json` in `AZURE_CONTAINER_COMMON`;
  system-health probes require marker freshness and fail on marker miss/error.

