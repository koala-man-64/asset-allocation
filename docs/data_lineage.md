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
- Per-symbol Delta tables (examples):
  - Market: `core.pipeline.DataPaths.get_market_data_path()` (written by `tasks/market_data/silver_market_data.py`)
  - Finance: `core.pipeline.DataPaths.get_finance_path()` (written by `tasks/finance_data/silver_finance_data.py`)
  - Earnings: `core.pipeline.DataPaths.get_earnings_path()` (written by `tasks/earnings_data/silver_earnings_data.py`)
  - Price Target: `core.pipeline.DataPaths.get_price_target_path()` (written by `tasks/price_target_data/silver_price_target_data.py`)

### Gold (feature store)

- Container: `AZURE_CONTAINER_GOLD`
- Feature engineering jobs (examples):
  - Market features: `tasks/market_data/gold_market_data.py` → `market/<ticker>`
    - Includes technical features such as candlestick patterns, Heikin-Ashi (`ha_*`), and Ichimoku (`ichimoku_*`) columns.
  - Finance features: `tasks/finance_data/gold_finance_data.py` → `finance/<ticker>`
  - Earnings features: `tasks/earnings_data/gold_earnings_data.py` → `earnings/<ticker>`
  - Price target features: `tasks/price_target_data/gold_price_target_data.py` → `targets/<ticker>`

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
- If the common container is unavailable (e.g., local tests), Silver falls back to legacy freshness checks.
- Market/Earnings default to **latest-only** ingestion:
  - `SILVER_LATEST_ONLY` (global)
  - `SILVER_MARKET_LATEST_ONLY`, `SILVER_EARNINGS_LATEST_ONLY` (domain overrides)
- Optional backfill range filters:
  - `BACKFILL_START_DATE`, `BACKFILL_END_DATE` (YYYY-MM-DD)

### Gold feature engineering
- Gold jobs skip unchanged tickers using Silver commit watermarks:
  - Path: `system/watermarks/gold_*` (JSON map keyed by ticker).
- If watermarks are unavailable, Gold fails fast and logs an error.

### System health markers
- Successful Bronze/Silver/Gold jobs emit system-health markers under
  `system/health_markers/<layer>/<domain>.json` in `AZURE_CONTAINER_COMMON`;
  system-health probes require marker freshness and fail on marker miss/error.

