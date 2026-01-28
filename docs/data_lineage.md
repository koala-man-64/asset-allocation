# Data Lineage (Bronze → Silver → Gold → Platinum)

This document describes how the data pipelines in `tasks/` flow through the Bronze/Silver/Gold/Platinum layers, how those layers map to storage paths, and which trading signals/strategies depend on which domains.

## Domains

- **Market**: OHLCV + derived market features (`tasks/market_data/*`)
- **Finance**: financial statements + ratios (`tasks/finance_data/*`)
- **Earnings**: earnings calendar + surprise metrics (`tasks/earnings_data/*`)
- **Price Target**: analyst targets + dispersion metrics (`tasks/price_target_data/*`)
- **Rankings / Signals (Platinum)**: strategy ranks and daily signals (`tasks/ranking/*`)

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
- Materialized “by-date” Delta tables (used for freshness probes):
  - `tasks/*/materialize_silver_*_by_date.py` → `market-data-by-date`, `finance-data-by-date`, `earnings-data-by-date`, `price-target-data-by-date`

### Gold (feature store)

- Container: `AZURE_CONTAINER_GOLD`
- Feature engineering jobs (examples):
  - Market features: `tasks/market_data/gold_market_data.py` → `market/<ticker>` and `market_by_date`
  - Finance features: `tasks/finance_data/gold_finance_data.py` → `finance/<ticker>` and `finance_by_date`
  - Earnings features: `tasks/earnings_data/gold_earnings_data.py` → `earnings/<ticker>` and `earnings_by_date`
  - Price target features: `tasks/price_target_data/gold_price_target_data.py` → `targets/<ticker>` and `targets_by_date`

### Platinum (rankings + signals)

- Container: `AZURE_FOLDER_RANKING`
- Canonical Delta tables live under the `platinum/` prefix in the ranking container:
  - `core.data_contract.CANONICAL_RANKINGS_PATH` → `platinum/rankings`
  - `core.data_contract.CANONICAL_COMPOSITE_SIGNALS_PATH` → `platinum/signals/daily`
  - `core.data_contract.CANONICAL_RANKING_SIGNALS_PATH` → `platinum/signals/ranking_signals`
- Produced by:
  - `tasks/ranking/runner.py` (writes canonical rankings)
  - `tasks/ranking/signals.py` (materializes canonical signals)
- Replicated to Postgres (for APIs/UI):
  - Writer: `tasks/ranking/postgres_signals.py`
  - Tables: `deploy/sql/postgres/migrations/0003_ranking_signals.sql` (`ranking.ranking_signal`, `ranking.composite_signal_daily`)

## Downstream Impact (Which signals depend on which domains)

Ranking strategies declare non-market dependencies via `sources_used` in `tasks/ranking/strategies.py`:

- **Market** is implicit for all strategies (always required)
- `Value_PE` depends on: `finance`
- `BrokenGrowthWithImprovingInternals` depends on: `finance`, `price_targets`

Operationally, this means:

- If **Market** is stale → all ranking-based signals should be considered impacted.
- If **Finance** is stale → at least `Value_PE` and `BrokenGrowthWithImprovingInternals` are impacted.
- If **Price Target** is stale → `BrokenGrowthWithImprovingInternals` is impacted.
- If **Earnings** is stale → currently no strategy declares it in `sources_used`, but it is available for future strategies.

The System Status UI consumes `GET /system/lineage` to display this impact per domain and correlate it with current `BUY` signals from `GET /signals`.

