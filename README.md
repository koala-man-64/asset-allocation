# AssetAllocation

Python project for market/finance data pipelines, ranking signals, and a backtest framework.

## Quickstart

### Prerequisites
- Python 3.10 (matches Docker/CI) and `pip`

### Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
python3 -m pip install -e .
```

### Run tests
```bash
python3 -m pytest -q
```

## Backtesting

The backtest execution model is:
- Generate targets at **close(T)**
- Execute trades at **open(T+1)** (daily bars)

### CLI
```bash
python3 -m asset_allocation.backtest.cli -c backtests/platinum_vcp_breakout_long.yaml
```

Artifacts are written under `backtest_results/<RUN_ID>/` (configurable via `output.local_dir`) and include:
- `config.resolved.json`
- `trades.csv`
- `daily_positions.parquet`
- `metrics_timeseries.parquet`
- `metrics_rolling.parquet`
- `metrics.json`
- `constraint_hits.json`
- `returns_monthly.csv`, `returns_quarterly.csv`, `returns_yearly.csv`

### Python API
```python
from asset_allocation.backtest import BacktestConfig, run_backtest

cfg = BacktestConfig.from_yaml("backtests/platinum_vcp_breakout_long.yaml", strict=True)
result = run_backtest(cfg)
print(result.run_id, result.output_dir)
```

### Service API (FastAPI)
See `docs/backtest_service.md`. Locally:
```bash
uvicorn asset_allocation.backtest.service.app:app --reload
```

## Deployment

Azure deployment is driven by `.github/workflows/deploy.yml` and manifests under `deploy/`.

## Dependency lockfiles
- `requirements.lock.txt` is used by Docker builds for reproducible images.
- `requirements-dev.lock.txt` is used by CI for reproducible test installs.

## Docs
- `docs/backtesting_guide.md`
- `docs/backtest_framework_analysis.md`
- `docs/backtest_service.md`
