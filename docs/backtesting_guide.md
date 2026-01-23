# Backtesting User Guide

This guide explains how to configure, run, and evaluate backtests using the Asset Allocation Engine.

## 1. Prerequisites

Ensure your environment is set up:
1.  **Python Environment**: Install dependencies via `pip install -r requirements.txt`.
2.  **Azure Credentials**: Ensure you have `AZURE_STORAGE_ACCOUNT_NAME` set if accessing ADLS data.
    *   Default: `assetallocstorage001`
    *   Login: `az login` (if running locally against ADLS).
3.  **Local Data (Optional)**: If testing without cloud access, set `BACKTEST_ALLOW_LOCAL_DATA="true"`.

## 2. Configuration (YAML)

Backtests are defined in YAML files. Store these in the `backtests/` directory (e.g., `backtests/my_strategy.yaml`).

### Structure
```yaml
run_name: "MY-RUN-001"
start_date: "2024-01-01"
end_date: "2024-12-31"
initial_cash: 100000.0

universe:
  symbols: ["AAPL", "MSFT"]  # Optional, can be dynamic based on strategy

data:
  price_source: "ADLS"       # or "local"
  price_path: "silver/market-data/{symbol}"
  # Optional: Delta signals table (container/path) used by signal-driven strategies.
  # Ranking composite signals (canonical): "ranking-data/platinum/signals/daily"
  signal_path: "ranking-data/platinum/signals/daily"
  frequency: "Daily"

strategy:
  class: StaticUniverseStrategy # or BuyAndHoldStrategy, TopNSignalStrategy
  parameters:
    symbols: ["AAPL", "MSFT"]
    rebalance: "monthly"    # Options: daily, weekly, monthly, quarterly, annually, or integer (days)

sizing:
  class: EqualWeightSizer
  parameters:
    max_positions: 10

# Optional: Kelly sizing (raw weights are constrained by the constraint pipeline)
# sizing:
#   class: KellySizer
#   parameters:
#     kelly_fraction: 0.5     # 1.0 = full Kelly; 0.0 = allocate nothing
#     lookback_days: 20       # trailing close-to-close returns window for covariance
#     mu_scale: 0.01          # expected DAILY return per score unit (scores -> mu)

broker:
  commission: 0.0005        # 5 basis points (0.05%)
  slippage_bps: 2.0         # 2 basis points
  
output:
  local_dir: "./backtest_results"
  save_trades: true
  save_daily_metrics: true
```

### ConfiguredStrategy (pipeline-based)
You can also define strategies using a single configurable pipeline:

- Example configs: `backtests/example_configured_topn.yaml`, `backtests/example_configured_vcp_breakout_long.yaml`
- Programmatic wrappers (for tests/tools): `asset_allocation/backtest/configured_strategy/legacy_migrations.py`

```yaml
strategy:
  type: configured
  rebalance:
    freq: every_n_days
    every_n_days: 5
  universe:
    source: signals
    filters: []
    require_columns: ["symbol", "date", "open", "close"]
  signals:
    provider: platinum_signals_daily
    columns: ["composite_percentile"]
  scoring:
    type: column
    column: composite_percentile
    fillna: drop
  selection:
    type: topn
    topn:
      n: 10
      side: long
      min_score: null
      higher_is_better: true
  holding_policy:
    type: replace_all
    replace_all:
      exit_if_not_selected: true
  exits:
    precedence: exit_over_scale
    rules: []
  postprocess:
    steps: []
  debug:
    record_intermediates: false
    record_reasons: false
```

### ConfiguredStrategy debug artifacts
When `strategy.type: configured` and either `strategy.debug.record_intermediates` or `strategy.debug.record_reasons` is enabled, the runner writes per-decision-date artifacts under:

- `backtest_results/<RUN_ID>/strategy_debug/`

Files are only emitted on dates where the strategy returns a decision (rebalance days, or non-rebalance days where exits/scales changed):

- `<YYYY-MM-DD>_universe.csv`, `<YYYY-MM-DD>_raw_scores.csv`, `<YYYY-MM-DD>_selected.csv`, `<YYYY-MM-DD>_held.csv`
- `<YYYY-MM-DD>_scores.csv`, `<YYYY-MM-DD>_scales.csv`
- `<YYYY-MM-DD>_exits.csv` (when `record_reasons: true`)

### CompositeStrategy (multi-leg blends)
Composite strategies let you blend multiple independent strategy “legs” into a single portfolio target without creating bespoke classes per blend.

- Example config: `backtests/example_composite_50_50.yaml`

```yaml
strategy:
  type: composite
  blend:
    method: weighted_sum
    normalize_final: gross
    target_gross: 1.0
    allow_overlap: true
  legs:
    - name: leg_a
      weight: 0.5
      strategy: { class: TopNSignalStrategy, parameters: { signal_column: "momentum_percentile", top_n: 10 } }
    - name: leg_b
      weight: 0.5
      strategy: { class: TopNSignalStrategy, parameters: { signal_column: "value_percentile", top_n: 10 } }
```

Composite run artifacts (written for each decision date where the composite emits a decision):
- `backtest_results/<RUN_ID>/legs/<LEG_NAME>/weights.csv` (per-leg target weights, pre-constraints)
- `backtest_results/<RUN_ID>/blend/blended_pre_constraints.csv`
- `backtest_results/<RUN_ID>/blend/blended_post_constraints.csv`

### Key Parameters
*   **`data.signal_path`**: Optional signals input table (Delta `container/path` or local file path when `price_source: local`).
    *   If using ranking-derived signals, set `signal_path: "ranking-data/platinum/signals/daily"` and use `signal_column: "composite_percentile"` in your strategy.
*   **`strategy.parameters.rebalance`**: Controls trading frequency.
    *   `daily`: Checks every bar (default).
    *   `weekly`: First trading day of the week.
    *   `monthly`: First trading day of the month.
    *   `quarterly` / `annually`: First day of quarter/year.
    *   `N` (int): Every N days.
*   **`broker.commission`**: Transaction cost as a decimal rate (e.g., `0.0005` = 5 bps).

## 3. Running a Backtest

Run the backtest using the Python module CLI from the project root.

**Basic Command:**
```bash
python -m backtest.cli --config backtests/my_strategy.yaml
```

**Custom Run ID (Overrides YAML):**
```bash
python -m backtest.cli --config backtests/my_strategy.yaml --run-id "TEST-RUN-v2"
```

**Output Directory (Overrides YAML):**
```bash
python -m backtest.cli --config backtests/my_strategy.yaml --output-dir ./my_results
```

## 4. Evaluating Results

Results are saved to `backtest_results/<RUN_ID>/`.

### Key Files
*   **`summary.json`**: High-level metrics.
    *   `total_return`: Absolute return (e.g., 0.25).
    *   `sharpe_ratio`: Risk-adjusted return.
    *   `trades`: Total trade count (low count = low turnover).
    *   `max_drawdown`: Worst peak-to-trough decline.
*   **`daily_metrics.csv`**: Time series of account value.
    *   Columns: `date`, `portfolio_value`, `daily_return`, `drawdown`, `commission`.
    *   Use this for plotting the equity curve.
*   **`trades.csv`**: Log of all executed trades.
    *   Columns: `execution_date`, `symbol`, `quantity`, `price`, `commission`, `slippage_cost`.
*   **`monthly_returns.csv`**: Pivot table of returns by month/year.

### Comparison
To compare multiple runs (e.g., impact of rebalancing frequency), check `summary.json` for:
1.  **Trades**: Did transaction count drop as expected?
2.  **Total Commission**: How much was saved?
3.  **Sharpe Ratio**: Did the structural change degrade risk-adjusted performance?
