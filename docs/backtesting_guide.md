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

### Key Parameters
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
python -m asset_allocation.backtest.cli --config backtests/my_strategy.yaml
```

**Custom Run ID (Overrides YAML):**
```bash
python -m asset_allocation.backtest.cli --config backtests/my_strategy.yaml --run-id "TEST-RUN-v2"
```

**Output Directory (Overrides YAML):**
```bash
python -m asset_allocation.backtest.cli --config backtests/my_strategy.yaml --output-dir ./my_results
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
