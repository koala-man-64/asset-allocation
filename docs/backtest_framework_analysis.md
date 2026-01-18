# Backtest & Strategy Framework Analysis

## 1. Executive Summary

The Asset Allocation backtesting framework is a configuration-driven, event-based system designed to simulate trading strategies against historical data. It supports daily resolution simulations with features for portfolio sizing, constraint management, and realistic execution modeling (slippage, commissions). The system requires distinct "Bronze" or "Silver" data inputs (prices and signals) and produces comprehensive metrics and trade logs.

## 2. Architecture Overview

The framework follows a classic event-loop architecture where the `BacktestEngine` iterates through historical dates, coordinating interactions between the Strategy, Broker, Sizer, and Portfolio components.

```mermaid
graph TD
    Config[BacktestConfig] --> Engine[BacktestEngine]
    Data[Data Loader] --> Engine
    
    subgraph Event Loop
        Engine --> Broker[SimulatedBroker]
        Broker --> Portfolio[Portfolio State]
        Engine --> Strategy[Strategy]
        Strategy --> Sizer[Sizer]
        Sizer --> Constraints[Constraint Manager]
        Constraints --> Engine
    end
    
    Engine --> Reporter[Reporter]
    Reporter --> Artifacts[Results (DB/Files)]
```

## 3. Key Components

### 3.1 Backtest Engine (`engine.py`)
The orchestrator of the simulation.
- **Responsibilities**: 
  - Loads data via `load_backtest_inputs`.
  - Aligns price and signal data index.
  - Manages the main daily loop (`run` method).
  - Handles the sequence of Execution (Open) -> Strategy (Close) -> Sizing.
- **Key Logic**: It separates trade execution (happening at the *Open* of day T) from strategy decision-making (happening at the *Close* of day T-1).

### 3.2 Strategy (`strategy.py`)
The abstract base class for user logic.
- **Responsibilities**:
  - `on_bar(date, prices, portfolio, signals)`: Returns a `StrategyDecision` containing raw scores/signals.
  - `should_rebalance(date)`: Determines if logic should run (Daily, Monthly, etc.).
- **Extensibility**: Users subclass this to implement specific alphas (e.g., `MomentumStrategy`, `StaticUniverseStrategy`).

### 3.3 Broker (`broker.py`) & Portfolio (`portfolio.py`)
Handles state and simulated execution.
- **SimulatedBroker**:
  - `execute_target_weights`: Calculates the delta between current holdings and target weights.
  - applies transaction costs (slippage/commissions).
  - Updates cash and positions.
- **Portfolio**:
  - Tracks `cash` and `positions` (symbol -> quantity).
  - Calculates `total_equity`.

### 3.4 Sizer (`sizer.py`)
Converts Strategy scores into Portfolio weights.
- **Role**: Decouples "what to buy" (Strategy) from "how much to buy" (Sizing).
- **Implementations**: `EqualWeightSizer`, etc.

### 3.5 Data Access (`data_access/loader.py`)
Abstraction for fetching historical data.
- **Sources**: 
  - `local`: Parquet/CSV files (mostly for dev/test).
  - `ADLS`: Delta Tables from Azure Data Lake Storage (Silver layer).
- **Filtering**: Automatically filters data to the simulation's start/end dates and universe.

## 4. Execution Flow (The "Daily Loop")

For each trading day `T`:

1. **Trade Execution (Market Open)**:
   - The Engine checks for `pending_targets` generated on `T-1`.
   - `Broker.execute_target_weights` aligns the portfolio to these targets using `T`'s *Open Prices*.
   - **Note**: This assumes orders are placed 'Next Day Open'.

2. **Portfolio Snapshot**:
   - The engine captures the state (Positions + Cash) after execution.
   - Calculates equity using `T`'s *Close Prices* (Mark-to-Market).

3. **Strategy Logic (Market Close)**:
   - If `should_rebalance(T)` is true:
     - `Strategy.on_bar()` creates scores.
     - `Sizer.size()` converts scores to target weights used for `pending_targets` (to be executed at `T+1` Open).
   - Constraints (e.g., max leverage, position limits) are applied to the weights.

4. **Reporting**:
   - `Reporter` records daily equity, returns, drawdown, and turnover.

## 5. Data Flow & Configuration
- **Inputs**: Defined in YAML/JSON configs (`backtests/backtest_monthly.yaml`).
- **Data Model**: Pandas DataFrames.
  - **Prices**: Must contain `date`, `symbol`, `open`, `close`.
  - **Signals**: Optional operational data matched by date/symbol.
- **Outputs**:
  - `metrics.json`: Summary stats (Sharpe, CAR, etc.).
  - `equity.csv`: Daily equity curve.
  - `trades.csv`: List of all executions.
  - `runs.sqlite3`: Persistent metadata storage for the API.

## 6. Observations
- **Execution Model**: The "Decision at Close -> Execute at Next Open" model is robust and avoids lookahead bias for daily strategies.
- **Separation of Concerns**: Good separation between Alpha (Strategy), Portfolio Construction (Sizer), and Execution (Broker).
- **Cloud Native**: Designed to run as a containerized service reading from ADLS, suitable for the "Gold" layer of a medallion architecture.
