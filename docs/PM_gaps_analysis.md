# Portfolio Manager Gap Analysis

A professional Portfolio Manager (PM) performs several critical functions that are currently simplified or missing in the `AssetAllocation` framework.

## 1. Portfolio Construction (The "Optimizer" Gap)

The current framework uses heuristic-based sizing (e.g., `EqualWeightSizer`). Professional desks typically use **Mean-Variance Optimization (MVO)** or **Robust Optimization**.

*   **Missing Capability:** Utility Function Maximization.
    *   *Real World:* `Maximize: w'µ - λ(w'Σw) - TransactionCosts(Δw)`
    *   *Framework:* "Rank by score, taketop N, give them 1/N weight."
*   **Implication:** The framework cannot mathematically balance the trade-off between expected return (Alpha) and risk (Covariance). It cannot naturally handle "substitutability" (e.g., "Google is expensive to trade, so buy Microsoft instead because they are highly correlated").

## 2. Factor Risk Models (The "Attribution" Gap)

The `Reporter` calculates aggregate risk (Volatility, Drawdown). It lacks **Factor Attribution**.

*   **Missing Capability:** Factor Decomposition.
    *   *Real World:* Risk is decomposed into `Market`, `Sector`, `Style` (Value, Momentum, Size), and `Idiosyncratic` components.
    *   *Questions PMs ask:* "Am I making money because I'm a stock picker, or just because I'm long Tech?" "Is my portfolio unintentionally short Interest Rates?"
*   **Implication:** Users can't see *why* a strategy works or fails structurally.

## 3. Transaction Cost Optimization (The "Implementation Shortfall" Gap)

The framework executes trades *after* sizing. Real PMs size *knowing* the cost.

*   **Missing Capability:** Cost-Aware Optimization.
    *   *Real World:* "I want to buy 5% more NVDA, but the spread is wide and impact is high. I will only buy 2%."
    *   *Framework:* "Target is 5%. Broker, go buy 5%." (Broker pays the generic slippage/commission).
*   **Implication:** It likely overestimates turnover and returns for strategies trading illiquid assets.

## 4. Alpha Blending & Regime Switching

*   **Missing Capability:** Dynamic Signal Combination.
    *   *Real World:* PMs blend multiple signals: `0.3 * Momentum + 0.4 * Reversal + 0.3 * Sentiment`. These weights might change based on market regimes (Volatile vs. Calm).
    *   *Framework:* Single `Strategy` class implies a monolithic decision logic. You'd have to code the blending *inside* the strategy manually.

## 5. Operational Alpha (Corporate Actions & Cash Management)

*   **Missing Capability:** Advanced Corporate Action Handling & Cash Sweeps.
    *   *Real World:* "I need to recall stock for voting." "I need to elect cash or stock for this merger." "I need to sweep idle cash specifically into T-Bills."
    *   *Framework:* Assumes price adjustments handle splits/dividends implicitly. Idle cash presumably sits at 0% interest (unless custom coded).

## Summary Table

| Feature | `AssetAllocation` Framework | Professional PM Desk |
| :--- | :--- | :--- |
| **Sizing** | Heuristic (Equal / Static) | Convex Optimization (Mean-Variance) |
| **Risk View** | Aggregate (Vol, DD) | Factor (Barra/Axioma Style/Sector) |
| **Costing** | Post-Decision Simulation | In-Decision Optimization |
| **Exec. Strategy**| Market on Open | VWAP, TWAP, POV, Dark Pools |
| **Alpha Mix** | Single Logic Block | Dynamic Multi-Signal Blending |
