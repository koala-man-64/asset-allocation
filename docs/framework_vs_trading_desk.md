# Backtest Framework vs. Real Trading Desk

This document compares the `AssetAllocation` backtesting framework components to the roles and systems found on a professional trading desk.

## 1. Role Mapping Matrix

| Framework Component | Real-World Trading Desk Role | Description & Comparison |
|---------------------|------------------------------|--------------------------|
| **`Strategy`** Class | **Portfolio Manager (PM) / Quant Researcher** | **The Brain.** <br>In reality, PMs generate "alphas" or investment ideas. In the code, `on_bar()` represents the PM waking up, looking at the day's close, and deciding "I like Apple, I dislike Microsoft." |
| **`Sizer`** Class | **Portfolio Construction / Risk Analyst** | **The Allocator.**<br>A PM might say "Buy Apple", but the Risk Analyst says "You can only hold 5% max." The `Sizer` takes raw signals and converts them into specific position weighting, balancing conviction vs. diversification. |
| **`Constraints`** | **Risk Manager / Compliance** | **The "No" Button.**<br>Hard limits enforced by the firm (e.g., "Max 150% Gross Exposure", "No more than 20% in Tech"). In the code, `apply()` rejects or trims orders that violate these rules before they reach execution. |
| **`BacktestEngine`** | **Order Management System (OMS)** | **The Nervous System.**<br>Orchestrates the flow. It takes the "Target Portfolio" from the PM/Risk layers and calculates the *difference* (trades) needed to get there from the current holdings. |
| **`Broker`** Class | **Execution Trader / Algo** | **The Hands.**<br>Actually goes to the market to buy/sell. <br>**Key Difference:** The framework assumes "Simulated Execution" (instant fill at next Open with static slippage). A real desk has "Working Orders," managing limit orders, dark pools, and varying liquidity throughout the day. |
| **`Portfolio`** Class | **Fund Administrator / Accounting** | **The Scorekeeper.**<br>Tracks official NAV (Net Asset Value), cash balances, and positions. In reality, this is often a "Shadow" system internal to the desk, reconciled daily with the external Prime Broker. |
| **`Reporter`** | **Investor Relations / Performance Analyst** | **The Report Card.**<br>Calculates Sharpe Ratio, Drawdowns, and monthly returns to explain performance to LPs (Limited Partners) or the Head of Desk. |
| **`Loader`** | **Data Engineering** | **The Feeder.**<br>Ensures clean, point-in-time data (Prices, Corporate Actions) reaches the engine. Essential for avoiding "Garbage In, Garbage Out." |

## 2. Execution Flow Comparison

### In the Framework (Event Loop)
1.  **Day T-1 Close:** `Strategy` calculates Signal. `Sizer` sets Target Weights.
2.  **Day T Open:** `Broker` calculates Shares needed = (Target - Current) / Price.
3.  **Day T Open:** `Broker` "Fills" entire order at `Open Price * (1 + Slippage)`.

### On a Real Desk
1.  **Day T-1 Close:** PM runs models, generates "Target Portfolio" for tomorrow.
2.  **Day T Pre-Market:** OMS stages orders. Risk checks applied.
3.  **Day T Open/Intraday:** Execution Traders (or VWAP/TWAP Algos) work the orders.
    *   *Reality:* You might not get filled. You might incur huge market impact cost. You might partial fill.
    *   *Framework:* Assumes 100% liquidity and constant slippage.

## 3. Critical Differences & Simplifications

1.  **Liquidity & Market Impact**: 
    *   The framework assumes you can buy any amount of stock at the Open price (plus a small fee).
    *   **Reality**: Buying $10M of a small cap stock at the open would send the price skyrocketing (Market Impact). You wouldn't get the "Open" price; you'd get a much worse average price.

2.  **Execution Timing**:
    *   The framework is "Next Day Open" execution.
    *   **Reality**: Desks trade VWAP (Volume Weighted Average Price) over the day, MOC (Market on Close), or limit orders. The frameworks "Open" assumption is a conservative proxy for VWAP execution in liquid markets.

3.  **Operational Friction**:
    *   The framework has perfect accounting.
    *   **Reality**: Trade breaks, failed settles, dividend withholding taxes, corporate action messiness (spin-offs, mergers) are massive headaches for the Back Office.

## 4. Verdict

The framework provides a **high-fidelity "Alpha" simulation** but a **medium-fidelity "Execution" simulation**. 

It is excellent for testing **"Is this a good investment idea?"** (Strategy/Sizer focus). 
It is less suited for testing **"Can I trade this size in this specific market microstructure?"** (High, frequency trading or massive scale execution).
