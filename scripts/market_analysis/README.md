# Market Analysis - Design & Operational Guide

## 1. Executive Overview
**Product**: Market Analysis Tool Suite
**Primary User**: Quantitative Analyst / Developer
**Goal**: Automate the retrieval of historical stock data from Yahoo Finance and calculate technical analysis indicators for strategy backtesting.
**Principles**:
- **Automated**: Minimizes manual data downloading via browser automation.
- **Robust**: Handles network flakiness, auth persistence, and rate limiting.
- **Local-First**: Caches data to CSV for fast subsequent access and offline analysis.

---

## 2. Information Architecture

### File Structure & Logic
- **Orchestrator**: `market_analysis_runner.py`  
  *Entry point. Manages the asyncio loop and Playwright browser lifecycle.*
- **Core Library**: `market_analysis_lib.py`  
  *Business logic for fetching symbols, downloading data, and orchestrating analysis.*
- **Math Layer**: `ta_lib.py`  
  *Pure functions for Technical Analysis (RSI, MACD, Bollinger Bands, Ichimoku).*
- **Configuration**: `config.py`  
  *Static configuration, manual ticker additions, and column pruning rules.*
- **Data Integrations**: `nasdaqdatalink` (API), `playwright` (Web Scraping).

### Data Flow Architecture
1.  **Input Sources**: 
    - NASDAQ Data Link (Active Ticker List)
    - `config.py` (Manual Overrides)
    - `blacklist.csv` (Exclusions)
2.  **Ingestion Process**:
    - **Check Cache**: Looks in `.../Yahoo/Price Data/` for recent files.
    - **Fetch**: If stale (>4h) or missing, launches browser to Yahoo Finance.
    - **Download**: Triggers CSV download for max history (10y+).
3.  **Processing**:
    - **Clean**: Removes 'Adj Close', standardizes dates, casts floats.
    - **Enrich**: (On Demand) Adds TA indicators via `ta_lib`.
4.  **Storage**:
    - Raw CSVs per ticker (`AAPL.csv`).
    - Aggregated CSV (`get_historical_data_output.csv`).

---

## 3. Key User Flows

### Flow A: Data Refresh (Primary Operational Flow)
**Actor**: Operator (running `market_analysis_runner.py`)

1.  **Initialization**:
    - Script launches a **visible** Chrome instance (`headless=False`).
    - Loads session cookies (`pw_cookies.json`).
    - Verifies Yahoo Login (auto-logs in if needed).
2.  **Discovery**:
    - Retreives active ticker list from NASDAQ + Config.
    - Filters out blacklisted symbols.
3.  **Ingestion Loop** (Async & Parallel):
    - **Step 1**: Check local file age.
    - **Step 2 (If Stale)**: 
        - Navigate to Yahoo download endpoint.
        - **Wait** for download to complete.
        - **Move** file from Downloads folder to Project Data folder.
        - **Clean** data (Dedup, Sort).
    - **Step 3**: Save to disk.
4.  **Completion**:
    - Terminal prints summary.
    - Browser closes.

### Flow B: Technical Analysis Calculation
**Actor**: Developer / Backtest Script

1.  **Call**: `malib.perform_technical_analysis(ticker)`
2.  **Computation**:
    - Loads local CSV.
    - **Core Indicators**: MACD, RSI, Bollinger Bands, Stochastics, OBV, ADL.
    - **Advanced**: Ichimoku Cloud, Jensen's Alpha (vs SPY).
    - **Normalization**: Z-Scoring and percentage diffs for ML readiness.
3.  **Result**: Returns enriched DataFrame.

---

## 4. UI/CLI Specification

### Console UX (Terminal)
The CLI serves as the primary status monitor.

| State | Output Pattern | Meaning |
| :--- | :--- | :--- |
| **Start** | `Retrieving historical data...` | Process begun. |
| **Cache Hit** | `✅ Using cached historical data ({Date})` | Skipped download, data is fresh. |
| **Download** | `♻️ Cache missing or stale → downloading...` | Active web request in progress. |
| **Success** | `💾 Wrote fresh data to {path}` | Data persisted successfully. |
| **Error** | `[Error] symbol={Sym}: {Msg}` | Non-blocking failure (e.g., 404). |
| **Wait** | `Sleeping for X seconds...` | Rate limit handling active. |

### Browser "Headless" UX
*Note: Currently runs `headless=False` for reliability.*
- **Visual**: User sees the browser open and navigate.
- **Intervention**: User interaction is technically possible but not expected unless solving a CAPTCHA loop.

---

## 5. Component Inventory

### Scripts & Modules
| Component | Responsibility | Dependencies |
| :--- | :--- | :--- |
| **`main_async`** | Event Loop & Browser Management | `playwright`, `asyncio` |
| **`get_symbols`** | list generation, blacklist filtering | `nasdaqdatalink` |
| **`refresh_stock_data_async`** | Async fetching pipeline | `playwright` |
| **`add_ta` / `perform_technical_analysis`** | Feature Engineering | `ta`, `ta_lib` |

### Key Library Functions (`ta_lib`)
- **Trend**: `calculate_macd`, `ichimoku_score`, `calculate_sma`
- **Momentum**: `calculate_rsi`, `calculate_stochastic_oscillator`, `calculate_cci`
- **Volatility**: `calculate_bollinger_bands`, `calculate_atr`
- **Volume**: `calculate_obv`, `calculate_adl`

---

## 6. Configuration Guide
Modify `config.py` to adjust the universe.

**Adding Manual Tickers**:
Add to the `TICKERS_TO_ADD` list:
```python
{
    'Symbol': 'NVDA',
    'Description': 'NVIDIA Corp',
    'Sector': 'Technology',
    'Industry': 'Semiconductors'
}
```

**Removing Indicators**:
Add column names to `COLUMNS_TO_REMOVE_TA` to exclude them from the final dataset.

---

## 7. Operational Notes & Handoffs
- **Rate Limiting**: The script includes `go_to_sleep()` logic to prevent Yahoo IP bans (429 errors).
- **Cookies**: `pw_cookies.json` is critical for seamless access. If login fails repeatedly, delete this file to force a fresh login.
- **Blacklist**: Symbols that fail repeatedly (404s) are auto-added to `blacklist.csv` to speed up future runs.
