---
provider: Alpha Vantage
contract_name: alphavantage-http-query-interface
contract_version: "1.0"
last_reviewed: "2026-02-02"
source: "https://www.alphavantage.co/documentation/"
transport: https
primary_endpoint: "GET https://www.alphavantage.co/query"
auth:
  type: api_key
  location: query
  param: apikey
default_response_format: json
---

# Interface Contract — Alpha Vantage HTTP Query API (Agent-Readable)

## 1) Base Interface

### 1.1 Base URL
```text
https://www.alphavantage.co
```

### 1.2 Primary path
```text
GET /query
```

### 1.3 Core routing rule
Every request must include:
- `function=<FUNCTION_NAME>`
- `apikey=<YOUR_KEY>`

---

## 2) Global Request Parameters (Common Patterns)

> Not every function supports every parameter below; treat these as *common patterns*.

### 2.1 `datatype` (optional)
- `json` (default)
- `csv`

### 2.2 `outputsize` (optional; time series family)
- `compact` (default; most recent slice)
- `full` (extended history where supported)

### 2.3 `interval` (time series + indicators)
Common values:
- `1min`, `5min`, `15min`, `30min`, `60min`
- `daily`, `weekly`, `monthly`

> For analytics functions, interval values are often uppercase (`DAILY`, `WEEKLY`, `MONTHLY`). Send parameter names and values in the casing shown in docs for that function.

### 2.4 `month` (optional; intraday + indicators)
- Format: `YYYY-MM`
- Meaning: request a specific historical month (where supported)

---

## 3) Global Response Contract

### 3.1 Success payload types
Depending on `function`, successful JSON responses are typically one of:
- **Time series**: metadata + timestamp-indexed values (OHLC[V])
- **Fundamentals**: a single object (overview/profile) and/or arrays (statements)
- **News/Intelligence**: list of items (articles/transcripts/transactions)
- **Analytics**: computed metrics across symbols/windows
- **Economic/Commodities**: periodic series

### 3.2 CSV responses
If `datatype=csv`, the body is CSV (no JSON wrapper). Use a separate parsing path.

---

## 4) Error Handling (Client Responsibilities)

### 4.1 Transport-level
- HTTP status != 200 => error

### 4.2 Application-level (HTTP 200 but unusable)
Treat as error if:
- The expected top-level payload block for the requested function is missing
- The response is a message/notice instead of data (e.g., invalid params or throttling)

### 4.3 Retry policy
- Retry transient network/5xx failures with exponential backoff
- Avoid blind retries on quota/throttle responses; back off and/or stop

---

## 5) Recommended Normalized Models (Client-Side)

These are **client-side** shapes that make downstream tooling consistent across functions.

### 5.1 `NormalizedBar`
```json
{
  "ts": "ISO-8601 timestamp",
  "symbol": "requested symbol",
  "open": 0.0,
  "high": 0.0,
  "low": 0.0,
  "close": 0.0,
  "volume": 0.0
}
```

### 5.2 `NormalizedIndicatorPoint`
```json
{
  "ts": "ISO-8601 timestamp",
  "symbol": "requested symbol",
  "name": "SMA|EMA|...",
  "value": 0.0,
  "params": { "interval": "...", "time_period": 0, "series_type": "close" }
}
```

### 5.3 `NormalizedNewsItem`
```json
{
  "published_at": "ISO-8601 or provider format",
  "title": "string",
  "url": "string",
  "summary": "string",
  "source": "string",
  "tickers": ["string"],
  "sentiment": {
    "overall": 0.0,
    "by_ticker": [{ "ticker": "string", "score": 0.0 }]
  }
}
```

---

## 6) Function Catalog (Agent-Callable)

All functions below are invoked as:

```text
GET https://www.alphavantage.co/query?function=<FUNCTION>&...&apikey=<KEY>
```

---

# A) Core Stock Time Series

## A1) TIME_SERIES_INTRADAY
**Intent:** Equity intraday OHLCV bars.

**Required params**
- `function=TIME_SERIES_INTRADAY`
- `symbol` (e.g., `IBM`)
- `interval` (`1min|5min|15min|30min|60min`)
- `apikey`

**Optional params**
- `adjusted=true|false`
- `extended_hours=true|false`
- `month=YYYY-MM`
- `outputsize=compact|full`
- `datatype=json|csv`

**Expected output (JSON, conceptual)**
```json
{
  "Meta Data": { "2. Symbol": "IBM", "4. Interval": "5min" },
  "Time Series (<interval>)": {
    "YYYY-MM-DD HH:MM:SS": {
      "1. open": "...",
      "2. high": "...",
      "3. low": "...",
      "4. close": "...",
      "5. volume": "..."
    }
  }
}
```

**Normalize**
- Each timestamp row => `NormalizedBar`

---

## A2) TIME_SERIES_DAILY
**Intent:** Daily OHLCV bars.

**Required params**
- `function=TIME_SERIES_DAILY`
- `symbol`
- `apikey`

**Optional params**
- `outputsize=compact|full`
- `datatype=json|csv`

**Normalize**
- Each date row => `NormalizedBar`

---

# B) Options

## B1) REALTIME_OPTIONS
**Intent:** Realtime options chain and/or contract data.

**Required params**
- `function=REALTIME_OPTIONS`
- `symbol`
- `apikey`

**Optional params**
- `require_greeks=true|false`
- `contract=<provider contract id / descriptor>`
- `datatype=json|csv`

**Expected output**
- A list/collection of option contracts (plus greeks if requested)

---

# C) Alpha Intelligence

## C1) NEWS_SENTIMENT
**Intent:** News feed with sentiment metadata.

**Required params**
- `function=NEWS_SENTIMENT`
- `apikey`

**Optional params**
- `tickers=<comma-separated>`
- `topics=<comma-separated>`
- `time_from=YYYYMMDDTHHMM`
- `time_to=YYYYMMDDTHHMM`
- `sort=LATEST|EARLIEST|RELEVANCE`
- `limit=<int>`

**Normalize**
- Each item => `NormalizedNewsItem`

---

## C2) EARNINGS_CALL_TRANSCRIPT
**Intent:** Earnings call transcript retrieval.

**Required params**
- `function=EARNINGS_CALL_TRANSCRIPT`
- `symbol`
- `quarter=1|2|3|4`
- `apikey`

**Optional params**
- `year=YYYY`

---

## C3) TOP_GAINERS_LOSERS
**Intent:** Market movers list.

**Required params**
- `function=TOP_GAINERS_LOSERS`
- `apikey`

---

## C4) INSIDER_TRANSACTIONS
**Intent:** Insider transaction events for a symbol.

**Required params**
- `function=INSIDER_TRANSACTIONS`
- `symbol`
- `apikey`

---

# D) Analytics (Multi-Symbol Computations)

> Analytics functions commonly use uppercase parameter names. Send them as documented.

## D1) ANALYTICS_FIXED_WINDOW
**Intent:** Metrics over a fixed historical window across multiple symbols.

**Required params**
- `function=ANALYTICS_FIXED_WINDOW`
- `SYMBOLS=<comma-separated>`
- `RANGE=<start>` and `RANGE=<end>` (two RANGE params)
- `INTERVAL=<interval>` (often `DAILY|WEEKLY|MONTHLY`)
- `CALCULATIONS=<comma-separated metrics>`
- `apikey`

**Optional params**
- `OHLC=open|high|low|close` (commonly default `close`)

---

## D2) ANALYTICS_SLIDING_WINDOW
**Intent:** Metrics over rolling windows across a range.

**Required params**
- `function=ANALYTICS_SLIDING_WINDOW`
- `SYMBOLS=<comma-separated>`
- `RANGE=<...>`
- `INTERVAL=<...>`
- `WINDOW_SIZE=<int>`
- `CALCULATIONS=<metric or metrics>`
- `apikey`

**Optional params**
- `OHLC=open|high|low|close`

---

# E) Fundamentals

## E1) OVERVIEW
**Intent:** Company overview / key fundamentals.

**Required params**
- `function=OVERVIEW`
- `symbol`
- `apikey`

---

## E2) ETF_PROFILE
**Intent:** ETF profile & holdings.

**Required params**
- `function=ETF_PROFILE`
- `symbol`
- `apikey`

---

## E3) DIVIDENDS
**Intent:** Dividend events.

**Required params**
- `function=DIVIDENDS`
- `symbol`
- `apikey`

**Optional params**
- `datatype=json|csv`

---

## E4) SPLITS
**Intent:** Split events.

**Required params**
- `function=SPLITS`
- `symbol`
- `apikey`

**Optional params**
- `datatype=json|csv`

---

## E5) INCOME_STATEMENT
**Intent:** Income statement history.

**Required params**
- `function=INCOME_STATEMENT`
- `symbol`
- `apikey`

---

## E6) BALANCE_SHEET
**Intent:** Balance sheet history.

**Required params**
- `function=BALANCE_SHEET`
- `symbol`
- `apikey`

---

# F) Foreign Exchange (FX)

## F1) CURRENCY_EXCHANGE_RATE
**Intent:** Current exchange rate between two currencies.

**Required params**
- `function=CURRENCY_EXCHANGE_RATE`
- `from_currency=<code>` (e.g., `USD`)
- `to_currency=<code>` (e.g., `JPY`)
- `apikey`

---

## F2) FX_INTRADAY
**Intent:** Intraday FX OHLC bars.

**Required params**
- `function=FX_INTRADAY`
- `from_symbol=<code>`
- `to_symbol=<code>`
- `interval=<1min|5min|...|60min>`
- `apikey`

**Optional params**
- `outputsize=compact|full`
- `datatype=json|csv`

---

# G) Digital & Crypto

## G1) CRYPTO_INTRADAY
**Intent:** Intraday crypto OHLC + volume.

**Required params**
- `function=CRYPTO_INTRADAY`
- `symbol=<crypto>` (e.g., `BTC`)
- `market=<quote>` (e.g., `USD`)
- `interval=<1min|5min|...|60min>`
- `apikey`

**Optional params**
- `outputsize=compact|full`
- `datatype=json|csv`

---

## G2) DIGITAL_CURRENCY_DAILY
**Intent:** Daily crypto time series.

**Required params**
- `function=DIGITAL_CURRENCY_DAILY`
- `symbol`
- `market`
- `apikey`

**Optional params**
- `datatype=json|csv`

---

# H) Commodities

## H1) ALL_COMMODITIES
**Intent:** Global commodities price index series.

**Required params**
- `function=ALL_COMMODITIES`
- `apikey`

**Optional params**
- `interval=monthly|quarterly|annual`
- `datatype=json|csv`

---

# I) Economic Indicators

## I1) NONFARM_PAYROLL
**Intent:** US nonfarm payroll series.

**Required params**
- `function=NONFARM_PAYROLL`
- `apikey`

**Optional params**
- `datatype=json|csv`

---

# J) Technical Indicators

## J1) SMA
**Intent:** Simple Moving Average series.

**Required params**
- `function=SMA`
- `symbol`
- `interval=<1min..60min|daily|weekly|monthly>`
- `time_period=<int>`
- `series_type=open|high|low|close`
- `apikey`

**Optional params**
- `month=YYYY-MM`
- `datatype=json|csv`

**Normalize**
- Each point => `NormalizedIndicatorPoint` with `name="SMA"`

---

## J2) EMA
**Intent:** Exponential Moving Average series.

**Required params**
- `function=EMA`
- `symbol`
- `interval=<1min..60min|daily|weekly|monthly>`
- `time_period=<int>`
- `series_type=open|high|low|close`
- `apikey`

**Optional params**
- `month=YYYY-MM`
- `datatype=json|csv`

**Normalize**
- Each point => `NormalizedIndicatorPoint` with `name="EMA"`

---

## 7) Agent Execution Checklist (Per Call)

1. Select `function` based on the required dataset.
2. Build query parameters with exact names/casing for that function.
3. Include `apikey`.
4. Prefer `datatype=json` unless CSV is explicitly needed.
5. Send `GET` request to `/query`.
6. If HTTP != 200 => fail fast.
7. If JSON: verify expected payload blocks exist (function-specific).
8. Normalize to internal model.
9. Cache where it reduces quota consumption.

---

## 8) Copy/Paste Templates

### 8.1 Intraday equity bars
```text
https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=YOUR_KEY
```

### 8.2 Daily equity bars
```text
https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&outputsize=compact&apikey=YOUR_KEY
```

### 8.3 News sentiment for a ticker
```text
https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=IBM&sort=LATEST&limit=50&apikey=YOUR_KEY
```

### 8.4 SMA indicator
```text
https://www.alphavantage.co/query?function=SMA&symbol=IBM&interval=weekly&time_period=10&series_type=close&apikey=YOUR_KEY
```
