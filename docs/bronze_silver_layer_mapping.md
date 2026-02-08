# Bronze & Silver Layer Audit Report

## 1. Execution Summary
This report details the audit of all Bronze and Silver layer jobs in the data pipeline. The objective was to map source columns to destination columns and identify any potential issues for the upcoming data source migration.

**Key Finding:** The job labeled `bronze_price_target_data` (and downstream `silver_price_target_data`) appears to differ significantly from its name. The schema suggests it is storing **News Sentiment** data rather than Price Targets.

## 2. Architectural Alignment Matrix
The following table maps the data flow from source (API) to Bronze and Silver layers.

| Domain | Bronze Job | Silver Job | Source API (Alpha Vantage) | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Market Data** | `bronze_market_data` | `silver_market_data` | `TIME_SERIES_DAILY_ADJUSTED` | Verified |
| **Finance Data** | `bronze_finance_data` | `silver_finance_data` | `OVERVIEW` | Verified |
| **Earnings Data** | `bronze_earnings_data` | `silver_earnings_data` | `EARNINGS` | Verified |
| **Price Target** | `bronze_price_target_data` | `silver_price_target_data` | `NEWS_SENTIMENT` (Inferred) | **Misnamed?** |

## 3. Detailed Column Mapping

### 3.1 Market Data Domain

#### Bronze: `bronze_market_data`
**Source:** Alpha Vantage `TIME_SERIES_DAILY_ADJUSTED`

| Column | Type | Notes |
| :--- | :--- | :--- |
| `symbol` | `VARCHAR(10)` | Input parameter |
| `date` | `DATE` | API Key (dict key) |
| `open` | `FLOAT` | API Key: `1. open` |
| `high` | `FLOAT` | API Key: `2. high` |
| `low` | `FLOAT` | API Key: `3. low` |
| `close` | `FLOAT` | API Key: `4. close` |
| `adjusted_close` | `FLOAT` | API Key: `5. adjusted close` |
| `volume` | `BIGINT` | API Key: `6. volume` |
| `dividend_amount`| `FLOAT` | API Key: `7. dividend amount` |
| `split_coefficient`| `FLOAT` | API Key: `8. split coefficient` |

#### Silver: `silver_market_data`
**Source:** `bronze_market_data`
**Transformation:** Deduplication on `(symbol, date)`.

| Column | Type | Transformation |
| :--- | :--- | :--- |
| `symbol` | `VARCHAR(10)` | Direct Copy |
| `date` | `DATE` | Direct Copy |
| `open` | `FLOAT` | Direct Copy |
| `high` | `FLOAT` | Direct Copy |
| `low` | `FLOAT` | Direct Copy |
| `close` | `FLOAT` | Direct Copy |
| `adjusted_close` | `FLOAT` | Direct Copy |
| `volume` | `BIGINT` | Direct Copy |
| `dividend_amount`| `FLOAT` | Direct Copy |
| `split_coefficient`| `FLOAT` | Direct Copy |

---

### 3.2 Finance Data Domain

#### Bronze: `bronze_finance_data`
**Source:** Alpha Vantage `OVERVIEW`

| Column | Type | API Key (Source) |
| :--- | :--- | :--- |
| `symbol` | `VARCHAR(10)` | `Symbol` |
| `asset_type` | `VARCHAR(50)` | `AssetType` |
| `name` | `VARCHAR(255)` | `Name` |
| `description` | `TEXT` | `Description` |
| `exchange` | `VARCHAR(50)` | `Exchange` |
| `currency` | `VARCHAR(10)` | `Currency` |
| `country` | `VARCHAR(50)` | `Country` |
| `sector` | `VARCHAR(50)` | `Sector` |
| `industry` | `VARCHAR(100)` | `Industry` |
| `market_cap` | `BIGINT` | `MarketCapitalization` |
| `ebitda` | `BIGINT` | `EBITDA` |
| `pe_ratio` | `FLOAT` | `PERatio` |
| `peg_ratio` | `FLOAT` | `PEGRatio` |
| `book_value` | `FLOAT` | `BookValue` |
| `dividend_per_share`| `FLOAT` | `DividendPerShare` |
| `dividend_yield` | `FLOAT` | `DividendYield` |
| `eps` | `FLOAT` | `EPS` |
| `revenue_ttm` | `BIGINT` | `RevenueTTM` |
| `gross_profit_ttm`| `BIGINT` | `GrossProfitTTM` |
| `diluted_eps_ttm` | `FLOAT` | `DilutedEPSTTM` |
| `quarterly_earnings_growth_yoy` | `FLOAT` | `QuarterlyEarningsGrowthYOY` |
| `quarterly_revenue_growth_yoy` | `FLOAT` | `QuarterlyRevenueGrowthYOY` |
| `analyst_target_price` | `FLOAT` | `AnalystTargetPrice` |
| `trailing_pe` | `FLOAT` | `TrailingPE` |
| `forward_pe` | `FLOAT` | `ForwardPE` |
| `price_to_sales_ratio_ttm` | `FLOAT` | `PriceToSalesRatioTTM` |
| `price_to_book_ratio` | `FLOAT` | `PriceToBookRatio` |
| `ev_to_revenue` | `FLOAT` | `EVToRevenue` |
| `ev_to_ebitda` | `FLOAT` | `EVToEBITDA` |
| `beta` | `FLOAT` | `Beta` |
| `52_week_high` | `FLOAT` | `52WeekHigh` |
| `52_week_low` | `FLOAT` | `52WeekLow` |
| `50_day_moving_average` | `FLOAT` | `50DayMovingAverage` |
| `200_day_moving_average` | `FLOAT` | `200DayMovingAverage` |
| `shares_outstanding` | `BIGINT` | `SharesOutstanding` |
| `dividend_date` | `DATE` | `DividendDate` |
| `ex_dividend_date` | `DATE` | `ExDividendDate` |


#### Silver: `silver_finance_data`
**Source:** `bronze_finance_data`
**Transformation:** Type Casting and Validation.

All columns from Bronze are carried over. Type casting handles string-to-number conversions ('None' -> NULL).

---

### 3.3 Earnings Data Domain

#### Bronze: `bronze_earnings_data`
**Source:** Alpha Vantage `EARNINGS` (quarterlyEarnings list)

| Column | Type | API Key (Source) |
| :--- | :--- | :--- |
| `symbol` | `VARCHAR(10)` | Input parameter |
| `fiscal_date_ending` | `DATE` | `fiscalDateEnding` |
| `reported_date` | `DATE` | `reportedDate` |
| `reported_eps` | `FLOAT` | `reportedEPS` |
| `estimated_eps` | `FLOAT` | `estimatedEPS` |
| `surprise` | `FLOAT` | `surprise` |
| `surprise_percentage`| `FLOAT` | `surprisePercentage` |

#### Silver: `silver_earnings_data`
**Source:** `bronze_earnings_data`
**Transformation:** Date standardization.

All columns carried over:
- `symbol`, `fiscal_date_ending`, `reported_date`, `reported_eps`, `estimated_eps`, `surprise`, `surprise_percentage`.

---

### 3.4 Price Target Data (Appears to be News Sentiment)

> **WARNING:** The naming `price_target_data` conflicts with the apparent content (News Sentiment).

#### Bronze: `bronze_price_target_data`
**Source:** Alpha Vantage `get_price_target` (likely `NEWS_SENTIMENT` endpoint upstream)

| Column | Type | Notes/Source Field |
| :--- | :--- | :--- |
| `symbol` | `VARCHAR(10)` | Input parameter |
| `published_date` | `TIMESTAMP` | `time_published` (inferred) |
| `news_url` | `TEXT` | `url` |
| `title` | `TEXT` | `title` |
| `text` | `TEXT` | `summary`? (check code) |
| `sentiment` | `VARCHAR(20)` | `overall_sentiment_label` |
| `sentiment_score` | `FLOAT` | `overall_sentiment_score` |

#### Silver: `silver_price_target_data`
**Source:** `bronze_price_target_data`
**Transformation:** No major transformation noted beyond select.

Columns: `symbol`, `published_date`, `news_url`, `title`, `text`, `sentiment`, `sentiment_score`.

## 4. Recommendations for Data Migration

1.  **Rename Price Target Jobs:** If this pipeline is indeed for News Sentiment, consider renaming the jobs and tables to `bronze_news_sentiment` and `silver_news_sentiment` to avoid confusion.
2.  **Verify New Source Compatibility:** Ensure the new data source provides fields compatible with:
    *   **Market Data:** OHLCV + Adjusted Close + Split/Dividend.
    *   **Finance:** Wide array of fundamental metrics (`ebitda`, `pe_ratio`, `market_cap`, etc.).
    *   **Earnings:** Historic quarterly earnings with surprise data.
    *   **News:** If strictly replacing Price Target, clarify if News is still needed or if actual Price Target data is desired.
