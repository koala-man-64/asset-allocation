# Silver & Gold Layer Audit Report

## 1. Execution Summary
This report details the audit of the Silver to Gold layer transformations. The Gold layer is designed for business-level aggregation and analytics.

**Key Finding:** The Gold layer jobs generally perform simple selects from the Silver layer, often filtering by the latest date or specific criteria. The column names are largely preserved from the Silver layer.

## 2. Architectural Alignment Matrix
The following table maps the data flow from Silver to Gold layers.

| Domain | Silver Job | Gold Job | Transformation Logic | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Market Data** | `silver_market_data` | `gold_market_data` | Per-symbol transform from Silver to Gold | Verified |
| **Finance Data** | `silver_finance_data` | `gold_finance_data` | Per-symbol transform from Silver to Gold | Verified |
| **Earnings Data** | `silver_earnings_data` | `gold_earnings_data` | Per-symbol transform from Silver to Gold | Verified |
| **Price Target** | `silver_price_target_data` | `gold_price_target_data` | Per-symbol transform from Silver to Gold | Verified |

## 3. Detailed Column Mapping

### 3.1 Market Data Domain

#### Silver: `silver_market_data`
*Source columns see Bronze-Silver report.*

#### Gold: `gold_market_data`
**Source:** `silver_market_data`
**Transformation:** Pass-through (Select *).

| Column | Type | Notes |
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

#### Silver: `silver_finance_data`
*Source columns see Bronze-Silver report.*

#### Gold: `gold_finance_data`
**Source:** `silver_finance_data`
**Transformation:** Pass-through (Select *).

All columns from Silver are carried over directly.

| Column | Type | Notes |
| :--- | :--- | :--- |
| `symbol` | `VARCHAR(10)` | `Symbol` |
| `asset_type` | `VARCHAR(50)` | `AssetType` |
| `name` | `VARCHAR(255)` | `Name` |
| ... | ... | ... |
| `ex_dividend_date` | `DATE` | `ExDividendDate` |

*(Full list matches Silver schema)*

---

### 3.3 Earnings Data Domain

#### Silver: `silver_earnings_data`
*Source columns see Bronze-Silver report.*

#### Gold: `gold_earnings_data`
**Source:** `silver_earnings_data`
**Transformation:** Pass-through (Select *).

All columns carried over:
- `symbol`, `fiscal_date_ending`, `reported_date`, `reported_eps`, `estimated_eps`, `surprise`, `surprise_percentage`.

---

### 3.4 Price Target (News Sentiment) Data

#### Silver: `silver_price_target_data`
*Source columns see Bronze-Silver report.*

#### Gold: `gold_price_target_data`
**Source:** `silver_price_target_data`
**Transformation:** Pass-through (Select *).

Columns: `symbol`, `published_date`, `news_url`, `title`, `text`, `sentiment`, `sentiment_score`.

## 4. Recommendations for Gold Layer

1.  **Deduplication/Audit:** The Gold layer still carries overlap with Silver in some domains. Periodically validate that each Gold feature set remains necessary for downstream analytics.
2.  **Naming:** If the Silver layer is renamed (e.g. for the News Sentiment finding), the Gold layer must also be updated to reflect the new source table names.
