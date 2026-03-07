# DATA

This document is the current medallion data interface contract for AssetAllocation. It covers the canonical persisted datasets written by the Bronze, Silver, and Gold jobs under `tasks/` for the `market`, `finance`, `earnings`, and `price-target` domains.

Scope notes:

- This contract documents the bucketed medallion tables, not Postgres control-plane tables.
- `finance` Bronze stores provider payloads as opaque JSON strings. Downstream contracts are the extracted Silver and Gold schemas, not the provider's full inner JSON shape.
- The optional `market_by_date` Gold materialization is excluded because its column set is runtime-configurable via `GOLD_MARKET_BY_DATE_COLUMNS`, so it is not a fixed interface contract.

## Contract Conventions

| Convention | Meaning |
| --- | --- |
| `symbol` | Uppercased ticker/symbol identifier used across all domains. |
| `date` / `obs_date` | Timezone-naive normalized date/datetime field written by the ETL job. |
| `number` | Nullable numeric metric. Writer code typically uses `float64` unless explicitly noted otherwise. |
| `binary flag` | `0` or `1` indicator derived from business logic or pattern detection. |
| `nullable int` | Integer metric persisted with null support. |
| `json string` | Compact serialized provider payload stored as a string; inner keys are provider-defined and not frozen by this repo. |
| `bucket` layout | Canonical persisted datasets are written to alphabet bucket paths such as `.../buckets/A`, `.../buckets/B`, and so on. |

## Layer And Domain Inventory

| Layer | Domain | Canonical path pattern | Row grain | Notes |
| --- | --- | --- | --- | --- |
| Bronze | market | `market-data/buckets/{bucket}` | `symbol` + `date` | Raw market bars normalized to a stable OHLCV-plus-short-interest shape. |
| Silver | market | `market-data/buckets/{bucket}` | `symbol` + `date` | Canonical market history with stable snake_case columns. |
| Gold | market | `market/buckets/{bucket}` | `symbol` + `date` | Technical-feature table built from Silver OHLCV history. |
| Bronze | finance | `finance-data/buckets/{bucket}` | `symbol` + `report_type` | Raw Alpha Vantage report payloads plus coverage metadata. |
| Silver | finance / `balance_sheet` | `finance-data/balance_sheet/buckets/{bucket}` | `symbol` + `date` | Daily forward-filled balance-sheet subset for Piotroski inputs. |
| Silver | finance / `income_statement` | `finance-data/income_statement/buckets/{bucket}` | `symbol` + `date` | Daily forward-filled income-statement subset for Piotroski inputs. |
| Silver | finance / `cash_flow` | `finance-data/cash_flow/buckets/{bucket}` | `symbol` + `date` | Daily forward-filled cash-flow subset for Piotroski inputs. |
| Gold | finance | `finance/buckets/{bucket}` | `symbol` + `date` | Piotroski F-score output only. |
| Bronze | earnings | `earnings-data/buckets/{bucket}` | `symbol` + `date` | Raw quarterly earnings observations normalized to canonical columns. |
| Silver | earnings | `earnings-data/buckets/{bucket}` | `symbol` + `date` | Canonical earnings history. |
| Gold | earnings | `earnings/buckets/{bucket}` | `symbol` + `date` | Daily earnings-surprise features with days-since-event context. |
| Bronze | price-target | `price-target-data/buckets/{bucket}` | `symbol` + `obs_date` | Raw analyst target snapshots plus ingestion metadata. |
| Silver | price-target | `price-target-data/buckets/{bucket}` | `symbol` + `obs_date` | Daily forward-filled target history. |
| Gold | price-target | `targets/buckets/{bucket}` | `symbol` + `obs_date` | Dispersion and revision features built from Silver targets. |

## Market

### Bronze Market

Path: `market-data/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | string | Uppercased ticker symbol. |
| `date` | datetime | Trading session date for the bar. |
| `open` | number | Session opening price. |
| `high` | number | Session high price. |
| `low` | number | Session low price. |
| `close` | number | Session closing price. |
| `volume` | number | Session traded volume. |
| `short_interest` | number | Short-interest value joined during Bronze ingestion when available. |
| `short_volume` | number | Short-volume value joined during Bronze ingestion when available. |
| `ingested_at` | string | UTC ingestion timestamp recorded when the Bronze row is written. |
| `source_hash` | string | Hash of the normalized Bronze payload used for change detection and watermarking. |

### Silver Market

Path: `market-data/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Canonical trading date. |
| `symbol` | string | Uppercased ticker symbol. |
| `open` | number | Session opening price. |
| `high` | number | Session high price. |
| `low` | number | Session low price. |
| `close` | number | Session closing price. |
| `volume` | number | Session traded volume. |
| `short_interest` | number | Canonical short-interest metric. |
| `short_volume` | number | Canonical short-volume metric. |

### Gold Market

Path: `market/buckets/{bucket}`

Base columns:

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Feature as-of date. |
| `symbol` | string | Uppercased ticker symbol. |
| `open` | number | Input open price carried from Silver. |
| `high` | number | Input high price carried from Silver. |
| `low` | number | Input low price carried from Silver. |
| `close` | number | Input close price carried from Silver. |
| `volume` | number | Input volume carried from Silver. |

Return, volatility, range, and volume-context columns:

| Column | Type | Description |
| --- | --- | --- |
| `return_1d` | number | One-day close-to-close return. |
| `return_5d` | number | Five-day close-to-close return. |
| `return_20d` | number | Twenty-day close-to-close return. |
| `return_60d` | number | Sixty-day close-to-close return. |
| `vol_20d` | number | Rolling 20-day standard deviation of daily return. |
| `vol_60d` | number | Rolling 60-day standard deviation of daily return. |
| `rolling_max_252d` | number | Rolling 252-day maximum close. |
| `drawdown_1y` | number | Current close divided by rolling 252-day max minus 1. |
| `true_range` | number | Max of intraday range and prior-close gap range. |
| `atr_14d` | number | Fourteen-day average true range. |
| `gap_atr` | number | Absolute open-to-prior-close gap normalized by ATR. |
| `bb_width_20d` | number | Normalized 20-day Bollinger-band width. |
| `range_close` | number | Intraday range divided by close. |
| `range_20` | number | Rolling 20-day high-low range divided by close. |
| `compression_score` | number | Percentile rank of `range_20` over a 252-day lookback. |
| `volume_z_20d` | number | Twenty-day z-score of volume. |
| `volume_pct_rank_252d` | number | Percentile rank of volume over a 252-day lookback. |

Trend and moving-average columns:

| Column | Type | Description |
| --- | --- | --- |
| `sma_20d` | number | 20-day simple moving average of close. |
| `sma_50d` | number | 50-day simple moving average of close. |
| `sma_200d` | number | 200-day simple moving average of close. |
| `sma_20_gt_sma_50` | binary flag | `1` when `sma_20d > sma_50d`. |
| `sma_50_gt_sma_200` | binary flag | `1` when `sma_50d > sma_200d`. |
| `trend_50_200` | number | `sma_50d / sma_200d - 1`. |
| `above_sma_50` | binary flag | `1` when close is above `sma_50d`. |
| `sma_20_crosses_above_sma_50` | binary flag | `1` on the row where `sma_20_gt_sma_50` flips from `0` to `1`. |
| `sma_20_crosses_below_sma_50` | binary flag | `1` on the row where `sma_20_gt_sma_50` flips from `1` to `0`. |
| `sma_50_crosses_above_sma_200` | binary flag | `1` on the row where `sma_50_gt_sma_200` flips from `0` to `1`. |
| `sma_50_crosses_below_sma_200` | binary flag | `1` on the row where `sma_50_gt_sma_200` flips from `1` to `0`. |

Candle-geometry columns:

| Column | Type | Description |
| --- | --- | --- |
| `range` | number | `high - low`, clipped at zero. |
| `body` | number | Absolute candle body size, `abs(close - open)`. |
| `is_bull` | binary flag | `1` when `close > open`. |
| `is_bear` | binary flag | `1` when `close < open`. |
| `upper_shadow` | number | Distance from candle body top to session high. |
| `lower_shadow` | number | Distance from session low to candle body bottom. |
| `body_to_range` | number | Candle body divided by session range. |
| `upper_to_range` | number | Upper shadow divided by session range. |
| `lower_to_range` | number | Lower shadow divided by session range. |

Candlestick pattern flags:

| Column | Type | Description |
| --- | --- | --- |
| `pat_doji` | binary flag | `1` when a doji pattern is detected. |
| `pat_spinning_top` | binary flag | `1` when a spinning-top pattern is detected. |
| `pat_bullish_marubozu` | binary flag | `1` when a bullish marubozu pattern is detected. |
| `pat_bearish_marubozu` | binary flag | `1` when a bearish marubozu pattern is detected. |
| `pat_star_gap_up` | binary flag | `1` when a star-style gap-up setup is detected. |
| `pat_star_gap_down` | binary flag | `1` when a star-style gap-down setup is detected. |
| `pat_star` | binary flag | `1` when a generic star candle is detected. |
| `pat_hammer` | binary flag | `1` when a hammer pattern is detected. |
| `pat_hanging_man` | binary flag | `1` when a hanging-man pattern is detected. |
| `pat_inverted_hammer` | binary flag | `1` when an inverted-hammer pattern is detected. |
| `pat_shooting_star` | binary flag | `1` when a shooting-star pattern is detected. |
| `pat_dragonfly_doji` | binary flag | `1` when a dragonfly-doji pattern is detected. |
| `pat_gravestone_doji` | binary flag | `1` when a gravestone-doji pattern is detected. |
| `pat_bullish_spinning_top` | binary flag | `1` when a bullish spinning-top context is detected. |
| `pat_bearish_spinning_top` | binary flag | `1` when a bearish spinning-top context is detected. |
| `pat_bullish_engulfing` | binary flag | `1` when a bullish engulfing pattern is detected. |
| `pat_bearish_engulfing` | binary flag | `1` when a bearish engulfing pattern is detected. |
| `pat_bullish_harami` | binary flag | `1` when a bullish harami pattern is detected. |
| `pat_bearish_harami` | binary flag | `1` when a bearish harami pattern is detected. |
| `pat_piercing_line` | binary flag | `1` when a piercing-line pattern is detected. |
| `pat_dark_cloud_line` | binary flag | `1` when a dark-cloud-line pattern is detected. |
| `pat_tweezer_bottom` | binary flag | `1` when a tweezer-bottom pattern is detected. |
| `pat_tweezer_top` | binary flag | `1` when a tweezer-top pattern is detected. |
| `pat_bullish_kicker` | binary flag | `1` when a bullish kicker pattern is detected. |
| `pat_bearish_kicker` | binary flag | `1` when a bearish kicker pattern is detected. |
| `pat_morning_star` | binary flag | `1` when a morning-star pattern is detected. |
| `pat_morning_doji_star` | binary flag | `1` when a morning-doji-star pattern is detected. |
| `pat_evening_star` | binary flag | `1` when an evening-star pattern is detected. |
| `pat_evening_doji_star` | binary flag | `1` when an evening-doji-star pattern is detected. |
| `pat_bullish_abandoned_baby` | binary flag | `1` when a bullish abandoned-baby pattern is detected. |
| `pat_bearish_abandoned_baby` | binary flag | `1` when a bearish abandoned-baby pattern is detected. |
| `pat_three_white_soldiers` | binary flag | `1` when a three-white-soldiers pattern is detected. |
| `pat_three_black_crows` | binary flag | `1` when a three-black-crows pattern is detected. |
| `pat_bullish_three_line_strike` | binary flag | `1` when a bullish three-line-strike pattern is detected. |
| `pat_bearish_three_line_strike` | binary flag | `1` when a bearish three-line-strike pattern is detected. |
| `pat_three_inside_up` | binary flag | `1` when a three-inside-up pattern is detected. |
| `pat_three_outside_up` | binary flag | `1` when a three-outside-up pattern is detected. |
| `pat_three_inside_down` | binary flag | `1` when a three-inside-down pattern is detected. |
| `pat_three_outside_down` | binary flag | `1` when a three-outside-down pattern is detected. |

Heikin-Ashi and Ichimoku columns:

| Column | Type | Description |
| --- | --- | --- |
| `ha_open` | number | Heikin-Ashi open value. |
| `ha_high` | number | Heikin-Ashi high value. |
| `ha_low` | number | Heikin-Ashi low value. |
| `ha_close` | number | Heikin-Ashi close value. |
| `ichimoku_tenkan_sen_9` | number | Ichimoku tenkan-sen (9-period conversion line). |
| `ichimoku_kijun_sen_26` | number | Ichimoku kijun-sen (26-period base line). |
| `ichimoku_senkou_span_a` | number | Ichimoku senkou span A at the row's as-of date. |
| `ichimoku_senkou_span_b` | number | Ichimoku senkou span B at the row's as-of date. |
| `ichimoku_senkou_span_a_26` | number | Senkou span A shifted 26 periods for alignment without look-ahead leakage. |
| `ichimoku_senkou_span_b_26` | number | Senkou span B shifted 26 periods for alignment without look-ahead leakage. |
| `ichimoku_chikou_span_26` | number | Close shifted 26 periods to represent the chikou span. |

Evidence:

- `README.md:3-9`
- `core/pipeline.py:29-37`
- `tasks/market_data/bronze_market_data.py:38-49`
- `tasks/market_data/silver_market_data.py:52-87`
- `tasks/market_data/gold_market_data.py:146-249`
- `tasks/technical_analysis/technical_indicators.py:92-182`
- `tasks/technical_analysis/technical_indicators.py:184-490`

## Finance

### Bronze Finance

Path: `finance-data/buckets/{bucket}`

`report_type` values emitted by the Bronze job currently come from the configured report set: `balance_sheet`, `cash_flow`, `income_statement`, and `overview`. The `overview` payload is kept for legacy valuation-path continuity, but Silver and Gold only materialize the Piotroski subdomains (`balance_sheet`, `income_statement`, `cash_flow`).

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | string | Uppercased ticker symbol. |
| `report_type` | string | Finance report family stored in `payload_json`. |
| `payload_json` | json string | Compact serialized provider response for the report type. |
| `source_min_date` | string | Earliest report date found in the provider payload, if available. |
| `source_max_date` | string | Latest report date found in the provider payload, if available. |
| `ingested_at` | string | UTC ingestion timestamp for the Bronze row. |
| `payload_hash` | string | Hash of `payload_json` used for change detection. |

### Silver Finance: `balance_sheet`

Path: `finance-data/balance_sheet/buckets/{bucket}`

Rows are extracted from Bronze JSON, reduced to the required Piotroski fields, then resampled to daily frequency with forward fill.

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Daily as-of date after forward fill. |
| `symbol` | string | Uppercased ticker symbol. |
| `long_term_debt` | number | Long-term debt input for leverage checks. |
| `total_assets` | number | Total assets input for ROA and asset-turnover calculations. |
| `current_assets` | number | Current assets input for liquidity calculations. |
| `current_liabilities` | number | Current liabilities input for liquidity calculations. |
| `shares_outstanding` | number | Shares outstanding input for share-dilution checks. |

### Silver Finance: `income_statement`

Path: `finance-data/income_statement/buckets/{bucket}`

Rows are extracted from Bronze JSON, reduced to the required Piotroski fields, then resampled to daily frequency with forward fill.

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Daily as-of date after forward fill. |
| `symbol` | string | Uppercased ticker symbol. |
| `total_revenue` | number | Total revenue input for growth and margin calculations. |
| `gross_profit` | number | Gross profit input for gross-margin calculations. |
| `net_income` | number | Net income input for ROA and profitability checks. |

### Silver Finance: `cash_flow`

Path: `finance-data/cash_flow/buckets/{bucket}`

Rows are extracted from Bronze JSON, reduced to the required Piotroski fields, then resampled to daily frequency with forward fill.

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Daily as-of date after forward fill. |
| `symbol` | string | Uppercased ticker symbol. |
| `operating_cash_flow` | number | Operating cash flow input for cash-generation and accrual checks. |

### Gold Finance

Path: `finance/buckets/{bucket}`

Gold finance computes a larger feature set internally, but the persisted table is the projected Piotroski output only.

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Daily as-of date from the merged Silver finance inputs. |
| `symbol` | string | Uppercased ticker symbol. |
| `piotroski_roa_pos` | nullable int | `1` when trailing-twelve-month ROA is positive. |
| `piotroski_cfo_pos` | nullable int | `1` when trailing-twelve-month operating cash flow is positive. |
| `piotroski_delta_roa_pos` | nullable int | `1` when trailing-twelve-month ROA improved versus four periods earlier. |
| `piotroski_accruals_pos` | nullable int | `1` when operating cash flow exceeds net income on a trailing-twelve-month basis. |
| `piotroski_leverage_decrease` | nullable int | `1` when long-term-debt-to-assets improved versus four periods earlier. |
| `piotroski_liquidity_increase` | nullable int | `1` when current ratio improved versus four periods earlier. |
| `piotroski_no_new_shares` | nullable int | `1` when shares outstanding did not increase versus four periods earlier. |
| `piotroski_gross_margin_increase` | nullable int | `1` when trailing-twelve-month gross margin improved versus four periods earlier. |
| `piotroski_asset_turnover_increase` | nullable int | `1` when trailing-twelve-month asset turnover improved versus four periods earlier. |
| `piotroski_f_score` | nullable int | Sum of the nine Piotroski component flags. |

Evidence:

- `core/pipeline.py:50-67`
- `core/pipeline.py:89`
- `tasks/finance_data/bronze_finance_data.py:42-74`
- `tasks/common/finance_contracts.py:3-132`
- `tasks/finance_data/silver_finance_data.py:242-284`
- `tasks/finance_data/silver_finance_data.py:287-312`
- `tasks/finance_data/silver_finance_data.py:696-745`
- `tasks/finance_data/gold_finance_data.py:95-109`
- `tasks/finance_data/gold_finance_data.py:459-651`

## Earnings

### Bronze Earnings

Path: `earnings-data/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | string | Uppercased ticker symbol. |
| `date` | datetime | Earnings event date. |
| `reported_eps` | number | Reported earnings per share. |
| `eps_estimate` | number | Consensus EPS estimate. |
| `surprise` | number | Surprise metric normalized from the provider payload. |
| `ingested_at` | string | UTC ingestion timestamp for the Bronze row. |
| `source_hash` | string | Hash of the normalized Bronze earnings payload. |

### Silver Earnings

Path: `earnings-data/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Canonical earnings date. |
| `symbol` | string | Uppercased ticker symbol. |
| `reported_eps` | number | Reported earnings per share. |
| `eps_estimate` | number | Consensus EPS estimate. |
| `surprise` | number | Surprise metric carried from Bronze. |

### Gold Earnings

Path: `earnings/buckets/{bucket}`

Gold earnings expands sparse quarterly observations into a daily forward-filled feature table.

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Daily as-of date after expansion. |
| `symbol` | string | Uppercased ticker symbol. |
| `reported_eps` | number | Last reported EPS value carried forward from the most recent earnings event. |
| `eps_estimate` | number | Last EPS estimate carried forward from the most recent earnings event. |
| `surprise` | number | Last raw surprise value carried forward from the most recent earnings event. |
| `surprise_pct` | number | `(reported_eps - eps_estimate) / abs(eps_estimate)`. |
| `surprise_mean_4q` | number | Rolling four-quarter mean of `surprise_pct`. |
| `surprise_std_8q` | number | Rolling eight-quarter standard deviation of `surprise_pct`. |
| `beat_rate_8q` | number | Rolling eight-quarter share of positive `surprise_pct` values. |
| `is_earnings_day` | binary flag | `1` on rows representing the actual earnings event date, else `0`. |
| `last_earnings_date` | datetime | Most recent earnings date carried forward to each daily row. |
| `days_since_earnings` | number | Integer day difference between `date` and `last_earnings_date`. |

Evidence:

- `core/pipeline.py:46`
- `core/pipeline.py:83-86`
- `tasks/earnings_data/bronze_earnings_data.py:44-52`
- `tasks/earnings_data/bronze_earnings_data.py:216-230`
- `tasks/earnings_data/silver_earnings_data.py:47-52`
- `tasks/earnings_data/silver_earnings_data.py:195-223`
- `tasks/earnings_data/gold_earnings_data.py:100-149`

## Price Target

### Bronze Price Target

Path: `price-target-data/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | string | Uppercased ticker symbol. |
| `obs_date` | datetime | Observation date for the target snapshot. |
| `tp_mean_est` | number | Mean analyst price target. |
| `tp_std_dev_est` | number | Standard deviation of analyst price targets. |
| `tp_high_est` | number | Highest analyst price target. |
| `tp_low_est` | number | Lowest analyst price target. |
| `tp_cnt_est` | number | Count of contributing analyst estimates. |
| `tp_cnt_est_rev_up` | number | Count of upward target revisions. |
| `tp_cnt_est_rev_down` | number | Count of downward target revisions. |
| `ingested_at` | string | UTC ingestion timestamp for the Bronze row. |
| `source_hash` | string | Hash of the normalized Bronze target payload. |

### Silver Price Target

Path: `price-target-data/buckets/{bucket}`

Silver price-target data is reindexed to a daily series and forward-filled so every stored row has the canonical target columns.

| Column | Type | Description |
| --- | --- | --- |
| `obs_date` | datetime | Daily observation date after forward fill. |
| `symbol` | string | Uppercased ticker symbol. |
| `tp_mean_est` | number | Mean analyst price target. |
| `tp_std_dev_est` | number | Standard deviation of analyst price targets. |
| `tp_high_est` | number | Highest analyst price target. |
| `tp_low_est` | number | Lowest analyst price target. |
| `tp_cnt_est` | number | Count of contributing analyst estimates. |
| `tp_cnt_est_rev_up` | number | Count of upward target revisions. |
| `tp_cnt_est_rev_down` | number | Count of downward target revisions. |

### Gold Price Target

Path: `targets/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `obs_date` | datetime | Daily observation date. |
| `symbol` | string | Uppercased ticker symbol. |
| `tp_mean_est` | number | Mean analyst price target carried from Silver. |
| `tp_std_dev_est` | number | Standard deviation of analyst targets carried from Silver. |
| `tp_high_est` | number | Highest analyst target carried from Silver. |
| `tp_low_est` | number | Lowest analyst target carried from Silver. |
| `tp_cnt_est` | number | Count of contributing analyst estimates. |
| `tp_cnt_est_rev_up` | number | Count of upward target revisions. |
| `tp_cnt_est_rev_down` | number | Count of downward target revisions. |
| `disp_abs` | number | Absolute target dispersion, `tp_high_est - tp_low_est`. |
| `disp_norm` | number | Target dispersion normalized by `tp_mean_est`. |
| `disp_std_norm` | number | Target standard deviation normalized by `tp_mean_est`. |
| `rev_net` | number | Net revisions, `tp_cnt_est_rev_up - tp_cnt_est_rev_down`. |
| `rev_ratio` | number | Revision ratio, `(tp_cnt_est_rev_up + 1) / (tp_cnt_est_rev_down + 1)`. |
| `rev_intensity` | number | Net revisions normalized by `tp_cnt_est`. |
| `disp_norm_change_30d` | number | Thirty-day change in normalized dispersion. |
| `tp_mean_change_30d` | number | Thirty-day change in mean target. |
| `disp_z` | number | 252-day z-score of normalized dispersion. |
| `tp_mean_slope_90d` | number | Ninety-day rolling slope of `tp_mean_est`. |

Evidence:

- `core/pipeline.py:75-79`
- `tasks/price_target_data/bronze_price_target_data.py:33-44`
- `tasks/price_target_data/bronze_price_target_data.py:148-169`
- `tasks/price_target_data/silver_price_target_data.py:45-55`
- `tasks/price_target_data/silver_price_target_data.py:111-183`
- `tasks/price_target_data/silver_price_target_data.py:263-306`
- `tasks/price_target_data/gold_price_target_data.py:125-186`
