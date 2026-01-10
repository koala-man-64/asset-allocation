# Market Data Dictionary

| Column Name | Description |
| :--- | :--- |
| **open** | The price at which the asset started trading during the period. |
| **high** | The maximum price reached during the trading period. |
| **low** | The minimum price reached during the trading period. |
| **close** | The final price at the end of the trading period. |
| **volume** | The total number of shares or contracts traded during the period. |
| **symbol** | The unique ticker identifier for the asset (e.g., AAPL, MSFT). |
| **return_1d** | The percentage change in price from the previous day's close. |
| **return_5d** | The percentage change in price over the last 5 trading days. |
| **return_20d** | The percentage change in price over the last 20 trading days (~1 month). |
| **return_60d** | The percentage change in price over the last 60 trading days (~3 months). |
| **vol_20d** | Annualized volatility calculated over a 20-day window. |
| **vol_60d** | Annualized volatility calculated over a 60-day window. |
| **rolling_max_252d** | The highest closing price observed over the last 252 trading days (~1 year). |
| **drawdown** | The percentage decline from the `rolling_max_252d` to the current price. |
| **true_range** | The greatest of: current high-low, abs(current high - prev close), or abs(current low - prev close). |
| **atr_14d** | Average True Range (14-day), a smoothing of the True Range values to measure volatility. |
| **sma_20d** | Simple Moving Average of the close price over the last 20 days. |
| **sma_50d** | Simple Moving Average of the close price over the last 50 days. |
| **sma_200d** | Simple Moving Average of the close price over the last 200 days. |
| **sma_20_gt_sma_50** | Boolean indicator: True if SMA 20 is greater than SMA 50. |
| **sma_50_gt_sma_200** | Boolean indicator: True if SMA 50 is greater than SMA 200 (often checks for long-term trend alignment). |
| **sma_20_crosses_above_sma_50** | Signal: True on the specific day SMA 20 crosses above SMA 50. |
| **sma_20_crosses_below_sma_50** | Signal: True on the specific day SMA 20 crosses below SMA 50. |
| **sma_50_crosses_above_sma_200** | Signal: True on the day SMA 50 crosses above SMA 200 (commonly called a "Golden Cross"). |
| **sma_50_crosses_below_sma_200** | Signal: True on the day SMA 50 crosses below SMA 200 (commonly called a "Death Cross"). |
| **bb_width_20d** | Bollinger Band Width (20-day): The difference between the upper and lower bands, normalized by the middle band. |
| **range_close** | The position of the close within the daily range (0 to 1), calculated as `(close - low) / (high - low)`. |
| **volume_z_20d** | Z-score of the current volume compared to the 20-day average volume (measures distinct volume spikes). |
| **volume_pct_rank_252d** | The percentile rank of the current volume compared to the last 252 days of volume data. |
| **gap_atr** | The absolute gap between the open price and the previous close, normalized by the 14-day ATR. |
| **trend_50_200** | The ratio of the 50-day SMA to the 200-day SMA minus 1, indicating trend strength and direction. |
| **above_sma_50** | Boolean indicator: True if the close price is above the 50-day SMA. |
| **range_20** | The range (High - Low) over the last 20 days relative to the current close price. |
| **compression_score** | The percentile rank of the 20-day range over the last 252 days (lower values indicate compression). |
