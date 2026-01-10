# Earnings Data Dictionary

| Column Name | Description |
| :--- | :--- |
| **symbol** | Unique ticker identifier for the company. |
| **reported_eps** | The actual Earnings Per Share (EPS) reported by the company for the period. |
| **eps_estimate** | The consensus analyst estimate for Earnings Per Share prior to the report. |
| **surprise** | The absolute difference between the reported EPS and the estimate (`reported_eps` - `eps_estimate`). |
| **date_parsed** | The date the earnings report was released or the data was recorded. |
| **surprise_pct** | The earnings surprise expressed as a percentage of the estimate. |
| **surprise_mean_4q** | The average earnings surprise (absolute or percentage) over the last 4 quarters. |
| **surprise_std_8q** | The standard deviation of earnings surprises over the last 8 quarters (measure of consistency). |
| **beat_rate_8q** | The percentage of quarters where the reported EPS exceeded the estimate over the last 8 quarters. |
| **is_earnings_day** | Flag indicating if an earnings report was released on this date (1 for yes, 0 for no). |
| **days_since_earnings** | Number of days elapsed since the last earnings report. |
