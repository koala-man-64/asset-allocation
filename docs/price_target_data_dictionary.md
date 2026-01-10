# Price Target Data Dictionary

| Column Name | Description |
| :--- | :--- |
| **ticker** | Unique ticker identifier for the company. |
| **tp_mean_est** | The mean (average) target price estimate from all covering analysts. |
| **tp_std_dev_est** | The standard deviation of the target price estimates, representing analyst disagreement. |
| **tp_high_est** | The highest target price estimate among all analysts. |
| **tp_low_est** | The lowest target price estimate among all analysts. |
| **tp_cnt_est** | The total number of analysts providing a target price estimate. |
| **tp_cnt_est_rev_up** | The count of analysts who have revised their estimates upward in the recent period. |
| **tp_cnt_est_rev_down** | The count of analysts who have revised their estimates downward in the recent period. |
| **disp_abs** | Absolute dispersion: The range or spread of analyst estimates (High - Low or similar metric). |
| **disp_norm** | Normalized dispersion: Absolute dispersion divided by the mean, allowing comparison across stocks of different prices. |
| **disp_std_norm** | Standardized normalized dispersion: Normalized dispersion adjusted (z-score) relative to historical or peer group norms. |
| **rev_net** | Net Revisions: Revisions Up minus Revisions Down. |
| **rev_ratio** | Revision Ratio: The proportion of positive revisions relative to total revisions. |
| **rev_intensity** | Revision Intensity: A measure combining the frequency and magnitude of revisions. |
| **disp_norm_change_30d** | Change in normalized dispersion over the last 30 days (change in analyst consensus/uncertainty). |
| **tp_mean_change_30d** | Change in the mean target price estimate over the last 30 days. |
| **tp_mean_slope_90d** | The slope of the regression line of the mean target price over the last 90 days (trend direction). |
| **disp_z** | Z-score of the normalized dispersion relative to its 252-day rolling mean and standard deviation. |
