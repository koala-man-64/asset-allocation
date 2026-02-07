# Gold Market Indicator Columns

This document defines additional Gold market columns added for Heikin-Ashi and Ichimoku indicators.

## Heikin-Ashi Columns

- `ha_close` = `(open + high + low + close) / 4`
- `ha_open` = `(prev_ha_open + prev_ha_close) / 2`
  - Seed value for first row per symbol: `(open + close) / 2`
- `ha_high` = `max(high, ha_open, ha_close)`
- `ha_low` = `min(low, ha_open, ha_close)`

## Ichimoku Columns

All columns are computed per symbol in date order.

- `ichimoku_tenkan_sen_9` = `(rolling_high_9 + rolling_low_9) / 2`
- `ichimoku_kijun_sen_26` = `(rolling_high_26 + rolling_low_26) / 2`
- `ichimoku_senkou_span_a` = `(ichimoku_tenkan_sen_9 + ichimoku_kijun_sen_26) / 2`
- `ichimoku_senkou_span_b` = `(rolling_high_52 + rolling_low_52) / 2`
- `ichimoku_senkou_span_a_26` = `ichimoku_senkou_span_a.shift(26)`
- `ichimoku_senkou_span_b_26` = `ichimoku_senkou_span_b.shift(26)`
- `ichimoku_chikou_span_26` = `close.shift(26)`

## Alignment Policy

Shifted Ichimoku columns use positive shifts only so row `t` never depends on future data (`t + n`).
