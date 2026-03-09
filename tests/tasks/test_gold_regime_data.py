from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from tasks.regime_data import gold_regime_data as regime_job


def test_validate_required_market_series_reports_missing_symbols() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["SPY", "SPY", "^VIX"],
            "date": ["2026-03-03", "2026-03-04", "2026-03-04"],
            "close": [580.0, 582.0, 21.5],
            "return_1d": [0.01, 0.003, None],
            "return_20d": [0.04, 0.05, None],
        }
    )

    normalized = regime_job._normalize_market_series(frame)

    with pytest.raises(ValueError) as excinfo:
        regime_job._validate_required_market_series(normalized)

    message = str(excinfo.value)
    assert "missing required regime symbols" in message
    assert "^VIX3M" in message
    assert "coverage=" in message


def test_assert_complete_regime_inputs_reports_non_overlapping_series() -> None:
    market_series = regime_job._validate_required_market_series(
        regime_job._normalize_market_series(
            pd.DataFrame(
                {
                    "symbol": ["SPY", "SPY", "^VIX", "^VIX3M"],
                    "date": ["2026-03-03", "2026-03-04", "2026-03-05", "2026-03-05"],
                    "close": [580.0, 582.0, 21.5, 22.0],
                    "return_1d": [0.01, 0.003, None, None],
                    "return_20d": [0.04, 0.05, None, None],
                }
            )
        )
    )

    inputs = regime_job._build_inputs_daily(
        market_series,
        computed_at=datetime(2026, 3, 9, tzinfo=timezone.utc),
    )

    with pytest.raises(ValueError) as excinfo:
        regime_job._assert_complete_regime_inputs(inputs, market_series=market_series)

    message = str(excinfo.value)
    assert "no complete SPY/^VIX/^VIX3M rows" in message
    assert "inputs_range=" in message
    assert "coverage=" in message
