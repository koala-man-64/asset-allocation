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


def test_validate_required_market_series_logs_fast_fail_context(monkeypatch: pytest.MonkeyPatch) -> None:
    frame = regime_job._normalize_market_series(
        pd.DataFrame(
            {
                "symbol": ["SPY", "^VIX"],
                "date": ["2026-03-03", "2026-03-03"],
                "close": [580.0, 21.5],
                "return_1d": [0.01, None],
                "return_20d": [0.04, None],
            }
        )
    )
    errors: list[str] = []

    monkeypatch.setattr(regime_job, "_summarize_market_sync_state", lambda _dsn: "market_sync_state=empty")
    monkeypatch.setattr(regime_job.mdc, "write_error", lambda msg: errors.append(str(msg)))

    with pytest.raises(ValueError) as excinfo:
        regime_job._validate_required_market_series(frame, dsn="postgresql://example")

    message = str(excinfo.value)
    assert "Gold regime fast-fail" in message
    assert "market_sync_state=empty" in message
    assert "gold-market-job" in message
    assert errors == [message]


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


def test_main_fast_fails_before_loading_models_when_market_inputs_are_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_initialized = False

    monkeypatch.setenv("POSTGRES_DSN", "postgresql://example")
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold")
    monkeypatch.setattr(regime_job.mdc, "log_environment_diagnostics", lambda: None)

    def _missing_market_inputs(_dsn: str) -> pd.DataFrame:
        raise ValueError("Gold regime fast-fail: missing market inputs")

    class _UnexpectedRepo:
        def __init__(self, _dsn: str) -> None:
            nonlocal repo_initialized
            repo_initialized = True

    monkeypatch.setattr(regime_job, "_load_market_series", _missing_market_inputs)
    monkeypatch.setattr(regime_job, "RegimeRepository", _UnexpectedRepo)

    with pytest.raises(ValueError, match="missing market inputs"):
        regime_job.main()

    assert repo_initialized is False
