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


def test_write_storage_outputs_refreshes_persisted_metadata_snapshots(monkeypatch: pytest.MonkeyPatch) -> None:
    parquet_paths: list[str] = []
    saved_artifact: dict[str, object] = {}
    snapshot_updates: list[dict[str, object]] = []

    class _FakeClient:
        def write_parquet(self, path: str, frame: pd.DataFrame) -> None:
            parquet_paths.append(path)
            assert isinstance(frame, pd.DataFrame)

    monkeypatch.setattr(regime_job.mdc, "get_storage_client", lambda _container: _FakeClient())
    monkeypatch.setattr(regime_job, "computed_at_iso", lambda: "2026-03-21T12:00:00+00:00")
    monkeypatch.setattr(
        "tasks.common.domain_artifacts.mdc.save_json_content",
        lambda payload, path, client=None: saved_artifact.update({"payload": payload, "path": path, "client": client}),
    )
    monkeypatch.setattr(
        "tasks.common.domain_artifacts.domain_metadata_snapshots.update_domain_metadata_snapshots_from_artifact",
        lambda **kwargs: snapshot_updates.append(kwargs),
    )

    inputs = pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-20")], "symbol": ["SPY"]})
    history = pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-20")], "regime_code": ["risk_on"]})
    latest = history.copy()
    transitions = pd.DataFrame({"effective_from_date": [pd.Timestamp("2026-03-20")]})

    regime_job._write_storage_outputs(
        gold_container="gold",
        inputs=inputs,
        history=history,
        latest=latest,
        transitions=transitions,
    )

    assert parquet_paths == [
        "regime/inputs.parquet",
        "regime/history.parquet",
        "regime/latest.parquet",
        "regime/transitions.parquet",
    ]
    assert saved_artifact["path"] == "regime/_metadata/domain.json"
    assert saved_artifact["payload"]["artifactPath"] == "regime/_metadata/domain.json"
    assert saved_artifact["payload"]["rootPath"] == "regime"
    assert len(snapshot_updates) == 1
    assert snapshot_updates[0]["layer"] == "gold"
    assert snapshot_updates[0]["domain"] == "regime"
    assert snapshot_updates[0]["artifact"]["artifactPath"] == "regime/_metadata/domain.json"
