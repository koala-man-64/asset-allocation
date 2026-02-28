from __future__ import annotations

from typing import Any

import pandas as pd

from tasks.market_data import materialize_gold_market_by_date as by_date


def test_materialize_market_by_date_projects_selected_columns(monkeypatch) -> None:
    frames = {
        "market/AAPL": pd.DataFrame(
            {
                "date": ["2025-01-02", "2025-01-03"],
                "symbol": ["AAPL", "AAPL"],
                "close": [189.0, 190.0],
                "return_1d": [0.01, 0.005],
                "volume": [1000, 1100],
            }
        ),
        "market/MSFT": pd.DataFrame(
            {
                "Date": ["2025-01-02"],
                "Symbol": ["MSFT"],
                "Close": [330.0],
                "Return 1d": [0.002],
                "Volume": [2000],
            }
        ),
    }

    monkeypatch.setattr(
        by_date.delta_core,
        "load_delta",
        lambda _container, path, **_kwargs: frames.get(path).copy() if path in frames else pd.DataFrame(),
    )
    monkeypatch.setattr(by_date, "get_backfill_range", lambda: (None, None))

    captured: dict[str, Any] = {}

    def fake_store_delta(df: pd.DataFrame, container: str, path: str, mode: str = "overwrite", **kwargs) -> None:
        captured["df"] = df.copy()
        captured["container"] = container
        captured["path"] = path
        captured["mode"] = mode
        captured["kwargs"] = dict(kwargs)

    monkeypatch.setattr(by_date.delta_core, "store_delta", fake_store_delta)

    config = by_date.MaterializeConfig(
        container="gold",
        domain="market",
        target_path="market_by_date",
        include_columns=["close", "return_1d", "missing_col"],
        year_month=None,
    )

    result = by_date.materialize_market_by_date(config, source_paths=["market/AAPL", "market/MSFT"])

    assert result.rows_written == 3
    assert result.target_path == "market_by_date"

    written = captured["df"]
    assert list(written.columns) == ["date", "symbol", "close", "return_1d", "year_month"]
    assert set(written["symbol"].tolist()) == {"AAPL", "MSFT"}
    assert set(written["year_month"].tolist()) == {"2025-01"}

    assert captured["mode"] == "overwrite"
    assert captured["kwargs"]["partition_by"] == ["year_month"]
    assert "schema_mode" not in captured["kwargs"]
    assert captured["kwargs"]["predicate"] is None


def test_materialize_market_by_date_filters_year_month_and_uses_partition_predicate(monkeypatch) -> None:
    frame = pd.DataFrame(
        {
            "date": ["2025-01-02", "2025-02-03"],
            "symbol": ["AAPL", "AAPL"],
            "close": [189.0, 193.0],
            "volume": [1000, 1200],
        }
    )

    monkeypatch.setattr(by_date.delta_core, "load_delta", lambda _container, _path, **_kwargs: frame.copy())
    monkeypatch.setattr(by_date, "get_backfill_range", lambda: (None, None))

    captured: dict[str, Any] = {}

    def fake_store_delta(df: pd.DataFrame, container: str, path: str, mode: str = "overwrite", **kwargs) -> None:
        captured["df"] = df.copy()
        captured["kwargs"] = dict(kwargs)

    monkeypatch.setattr(by_date.delta_core, "store_delta", fake_store_delta)

    config = by_date.MaterializeConfig(
        container="gold",
        domain="market",
        target_path="market_by_date",
        include_columns=None,
        year_month="2025-01",
    )

    result = by_date.materialize_market_by_date(config, source_paths=["market/AAPL"])

    assert result.rows_written == 1
    written = captured["df"]
    assert written["year_month"].tolist() == ["2025-01"]
    assert written["date"].dt.month.tolist() == [1]

    assert captured["kwargs"]["partition_by"] == ["year_month"]
    assert "schema_mode" not in captured["kwargs"]
    assert captured["kwargs"]["predicate"] == "year_month = '2025-01'"


def test_materialize_market_by_date_filters_year_month_range_and_uses_range_predicate(monkeypatch) -> None:
    frame = pd.DataFrame(
        {
            "date": ["2025-01-15", "2025-02-03", "2025-03-10"],
            "symbol": ["AAPL", "AAPL", "AAPL"],
            "close": [189.0, 193.0, 197.0],
            "volume": [1000, 1200, 1300],
        }
    )

    monkeypatch.setattr(by_date.delta_core, "load_delta", lambda _container, _path, **_kwargs: frame.copy())
    monkeypatch.setattr(by_date, "get_backfill_range", lambda: (None, None))

    captured: dict[str, Any] = {}

    def fake_store_delta(df: pd.DataFrame, container: str, path: str, mode: str = "overwrite", **kwargs) -> None:
        captured["df"] = df.copy()
        captured["kwargs"] = dict(kwargs)

    monkeypatch.setattr(by_date.delta_core, "store_delta", fake_store_delta)

    config = by_date.MaterializeConfig(
        container="gold",
        domain="market",
        target_path="market_by_date",
        include_columns=None,
        year_month="2025-01",
        year_month_end="2025-02",
    )

    result = by_date.materialize_market_by_date(config, source_paths=["market/AAPL"])

    assert result.rows_written == 2
    written = captured["df"]
    assert written["year_month"].tolist() == ["2025-01", "2025-02"]
    assert written["date"].dt.month.tolist() == [1, 2]

    assert captured["kwargs"]["partition_by"] == ["year_month"]
    assert "schema_mode" not in captured["kwargs"]
    assert captured["kwargs"]["predicate"] == "year_month >= '2025-01' AND year_month <= '2025-02'"
