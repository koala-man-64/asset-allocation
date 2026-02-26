import pandas as pd

from core import delta_core
from tasks.finance_data import gold_finance_data


def test_gold_finance_process_ticker_writes_without_schema_merge(monkeypatch):
    base_df = pd.DataFrame(
        {
            "Date": ["01/01/2020"],
            "Symbol": ["AAPL"],
            "Total Revenue": ["10M"],
            "Net Income": [1.0],
            "Operating Cash Flow": [2.0],
            "Total Assets": [100.0],
        }
    )

    monkeypatch.setattr(delta_core, "load_delta", lambda *args, **kwargs: base_df)

    def fake_compute_features(_merged: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"b": [1], "a": [2], "extra": [3]})

    monkeypatch.setattr(gold_finance_data, "compute_features", fake_compute_features)

    captured: dict[str, object] = {}

    def fake_store_delta(df: pd.DataFrame, container: str, path: str, mode: str = "overwrite", **kwargs) -> None:
        captured["df"] = df.copy()
        captured["container"] = container
        captured["path"] = path
        captured["mode"] = mode
        captured["kwargs"] = dict(kwargs)

    monkeypatch.setattr(delta_core, "store_delta", fake_store_delta)

    result = gold_finance_data._process_ticker(
        (
            "AAPL",
            "finance/income",
            "finance/balance",
            "finance/cashflow",
            "finance/valuation",
            "finance/AAPL",
            "silver",
            "gold",
            None,
        )
    )

    assert result["status"] == "ok"
    assert captured["mode"] == "overwrite"
    assert (captured["kwargs"] or {}).get("schema_mode") is None
    assert list((captured["df"]).columns) == ["b", "a", "extra"]


def test_gold_finance_process_ticker_applies_backfill_start(monkeypatch):
    base_df = pd.DataFrame(
        {
            "Date": ["01/01/2020"],
            "Symbol": ["AAPL"],
            "Total Revenue": ["10M"],
            "Net Income": [1.0],
            "Operating Cash Flow": [2.0],
            "Total Assets": [100.0],
        }
    )
    monkeypatch.setattr(delta_core, "load_delta", lambda *args, **kwargs: base_df)

    def fake_compute_features(_merged: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2023-12-31"), pd.Timestamp("2024-01-10")],
                "symbol": ["AAPL", "AAPL"],
                "feature_x": [1.0, 2.0],
            }
        )

    monkeypatch.setattr(gold_finance_data, "compute_features", fake_compute_features)

    captured: dict[str, object] = {}

    def fake_store_delta(df: pd.DataFrame, container: str, path: str, mode: str = "overwrite", **kwargs) -> None:
        captured["df"] = df.copy()
        captured["container"] = container
        captured["path"] = path
        captured["mode"] = mode
        captured["kwargs"] = dict(kwargs)

    vacuum_calls = {"count": 0}

    monkeypatch.setattr(delta_core, "store_delta", fake_store_delta)
    monkeypatch.setattr(delta_core, "vacuum_delta_table", lambda *args, **kwargs: vacuum_calls.__setitem__("count", vacuum_calls["count"] + 1) or 0)

    result = gold_finance_data._process_ticker(
        (
            "AAPL",
            "finance/income",
            "finance/balance",
            "finance/cashflow",
            "finance/valuation",
            "finance/AAPL",
            "silver",
            "gold",
            "2024-01-01",
        )
    )

    assert result["status"] == "ok"
    assert captured["mode"] == "overwrite"
    assert pd.to_datetime((captured["df"])["date"]).min().date().isoformat() >= "2024-01-01"
    assert vacuum_calls["count"] == 1


def test_gold_finance_process_ticker_fails_when_required_source_missing(monkeypatch):
    base_df = pd.DataFrame(
        {
            "Date": ["01/01/2020"],
            "Symbol": ["AAPL"],
            "Total Revenue": ["10M"],
            "Net Income": [1.0],
            "Operating Cash Flow": [2.0],
            "Total Assets": [100.0],
        }
    )

    def fake_load_delta(_container: str, path: str):
        if "valuation" in path:
            return None
        return base_df

    monkeypatch.setattr(delta_core, "load_delta", fake_load_delta)

    result = gold_finance_data._process_ticker(
        (
            "AAPL",
            "finance/income",
            "finance/balance",
            "finance/cashflow",
            "finance/valuation",
            "finance/AAPL",
            "silver",
            "gold",
            None,
        )
    )

    assert result["status"] == "failed_source"
    assert "Missing required Silver source table for valuation" in (result.get("error") or "")
