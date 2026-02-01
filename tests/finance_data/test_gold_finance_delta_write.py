import pandas as pd

from core import delta_core
from tasks.finance_data import gold_finance_data


def test_gold_finance_process_ticker_merges_delta_schema(monkeypatch):
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
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda _container, _path: ["a", "b"])

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
        )
    )

    assert result["status"] == "ok"
    assert captured["mode"] == "overwrite"
    assert (captured["kwargs"] or {}).get("schema_mode") == "merge"
    assert list((captured["df"]).columns) == ["a", "b", "extra"]

