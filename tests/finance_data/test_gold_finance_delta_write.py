import pandas as pd

from core import delta_core
from core.pipeline import DataPaths
from tasks.finance_data import gold_finance_data


def test_gold_finance_process_ticker_writes_overwrite(monkeypatch):
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


def test_gold_finance_process_ticker_sanitizes_index_artifacts_before_delta_write(monkeypatch, tmp_path):
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
        out = pd.DataFrame(
            {
                "date": [pd.Timestamp("2024-01-10")],
                "symbol": ["AAPL"],
                "feature_x": [2.0],
                "__index_level_0__": [9],
            }
        )
        out.index = pd.Index([5])
        return out

    monkeypatch.setattr(gold_finance_data, "compute_features", fake_compute_features)
    monkeypatch.setattr(delta_core, "_ensure_container_exists", lambda _container: None)
    monkeypatch.setattr(delta_core, "get_delta_table_uri", lambda _container, _path: str(tmp_path / "gold_finance"))
    monkeypatch.setattr(delta_core, "get_delta_storage_options", lambda _container=None: {})
    monkeypatch.setattr(delta_core, "_get_existing_delta_schema_columns", lambda _uri, _opts: None)

    captured: dict[str, object] = {}

    def fake_write_deltalake(_uri, df: pd.DataFrame, **kwargs) -> None:
        captured["df"] = df.copy()
        captured["kwargs"] = dict(kwargs)

    monkeypatch.setattr(delta_core, "write_deltalake", fake_write_deltalake)

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
    assert "__index_level_0__" not in captured["df"].columns
    assert isinstance(captured["df"].index, pd.RangeIndex)
    assert captured["df"].index.start == 0
    assert captured["df"].index.step == 1


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


def test_run_alpha26_finance_gold_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_gold_finance_bucket_path("A")
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(gold_finance_data.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "gold_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core, "get_delta_last_commit", lambda *_args, **_kwargs: None)

    def _fake_get_schema(_container: str, path: str):
        captured["checked_paths"].append(path)
        return None

    def _fake_store(_df: pd.DataFrame, _container: str, _path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["mode"] = mode

    monkeypatch.setattr(delta_core, "get_delta_schema_columns", _fake_get_schema)
    monkeypatch.setattr(delta_core, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold_finance_data._run_alpha26_finance_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks={},
    )

    assert processed == 0
    assert skipped_unchanged == 0
    assert skipped_missing_source == 1
    assert failed == 0
    assert watermarks_dirty is False
    assert alpha26_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 0
    assert captured["checked_paths"] == [target_path]


def test_run_alpha26_finance_gold_writes_empty_bucket_when_schema_exists(monkeypatch):
    target_path = DataPaths.get_gold_finance_bucket_path("A")
    existing_cols = ["date", "symbol", "feature_x"]
    captured: dict[str, object] = {"store_calls": 0}

    monkeypatch.setattr(gold_finance_data.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "gold_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core, "get_delta_last_commit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda _container, path: existing_cols if path == target_path else None)

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["df"] = df.copy()
        captured["path"] = path
        captured["mode"] = mode

    monkeypatch.setattr(delta_core, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold_finance_data._run_alpha26_finance_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks={},
    )

    assert processed == 1
    assert skipped_unchanged == 0
    assert skipped_missing_source == 1
    assert failed == 0
    assert watermarks_dirty is False
    assert alpha26_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 1
    assert captured["path"] == target_path
    assert captured["mode"] == "overwrite"
    df_written = captured["df"]
    assert isinstance(df_written, pd.DataFrame)
    assert df_written.empty
    assert list(df_written.columns) == ["date", "symbol"]
