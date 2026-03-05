import pandas as pd
import pytest

from core import delta_core
from core.pipeline import DataPaths
from tasks.finance_data import gold_finance_data


def test_build_job_config_reads_required_containers(monkeypatch):
    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver")
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold")
    cfg = gold_finance_data._build_job_config()
    assert cfg.silver_container == "silver"
    assert cfg.gold_container == "gold"


def test_build_job_config_requires_containers(monkeypatch):
    monkeypatch.delenv("AZURE_CONTAINER_SILVER", raising=False)
    monkeypatch.delenv("AZURE_CONTAINER_GOLD", raising=False)
    with pytest.raises(ValueError, match="AZURE_CONTAINER_SILVER"):
        gold_finance_data._build_job_config()


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


def test_run_alpha26_finance_gold_preflight_blocks_nonrecoverable_schema_drift(monkeypatch):
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "gold_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core, "get_delta_last_commit", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(delta_core, "store_delta", lambda *_args, **_kwargs: None)

    date_value = pd.Timestamp("2024-01-01")
    ticker = "AAPL"

    income_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "Total Revenue": [100.0],
            "Gross Profit": [40.0],
            "Operating Income": [20.0],
            "Net Income": [10.0],
        }
    )
    balance_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "Total Debt": [300.0],
            "Long Term Debt": [250.0],
            "Total Assets": [1_000.0],
            "Current Assets": [500.0],
            "Current Liabilities": [250.0],
        }
    )
    cashflow_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "Operating Cash Flow": [25.0],
            # Intentionally omit both Free Cash Flow and Capital Expenditures to force non-recoverable drift.
        }
    )
    valuation_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "Shares Outstanding": [100.0],
            "PE Ratio": [20.0],
            "EV/EBITDA": [10.0],
            "Market Cap": [1_000_000.0],
            "EBITDA": [30.0],
            "Forward P/E": [18.0],
            "EV/Revenue": [5.0],
            "Cash And Cash Equivalents": [100.0],
        }
    )

    def _fake_load_delta(_container: str, path: str):
        if "income_statement" in path:
            return income_df
        if "balance_sheet" in path:
            return balance_df
        if "cash_flow" in path:
            return cashflow_df
        if "valuation" in path:
            return valuation_df
        return pd.DataFrame()

    monkeypatch.setattr(delta_core, "load_delta", _fake_load_delta)
    compute_calls = {"count": 0}

    def _unexpected_compute(_merged: pd.DataFrame) -> pd.DataFrame:
        compute_calls["count"] += 1
        return pd.DataFrame({"date": [], "symbol": []})

    monkeypatch.setattr(gold_finance_data, "compute_features", _unexpected_compute)

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
    assert skipped_missing_source == 0
    assert failed == 1
    assert watermarks_dirty is False
    assert alpha26_symbols == 0
    assert index_path == "index"
    assert compute_calls["count"] == 0
