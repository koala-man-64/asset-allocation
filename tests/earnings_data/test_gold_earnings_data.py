import pytest
import pandas as pd
from core import delta_core
from core.pipeline import DataPaths
from tasks.earnings_data import gold_earnings_data as gold

def test_compute_features():
    """
    Verifies compute_features:
    1. Snake-casing columns.
    2. Calculating surprise %.
    3. Resampling to daily with ffill.
    4. Identifying earnings days.
    """
    df_raw = pd.DataFrame({
        "Date": ["2023-01-01", "2023-04-01"],
        "Symbol": ["TEST", "TEST"],
        "Reported EPS": [1.1, 1.2],
        "EPS Estimate": [1.0, 1.0],
    })
    
    res = gold.compute_features(df_raw)
    
    # Check Result
    # 1. Columns snake_cased
    assert "reported_eps" in res.columns
    assert "surprise_pct" in res.columns
    
    # 2. Daily resampling
    # 2023-01-01 to 2023-04-01 is ~90 days
    assert len(res) >= 90 
    
    # 3. Validation of a specific date
    row_jan1 = res[res["date"] == pd.Timestamp("2023-01-01")].iloc[0]
    assert row_jan1["is_earnings_day"] == 1.0
    assert row_jan1["surprise_pct"] == (1.1 - 1.0) / 1.0 # 0.1
    
    row_jan2 = res[res["date"] == pd.Timestamp("2023-01-02")].iloc[0]
    assert row_jan2["is_earnings_day"] == 0.0
    # Should be ffilled
    assert row_jan2["reported_eps"] == 1.1

def test_compute_features_missing_cols():
    df_raw = pd.DataFrame({"Date": []})
    with pytest.raises(ValueError, match="Missing required columns"):
        gold.compute_features(df_raw)


def test_run_alpha26_earnings_gold_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_gold_earnings_bucket_path("A")
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
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
    ) = gold._run_alpha26_earnings_gold(
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


def test_run_alpha26_earnings_gold_writes_empty_bucket_when_schema_exists(monkeypatch):
    target_path = DataPaths.get_gold_earnings_bucket_path("A")
    existing_cols = ["date", "symbol", "surprise"]
    captured: dict[str, object] = {"store_calls": 0}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core, "get_delta_last_commit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "get_delta_schema_columns",
        lambda _container, path: existing_cols if path == target_path else None,
    )

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
    ) = gold._run_alpha26_earnings_gold(
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
    assert list(df_written.columns) == existing_cols
