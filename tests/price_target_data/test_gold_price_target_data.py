import pandas as pd

from core import delta_core
from core.pipeline import DataPaths
from tasks.price_target_data import gold_price_target_data as gold
from tasks.common.gold_output_contracts import GOLD_PRICE_TARGET_OUTPUT_COLUMNS


def test_run_alpha26_price_target_gold_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_gold_price_targets_bucket_path("A")
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
    ) = gold._run_alpha26_price_target_gold(
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


def test_run_alpha26_price_target_gold_writes_empty_bucket_when_schema_exists(monkeypatch):
    target_path = DataPaths.get_gold_price_targets_bucket_path("A")
    existing_cols = ["obs_date", "symbol", "tp_mean_est"]
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
    ) = gold._run_alpha26_price_target_gold(
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


def test_run_alpha26_price_target_gold_projects_contract_before_write(monkeypatch):
    target_path = DataPaths.get_gold_price_targets_bucket_path("A")
    captured: dict[str, object] = {}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if path == DataPaths.get_silver_price_target_bucket_path("A") else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "load_delta",
        lambda _container, path: pd.DataFrame(
            {
                "obs_date": [pd.Timestamp("2026-02-14")],
                "symbol": ["NVDA"],
            }
        )
        if path == DataPaths.get_silver_price_target_bucket_path("A")
        else pd.DataFrame(),
    )
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda _df: pd.DataFrame(
            {
                "obs_date": [pd.Timestamp("2026-02-14")],
                "symbol": ["nvda"],
                "tp_mean_est": [220.5],
                "tp_cnt_est": [17],
                "extra_metric": [99],
            }
        ),
    )

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite", **_kwargs) -> None:
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
    ) = gold._run_alpha26_price_target_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks={},
    )

    assert processed == 1
    assert skipped_unchanged == 0
    assert skipped_missing_source == 0
    assert failed == 0
    assert watermarks_dirty is True
    assert alpha26_symbols == 1
    assert index_path == "index"
    assert captured["path"] == target_path
    assert captured["mode"] == "overwrite"
    assert list(captured["df"].columns) == list(GOLD_PRICE_TARGET_OUTPUT_COLUMNS)
    assert "extra_metric" not in captured["df"].columns
    assert captured["df"].loc[0, "symbol"] == "NVDA"


def test_run_alpha26_price_target_gold_blocks_publication_when_bucket_fails(monkeypatch):
    index_calls = {"count": 0}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **_kwargs: index_calls.__setitem__("count", int(index_calls["count"]) + 1) or "index",
    )
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if path == DataPaths.get_silver_price_target_bucket_path("A") else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "load_delta",
        lambda _container, path: pd.DataFrame(
            {
                "obs_date": [pd.Timestamp("2026-02-14")],
                "symbol": ["NVDA"],
            }
        )
        if path == DataPaths.get_silver_price_target_bucket_path("A")
        else pd.DataFrame(),
    )
    monkeypatch.setattr(gold, "compute_features", lambda _df: (_ for _ in ()).throw(ValueError("boom")))
    monkeypatch.setattr(delta_core, "store_delta", lambda *_args, **_kwargs: None)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_price_target_gold(
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
    assert index_path is None
    assert index_calls["count"] == 0
