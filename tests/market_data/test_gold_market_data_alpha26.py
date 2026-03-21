from __future__ import annotations

import pandas as pd
import pytest

from core import core as core_module
from core import delta_core as delta_core_module
from core.pipeline import DataPaths
from core.postgres import PostgresError
from tasks.market_data import gold_market_data as gold
from tasks.common.postgres_gold_sync import GoldSyncResult


def _silver_bucket_df(symbol: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-01-02")],
            "symbol": [symbol],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000.0],
        }
    )


def test_run_alpha26_market_gold_blocks_watermark_when_compute_fails(monkeypatch):
    watermarks: dict = {}
    captured_index: dict = {}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(
        gold.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame({"symbol": ["OLD"], "bucket": ["A"]}),
    )
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "system/gold-index/market/latest.parquet",
    )
    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", lambda *_args, **_kwargs: 123.0)
    monkeypatch.setattr(delta_core_module, "load_delta", lambda *_args, **_kwargs: _silver_bucket_df("AAPL"))
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("store_delta should not be called")),
    )
    monkeypatch.setattr(gold, "compute_features", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("boom")))

    (
        _processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
        bucket_results,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert failed == 1
    assert watermarks_dirty is False
    assert watermarks == {}
    assert index_path is None
    assert captured_index == {}
    assert len(bucket_results) == 1
    assert bucket_results[0].status == "failed_compute"
    assert bucket_results[0].watermark_updated is False


def test_run_alpha26_market_gold_updates_only_successful_bucket_watermarks(monkeypatch):
    watermarks: dict = {}
    captured_index: dict = {}
    written_paths: list[str] = []

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A", "B"])
    monkeypatch.setattr(
        gold.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame({"symbol": ["MSFT"], "bucket": ["B"]}),
    )
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "system/gold-index/market/latest.parquet",
    )

    def _fake_last_commit(_container: str, path: str):
        if path.endswith("/A"):
            return 100.0
        if path.endswith("/B"):
            return 200.0
        return None

    def _fake_load_delta(_container: str, path: str):
        if path.endswith("/A"):
            return _silver_bucket_df("AAPL")
        if path.endswith("/B"):
            return _silver_bucket_df("MSFT")
        return pd.DataFrame()

    def _fake_compute_features(df: pd.DataFrame) -> pd.DataFrame:
        symbol = str(df["symbol"].iloc[0]).strip().upper()
        if symbol == "MSFT":
            raise ValueError("compute failure")
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-01-02")],
                "symbol": [symbol],
                "close": [100.5],
            }
        )

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(delta_core_module, "load_delta", _fake_load_delta)
    monkeypatch.setattr(gold, "compute_features", _fake_compute_features)
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda _df, _container, path, **_kwargs: written_paths.append(str(path)),
    )

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
        bucket_results,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 1
    assert failed == 1
    assert watermarks_dirty is False
    assert index_path is None
    assert set(written_paths) == {"market/buckets/A"}
    assert watermarks == {}
    assert captured_index == {}
    assert sorted(result.status for result in bucket_results) == ["failed_compute", "ok"]


def test_run_alpha26_market_gold_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_gold_market_bucket_path("A")
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", lambda *_args, **_kwargs: None)

    def _fake_get_schema(_container: str, path: str):
        captured["checked_paths"].append(path)
        return None

    def _fake_store(*_args, **_kwargs):
        captured["store_calls"] = int(captured["store_calls"]) + 1

    monkeypatch.setattr(delta_core_module, "get_delta_schema_columns", _fake_get_schema)
    monkeypatch.setattr(delta_core_module, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
        bucket_results,
    ) = gold._run_alpha26_market_gold(
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
    assert len(bucket_results) == 1
    assert bucket_results[0].status == "skipped_empty_no_schema"


def test_run_alpha26_market_gold_processes_bucket_when_postgres_bootstrap_missing(monkeypatch):
    watermarks = {"bucket::A": {"silver_last_commit": 100.0}}
    captured_index: dict = {}
    written_paths: list[str] = []
    sync_calls: list[dict[str, object]] = []

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "system/gold-index/market/latest.parquet",
    )
    monkeypatch.setattr(gold, "resolve_postgres_dsn", lambda: "postgresql://test")
    monkeypatch.setattr(gold, "load_domain_sync_state", lambda *_args, **_kwargs: {})
    def _fake_last_commit(_container: str, path: str):
        if path == DataPaths.get_silver_market_bucket_path("A"):
            return 100.0
        return None

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "load_delta", lambda *_args, **_kwargs: _silver_bucket_df("AAPL"))
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda df: pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-01-02")],
                "symbol": [str(df["symbol"].iloc[0]).strip().upper()],
                "close": [100.5],
            }
        ),
    )
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda _df, _container, path, **_kwargs: written_paths.append(str(path)),
    )

    def _fake_sync_gold_bucket(**kwargs):
        sync_calls.append(kwargs)
        return GoldSyncResult(
            status="ok",
            domain="market",
            bucket="A",
            row_count=1,
            symbol_count=1,
            scope_symbol_count=1,
            source_commit=100.0,
            min_key=pd.Timestamp("2026-01-02").date(),
            max_key=pd.Timestamp("2026-01-02").date(),
        )

    monkeypatch.setattr(gold, "sync_gold_bucket", _fake_sync_gold_bucket)

    (
        processed,
        skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        _index_path,
        bucket_results,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 1
    assert skipped_unchanged == 0
    assert failed == 0
    assert watermarks_dirty is True
    assert written_paths == ["market/buckets/A"]
    assert len(sync_calls) == 1
    assert sync_calls[0]["bucket"] == "A"
    assert sync_calls[0]["scope_symbols"] == ["AAPL"]
    assert watermarks["bucket::A"]["silver_last_commit"] == 100.0
    assert captured_index["symbol_to_bucket"] == {"AAPL": "A"}
    assert bucket_results[0].status == "ok"


def test_run_alpha26_market_gold_blocks_watermark_when_postgres_sync_fails(monkeypatch):
    watermarks = {"bucket::A": {"silver_last_commit": 90.0}}
    written_paths: list[str] = []

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(gold, "resolve_postgres_dsn", lambda: "postgresql://test")
    monkeypatch.setattr(gold, "load_domain_sync_state", lambda *_args, **_kwargs: {})
    def _fake_last_commit(_container: str, path: str):
        if path == DataPaths.get_silver_market_bucket_path("A"):
            return 100.0
        return None

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "load_delta", lambda *_args, **_kwargs: _silver_bucket_df("AAPL"))
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda df: pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-01-02")],
                "symbol": [str(df["symbol"].iloc[0]).strip().upper()],
                "close": [100.5],
            }
        ),
    )
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda _df, _container, path, **_kwargs: written_paths.append(str(path)),
    )
    monkeypatch.setattr(
        gold,
        "sync_gold_bucket",
        lambda **_kwargs: (_ for _ in ()).throw(PostgresError("sync failed")),
    )

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
        bucket_results,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 0
    assert failed == 1
    assert watermarks_dirty is False
    assert index_path is None
    assert watermarks["bucket::A"]["silver_last_commit"] == 90.0
    assert written_paths == ["market/buckets/A"]
    assert bucket_results[0].status == "failed_write"


def test_run_alpha26_market_gold_aligns_empty_bucket_to_existing_schema(monkeypatch):
    target_path = DataPaths.get_gold_market_bucket_path("A")
    existing_cols = ["date", "symbol", "close", "return_1d"]
    captured: dict[str, object] = {"store_calls": 0}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core_module,
        "get_delta_schema_columns",
        lambda _container, path: existing_cols if path == target_path else None,
    )

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite", **_kwargs):
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["df"] = df.copy()
        captured["path"] = path
        captured["mode"] = mode

    monkeypatch.setattr(delta_core_module, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
        bucket_results,
    ) = gold._run_alpha26_market_gold(
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
    assert captured["df"].empty
    assert list(captured["df"].columns) == existing_cols
    assert len(bucket_results) == 1
    assert bucket_results[0].status == "ok"


def test_run_alpha26_market_gold_does_not_advance_watermark_for_empty_bucket_without_schema(monkeypatch):
    target_path = DataPaths.get_gold_market_bucket_path("A")
    watermarks: dict = {}
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", lambda *_args, **_kwargs: 123.0)
    monkeypatch.setattr(delta_core_module, "load_delta", lambda *_args, **_kwargs: _silver_bucket_df("AAPL"))
    monkeypatch.setattr(gold, "compute_features", lambda *_args, **_kwargs: pd.DataFrame(columns=["date", "symbol"]))

    def _fake_get_schema(_container: str, path: str):
        captured["checked_paths"].append(path)
        return None

    def _fake_store(*_args, **_kwargs):
        captured["store_calls"] = int(captured["store_calls"]) + 1

    monkeypatch.setattr(delta_core_module, "get_delta_schema_columns", _fake_get_schema)
    monkeypatch.setattr(delta_core_module, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
        bucket_results,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 0
    assert skipped_unchanged == 0
    assert skipped_missing_source == 0
    assert failed == 0
    assert watermarks_dirty is False
    assert watermarks == {}
    assert alpha26_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 0
    assert captured["checked_paths"] == [target_path]
    assert len(bucket_results) == 1
    assert bucket_results[0].status == "skipped_empty_no_schema"


def test_main_fails_closed_when_gold_reconciliation_fails(monkeypatch):
    monkeypatch.setattr(core_module, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(gold.layer_bucketing, "gold_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(gold, "get_backfill_range", lambda: (None, None))
    monkeypatch.setattr(gold, "load_watermarks", lambda _name: {})
    monkeypatch.setattr(
        gold,
        "_build_job_config",
        lambda: gold.FeatureJobConfig(
            silver_container="silver",
            gold_container="gold",
        ),
    )
    monkeypatch.setattr(
        gold,
        "_run_alpha26_market_gold",
        lambda **_kwargs: (1, 0, 0, 0, False, 1, "system/gold-index/market/latest.parquet", []),
    )
    monkeypatch.setattr(
        gold,
        "_run_market_reconciliation",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("reconciliation boom")),
    )
    monkeypatch.setattr(gold, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(core_module, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(core_module, "write_error", lambda *_args, **_kwargs: None)

    assert gold.main() == 1


def test_build_job_config_reads_required_containers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver")
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold")

    cfg = gold._build_job_config()

    assert cfg.silver_container == "silver"
    assert cfg.gold_container == "gold"


def test_build_job_config_requires_silver_container(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AZURE_CONTAINER_SILVER", raising=False)
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold")

    with pytest.raises(ValueError, match="AZURE_CONTAINER_SILVER"):
        gold._build_job_config()


def test_build_job_config_requires_gold_container(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver")
    monkeypatch.delenv("AZURE_CONTAINER_GOLD", raising=False)

    with pytest.raises(ValueError, match="AZURE_CONTAINER_GOLD"):
        gold._build_job_config()
