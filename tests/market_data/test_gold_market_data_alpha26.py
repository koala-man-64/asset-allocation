from __future__ import annotations

import pandas as pd

from core import core as core_module
from core import delta_core as delta_core_module
from tasks.market_data import gold_market_data as gold


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
    monkeypatch.setattr(gold.layer_bucketing, "gold_alpha26_force_rebuild", lambda: False)
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
        _index_path,
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
    assert captured_index["symbol_to_bucket"] == {"OLD": "A"}
    assert len(bucket_results) == 1
    assert bucket_results[0].status == "failed_compute"
    assert bucket_results[0].watermark_updated is False


def test_run_alpha26_market_gold_updates_only_successful_bucket_watermarks(monkeypatch):
    watermarks: dict = {}
    captured_index: dict = {}
    written_paths: list[str] = []

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A", "B"])
    monkeypatch.setattr(gold.layer_bucketing, "gold_alpha26_force_rebuild", lambda: False)
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
        _index_path,
        bucket_results,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 1
    assert failed == 1
    assert watermarks_dirty is True
    assert set(written_paths) == {"market/buckets/A"}
    assert set(watermarks.keys()) == {"bucket::A"}
    assert watermarks["bucket::A"]["silver_last_commit"] == 100.0
    assert captured_index["symbol_to_bucket"] == {"AAPL": "A", "MSFT": "B"}
    assert sorted(result.status for result in bucket_results) == ["failed_compute", "ok"]


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
            max_workers=1,
            tickers=[],
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
