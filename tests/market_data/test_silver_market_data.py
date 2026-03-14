import pytest
import uuid
import pandas as pd
from unittest.mock import patch

from tasks.market_data import silver_market_data as silver
from core import delta_core
from core import config as cfg
from core.pipeline import DataPaths

@pytest.fixture
def unique_ticker():
    return f"TEST_MKT_{uuid.uuid4().hex[:8].upper()}"


def _market_bucket_blob_name(symbol: str) -> str:
    return f"market-data/buckets/{symbol[0]}.parquet"


def _market_bucket_bytes(rows: list[dict[str, object]]) -> bytes:
    return pd.DataFrame(rows).to_parquet(index=False)


def test_silver_processing(unique_ticker):
    """
     Verifies Silver Processing:
     1. Mocks reading a Bronze alpha26 bucket.
     2. Calls silver.process_file.
     3. Verifies Delta Write to Silver.
    """
    symbol = unique_ticker
    blob_name = _market_bucket_blob_name(symbol)
    parquet_content = _market_bucket_bytes(
        [
            {
                "symbol": symbol,
                "date": "2023-01-01",
                "open": 100.0,
                "high": 105.0,
                "low": 95.0,
                "close": 102.0,
                "volume": 1000.0,
            }
        ]
    )

    with patch('core.core.read_raw_bytes') as mock_read, \
         patch('core.delta_core.store_delta') as mock_store_delta, \
         patch('core.delta_core.load_delta') as mock_load_delta:
        mock_read.return_value = parquet_content
        mock_load_delta.return_value = None

        silver.process_file(blob_name)

        mock_read.assert_called_with(blob_name, client=silver.bronze_client)
        mock_store_delta.assert_called_once()
        args, kwargs = mock_store_delta.call_args
        df_saved = args[0]
        container = args[1]
        path = args[2]
        
        assert container == cfg.AZURE_CONTAINER_SILVER
        assert path == DataPaths.get_silver_market_bucket_path(symbol[0])
        assert len(df_saved) == 1
        assert df_saved.iloc[0]["close"] == 102


def test_silver_processing_accepts_parseable_date_strings(unique_ticker):
    symbol = unique_ticker
    blob_name = _market_bucket_blob_name(symbol)
    parquet_content = _market_bucket_bytes(
        [
            {
                "symbol": symbol,
                "date": "2024-01-03T00:00:00Z",
                "open": 10.5,
                "high": 12.0,
                "low": 10.0,
                "close": 11.0,
                "volume": 150.0,
            }
        ]
    )

    with patch("core.core.read_raw_bytes") as mock_read, patch(
        "core.delta_core.store_delta"
    ) as mock_store_delta, patch("core.delta_core.load_delta") as mock_load_delta:
        mock_read.return_value = parquet_content
        mock_load_delta.return_value = None

        silver.process_file(blob_name)

        mock_store_delta.assert_called_once()


def test_silver_processing_rounds_price_columns_with_half_up(unique_ticker):
    symbol = unique_ticker
    blob_name = _market_bucket_blob_name(symbol)
    parquet_content = _market_bucket_bytes(
        [
            {
                "symbol": symbol,
                "date": "2024-01-03",
                "open": 1.005,
                "high": 2.115,
                "low": 0.995,
                "close": 1.115,
                "volume": 100.0,
            }
        ]
    )

    with patch("core.core.read_raw_bytes", return_value=parquet_content), patch(
        "core.delta_core.store_delta"
    ) as mock_store_delta, patch("core.delta_core.load_delta", return_value=None):
        silver.process_file(blob_name)

        df_saved = mock_store_delta.call_args.args[0]
        row = df_saved.iloc[0]
        assert row["open"] == pytest.approx(1.01)
        assert row["high"] == pytest.approx(2.12)
        assert row["low"] == pytest.approx(1.00)
        assert row["close"] == pytest.approx(1.12)


def test_silver_processing_includes_supplemental_market_metrics(unique_ticker):
    symbol = unique_ticker
    blob_name = _market_bucket_blob_name(symbol)
    parquet_content = _market_bucket_bytes(
        [
            {
                "symbol": symbol,
                "date": "2024-01-03",
                "open": 10.5,
                "high": 12.0,
                "low": 10.0,
                "close": 11.0,
                "volume": 150.0,
                "short_interest": 1200.0,
                "short_volume": 500.0,
            }
        ]
    )

    with patch("core.core.read_raw_bytes") as mock_read, patch(
        "core.delta_core.store_delta"
    ) as mock_store_delta, patch("core.delta_core.load_delta") as mock_load_delta:
        mock_read.return_value = parquet_content
        mock_load_delta.return_value = None

        silver.process_file(blob_name)

        mock_store_delta.assert_called_once()
        args, _ = mock_store_delta.call_args
        df_saved = args[0]

        assert "short_interest" in df_saved.columns
        assert "short_volume" in df_saved.columns
        assert float(df_saved.iloc[0]["short_interest"]) == pytest.approx(1200.0)
        assert float(df_saved.iloc[0]["short_volume"]) == pytest.approx(500.0)


def test_silver_processing_merges_history_symbol_without_duplicate_symbol_columns(unique_ticker):
    symbol = unique_ticker
    blob_name = _market_bucket_blob_name(symbol)
    parquet_content = _market_bucket_bytes(
        [
            {
                "symbol": symbol,
                "date": "2024-01-03",
                "open": 10.5,
                "high": 12.0,
                "low": 10.0,
                "close": 11.0,
                "volume": 150.0,
            }
        ]
    )
    history = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-02"),
                "open": 10.0,
                "high": 11.5,
                "low": 9.8,
                "close": 10.8,
                "volume": 125.0,
                "symbol": symbol,
                "short_interest": 1000.0,
                "short_volume": 500.0,
            }
        ]
    )

    with patch("core.core.read_raw_bytes", return_value=parquet_content), patch(
        "core.delta_core.store_delta"
    ) as mock_store, patch(
        "core.delta_core.load_delta", return_value=history
    ):
        assert silver.process_file(blob_name) is True

        df_saved = mock_store.call_args[0][0]
        assert "symbol_2" not in df_saved.columns
        assert "symbol" in df_saved.columns
        assert set(df_saved["symbol"].dropna().astype(str).unique()) == {symbol}


def test_silver_processing_repairs_duplicate_symbol_suffix_columns(unique_ticker):
    symbol = unique_ticker
    blob_name = _market_bucket_blob_name(symbol)
    parquet_content = _market_bucket_bytes(
        [
            {
                "symbol": symbol,
                "date": "2024-01-03",
                "open": 10.5,
                "high": 12.0,
                "low": 10.0,
                "close": 11.0,
                "volume": 150.0,
            }
        ]
    )
    history = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-02"),
                "open": 10.0,
                "high": 11.5,
                "low": 9.8,
                "close": 10.8,
                "volume": 125.0,
                "symbol_2": symbol,
            }
        ]
    )

    with patch("core.core.read_raw_bytes", return_value=parquet_content), patch(
        "core.delta_core.store_delta"
    ) as mock_store, patch(
        "core.delta_core.load_delta", return_value=history
    ):
        assert silver.process_file(blob_name) is True

        df_saved = mock_store.call_args[0][0]
        assert "symbol_2" not in df_saved.columns
        assert "symbol" in df_saved.columns
        assert set(df_saved["symbol"].dropna().astype(str).unique()) == {symbol}


def test_silver_processing_drops_index_artifact_columns(unique_ticker):
    symbol = unique_ticker
    blob_name = _market_bucket_blob_name(symbol)
    parquet_content = _market_bucket_bytes(
        [
            {
                "symbol": symbol,
                "date": "2024-01-03",
                "open": 10.5,
                "high": 12.0,
                "low": 10.0,
                "close": 11.0,
                "volume": 150.0,
            }
        ]
    )
    history = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-02"),
                "open": 10.0,
                "high": 11.5,
                "low": 9.8,
                "close": 10.8,
                "volume": 125.0,
                "symbol": symbol,
                "__index_level_0__": 42,
                "Unnamed: 0": 7,
                "index": 3,
            }
        ]
    )

    with patch("core.core.read_raw_bytes", return_value=parquet_content), patch(
        "core.delta_core.store_delta"
    ) as mock_store, patch(
        "core.delta_core.load_delta", return_value=history
    ):
        assert silver.process_file(blob_name) is True

        df_saved = mock_store.call_args[0][0]
        assert "index" not in df_saved.columns
        assert "level_0" not in df_saved.columns
        assert "index_level_0" not in df_saved.columns
        assert "unnamed_0" not in df_saved.columns


def test_silver_processing_applies_backfill_start_cutoff(unique_ticker):
    symbol = unique_ticker
    blob_name = _market_bucket_blob_name(symbol)
    parquet_content = _market_bucket_bytes(
        [
            {
                "symbol": symbol,
                "date": "2023-12-31",
                "open": 100.0,
                "high": 105.0,
                "low": 95.0,
                "close": 102.0,
                "volume": 1000.0,
            },
            {
                "symbol": symbol,
                "date": "2024-01-03",
                "open": 101.0,
                "high": 106.0,
                "low": 96.0,
                "close": 103.0,
                "volume": 1100.0,
            },
        ]
    )
    history = pd.DataFrame(
        [
            {"Date": pd.Timestamp("2023-12-30"), "Open": 99, "High": 104, "Low": 94, "Close": 101, "Volume": 900, "Symbol": symbol}
        ]
    )

    with patch("core.core.read_raw_bytes", return_value=parquet_content), patch(
        "core.delta_core.store_delta"
    ) as mock_store, patch(
        "core.delta_core.load_delta", return_value=history
    ), patch(
        "tasks.market_data.silver_market_data.get_backfill_range",
        return_value=(pd.Timestamp("2024-01-01"), None),
    ), patch(
        "core.delta_core.vacuum_delta_table", return_value=0
    ):
        assert silver.process_file(blob_name) is True
        df_saved = mock_store.call_args[0][0]
        assert pd.to_datetime(df_saved["date"]).min().date().isoformat() >= "2024-01-01"


def test_run_market_reconciliation_cutoff_store_path_sanitizes_index_artifacts(monkeypatch, tmp_path):
    class _FakeSilverClient:
        def delete_prefix(self, _path: str) -> int:
            return 0

    fake_client = _FakeSilverClient()
    monkeypatch.setattr(silver, "silver_client", fake_client)
    monkeypatch.setattr(silver, "collect_delta_market_symbols", lambda *, client, root_prefix: {"AAPL"})
    monkeypatch.setattr(silver, "get_backfill_range", lambda: (pd.Timestamp("2016-01-01"), None))

    monkeypatch.setattr(delta_core, "_ensure_container_exists", lambda _container: None)
    monkeypatch.setattr(delta_core, "get_delta_table_uri", lambda _container, _path: str(tmp_path / "silver_market"))
    monkeypatch.setattr(delta_core, "get_delta_storage_options", lambda _container=None: {})
    monkeypatch.setattr(delta_core, "_get_existing_delta_schema_columns", lambda _uri, _opts: None)

    captured: dict = {}

    def fake_write_deltalake(_uri, df: pd.DataFrame, **kwargs):
        captured["df"] = df.copy()
        captured["kwargs"] = dict(kwargs)

    monkeypatch.setattr(delta_core, "write_deltalake", fake_write_deltalake)

    def _fake_enforce_backfill_cutoff_on_bucket_tables(**kwargs):
        dirty = pd.DataFrame(
            {
                "date": [pd.Timestamp("2024-01-10")],
                "symbol": ["AAPL"],
                "close": [101.0],
                "__index_level_0__": [8],
            }
        )
        dirty.index = pd.Index([12])
        kwargs["store_table"](dirty, DataPaths.get_silver_market_bucket_path("A"))
        return type(
            "_Stats",
            (),
            {"tables_scanned": 1, "tables_rewritten": 1, "deleted_blobs": 0, "rows_dropped": 1, "errors": 0},
        )()

    monkeypatch.setattr(
        silver,
        "enforce_backfill_cutoff_on_bucket_tables",
        _fake_enforce_backfill_cutoff_on_bucket_tables,
    )

    silver._run_market_reconciliation(bronze_blob_list=[{"name": "market-data/buckets/A.parquet"}])

    assert "__index_level_0__" not in captured["df"].columns
    assert isinstance(captured["df"].index, pd.RangeIndex)
    assert captured["df"].index.start == 0
    assert captured["df"].index.step == 1


def test_main_skips_alpha26_write_when_no_market_data(monkeypatch):
    class _FakeBronzeClient:
        def list_blob_infos(self, *, name_starts_with: str):
            assert name_starts_with == "market-data/"
            return []

    messages: list[str] = []
    saved_last_success: dict = {}

    def _save_last_success(_name: str, metadata=None):
        if metadata:
            saved_last_success.update(metadata)

    monkeypatch.setattr(silver, "bronze_client", _FakeBronzeClient())
    monkeypatch.setattr(silver, "load_watermarks", lambda _name: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _name: None)
    monkeypatch.setattr(silver, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver, "save_last_success", _save_last_success)
    monkeypatch.setattr(silver.bronze_bucketing, "bronze_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(silver, "_detect_missing_alpha26_market_buckets", lambda: (False, set()))
    monkeypatch.setattr(silver, "_run_market_reconciliation", lambda *, bronze_blob_list: (0, 0))
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(silver.mdc, "write_error", lambda msg: messages.append(f"ERROR:{msg}"))

    def _unexpected_write(_frames, *, touched_buckets=None):
        del touched_buckets
        raise AssertionError("_write_alpha26_market_buckets should not be called when no rows are staged.")

    monkeypatch.setattr(silver, "_write_alpha26_market_buckets", _unexpected_write)

    assert silver.main() == 0
    assert any("alpha26 bucket write skipped: no staged rows" in msg for msg in messages)
    assert saved_last_success.get("processed") == 0
    assert saved_last_success.get("alpha26_staged_rows") == 0
    assert saved_last_success.get("alpha26_symbols") == 0
    assert saved_last_success.get("column_count") == len(silver._ALPHA26_MARKET_MIN_COLUMNS)


def test_main_skips_alpha26_write_when_candidates_produce_no_staged_rows(monkeypatch):
    blob = {"name": "market-data/buckets/A.parquet"}

    class _FakeBronzeClient:
        def list_blob_infos(self, *, name_starts_with: str):
            assert name_starts_with == "market-data/"
            return [dict(blob)]

    messages: list[str] = []
    saved_last_success: dict = {}

    def _save_last_success(_name: str, metadata=None):
        if metadata:
            saved_last_success.update(metadata)

    def _fake_process_alpha26_bucket_blob(
        _blob,
        *,
        watermarks,
        include_history=False,
        persist=False,
        force_reprocess=False,
        alpha26_bucket_frames=None,
    ):
        del watermarks, include_history, persist, alpha26_bucket_frames
        assert force_reprocess is False
        return "ok"

    monkeypatch.setattr(silver, "bronze_client", _FakeBronzeClient())
    monkeypatch.setattr(silver, "load_watermarks", lambda _name: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _name: None)
    monkeypatch.setattr(silver, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver, "save_last_success", _save_last_success)
    monkeypatch.setattr(silver, "should_process_blob_since_last_success", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(silver, "process_alpha26_bucket_blob", _fake_process_alpha26_bucket_blob)
    monkeypatch.setattr(silver, "_detect_missing_alpha26_market_buckets", lambda: (False, set()))
    monkeypatch.setattr(silver.bronze_bucketing, "bronze_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver, "_run_market_reconciliation", lambda *, bronze_blob_list: (0, 0))
    monkeypatch.setattr(silver.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(silver.mdc, "write_error", lambda msg: messages.append(f"ERROR:{msg}"))

    def _unexpected_write(_frames, *, touched_buckets=None):
        del touched_buckets
        raise AssertionError(
            "_write_alpha26_market_buckets should not be called when candidates do not stage any rows."
        )

    monkeypatch.setattr(silver, "_write_alpha26_market_buckets", _unexpected_write)

    assert silver.main() == 0
    assert any("alpha26 bucket write skipped: no staged rows" in msg for msg in messages)
    assert saved_last_success.get("processed") == 1
    assert saved_last_success.get("alpha26_staged_rows") == 0
    assert saved_last_success.get("alpha26_symbols") == 0
    assert saved_last_success.get("column_count") == len(silver._ALPHA26_MARKET_MIN_COLUMNS)


def test_main_bootstraps_alpha26_write_when_silver_buckets_missing(monkeypatch):
    blob = {"name": "market-data/buckets/A.parquet"}

    class _FakeBronzeClient:
        def list_blob_infos(self, *, name_starts_with: str):
            assert name_starts_with == "market-data/"
            return [dict(blob)]

    messages: list[str] = []
    saved_last_success: dict = {}
    write_calls = {"count": 0}

    def _save_last_success(_name: str, metadata=None):
        if metadata:
            saved_last_success.update(metadata)

    def _fake_process_alpha26_bucket_blob(
        _blob,
        *,
        watermarks,
        include_history=False,
        persist=False,
        force_reprocess=False,
        alpha26_bucket_frames=None,
    ):
        del watermarks, include_history, persist
        assert force_reprocess is True
        assert alpha26_bucket_frames is not None
        alpha26_bucket_frames.setdefault("A", []).append(
            pd.DataFrame({"symbol": ["AAPL"], "date": [pd.Timestamp("2025-01-02")]})
        )
        return "ok"

    def _fake_write(frames, *, touched_buckets=None):
        write_calls["count"] += 1
        assert "A" in frames
        assert touched_buckets == {"A"}
        return 1, "system/silver-index/market/latest.parquet", 9

    monkeypatch.setattr(silver, "bronze_client", _FakeBronzeClient())
    monkeypatch.setattr(silver, "load_watermarks", lambda _name: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _name: None)
    monkeypatch.setattr(silver, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver, "save_last_success", _save_last_success)
    monkeypatch.setattr(silver, "should_process_blob_since_last_success", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(silver, "process_alpha26_bucket_blob", _fake_process_alpha26_bucket_blob)
    monkeypatch.setattr(silver, "_write_alpha26_market_buckets", _fake_write)
    monkeypatch.setattr(silver, "_detect_missing_alpha26_market_buckets", lambda: (True, {"A"}))
    monkeypatch.setattr(silver.bronze_bucketing, "bronze_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(silver, "_run_market_reconciliation", lambda *, bronze_blob_list: (0, 0))
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(silver.mdc, "write_error", lambda msg: messages.append(f"ERROR:{msg}"))
    monkeypatch.setattr(silver.mdc, "write_warning", lambda msg: messages.append(f"WARN:{msg}"))

    assert silver.main() == 0
    assert write_calls["count"] == 1
    assert any("bootstrap required" in msg for msg in messages)
    assert saved_last_success.get("processed") == 1
    assert saved_last_success.get("alpha26_staged_rows") == 1
    assert saved_last_success.get("alpha26_symbols") == 1
    assert saved_last_success.get("column_count") == 9


def test_write_alpha26_market_buckets_enforces_typed_schema_for_empty_buckets(monkeypatch):
    captured: dict[str, pd.DataFrame] = {}

    def _fake_store_delta(df, _container, path, mode="overwrite"):
        assert mode == "overwrite"
        captured[str(path)] = df.copy()

    monkeypatch.setattr(delta_core, "store_delta", _fake_store_delta)
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ["A", "C"])
    monkeypatch.setattr(
        silver.layer_bucketing,
        "write_layer_symbol_index",
        lambda **_kwargs: "system/silver-index/market/latest.parquet",
    )

    bucket_frames = {
        "A": [
            pd.DataFrame(
                {
                    "date": [pd.Timestamp("2026-01-02")],
                    "symbol": ["aapl"],
                    "open": [100.0],
                    "high": [102.0],
                    "low": [99.0],
                    "close": [101.0],
                    "volume": [1000],
                    "short_interest": [pd.NA],
                    "short_volume": [pd.NA],
                    "unexpected": [None],
                }
            )
        ]
    }

    symbol_count, index_path, _column_count = silver._write_alpha26_market_buckets(bucket_frames)

    assert symbol_count == 1
    assert index_path == "system/silver-index/market/latest.parquet"

    path_a = DataPaths.get_silver_market_bucket_path("A")
    path_c = DataPaths.get_silver_market_bucket_path("C")
    assert set(captured.keys()) == {path_a, path_c}

    for path in (path_a, path_c):
        frame = captured[path]
        assert list(frame.columns) == silver._ALPHA26_MARKET_MIN_COLUMNS
        assert str(frame.dtypes["date"]).startswith("datetime64")
        assert str(frame.dtypes["symbol"]).startswith("string")
        for col in silver._ALPHA26_MARKET_NUMERIC_COLUMNS:
            assert pd.api.types.is_numeric_dtype(frame.dtypes[col])
        assert all(str(dtype) != "object" for dtype in frame.dtypes.tolist())

    assert captured[path_a]["symbol"].tolist() == ["AAPL"]
    assert "unexpected" not in captured[path_a].columns


def test_write_alpha26_market_buckets_partial_update_preserves_untouched_symbol_index(monkeypatch):
    captured_paths: list[str] = []
    captured_index: dict = {}

    def _fake_store_delta(df, _container, path, mode="overwrite"):
        del df
        assert mode == "overwrite"
        captured_paths.append(str(path))

    monkeypatch.setattr(delta_core, "store_delta", _fake_store_delta)
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ["A", "C"])
    monkeypatch.setattr(
        silver.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame(
            {
                "symbol": ["AAPL", "CSCO"],
                "bucket": ["A", "C"],
            }
        ),
    )
    monkeypatch.setattr(
        silver.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "system/silver-index/market/latest.parquet",
    )

    bucket_frames = {
        "A": [
            pd.DataFrame(
                {
                    "date": [pd.Timestamp("2026-01-02")],
                    "symbol": ["amzn"],
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.5],
                    "volume": [1000.0],
                    "short_interest": [pd.NA],
                    "short_volume": [pd.NA],
                }
            )
        ]
    }

    symbol_count, index_path, _column_count = silver._write_alpha26_market_buckets(
        bucket_frames,
        touched_buckets={"A"},
    )

    assert index_path == "system/silver-index/market/latest.parquet"
    assert captured_paths == [DataPaths.get_silver_market_bucket_path("A")]
    assert symbol_count == 2
    assert captured_index["symbol_to_bucket"] == {"AMZN": "A", "CSCO": "C"}


def test_write_alpha26_market_buckets_partial_update_fails_closed_without_prior_index(monkeypatch):
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ["A", "C"])
    monkeypatch.setattr(
        silver.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame(columns=["symbol", "bucket"]),
    )

    bucket_frames = {
        "A": [
            pd.DataFrame(
                {
                    "date": [pd.Timestamp("2026-01-02")],
                    "symbol": ["amzn"],
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.5],
                    "volume": [1000.0],
                    "short_interest": [pd.NA],
                    "short_volume": [pd.NA],
                }
            )
        ]
    }

    with pytest.raises(RuntimeError, match="incremental alpha26 write blocked"):
        silver._write_alpha26_market_buckets(bucket_frames, touched_buckets={"A"})


def test_main_fails_closed_when_market_reconciliation_fails(monkeypatch):
    class _FakeBronzeClient:
        def list_blob_infos(self, *, name_starts_with: str):
            assert name_starts_with == "market-data/"
            return []

    monkeypatch.setattr(silver, "bronze_client", _FakeBronzeClient())
    monkeypatch.setattr(silver, "load_watermarks", lambda _name: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _name: None)
    monkeypatch.setattr(silver, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver, "save_last_success", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.bronze_bucketing, "bronze_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(silver, "_detect_missing_alpha26_market_buckets", lambda: (False, set()))
    monkeypatch.setattr(
        silver,
        "_run_market_reconciliation",
        lambda *, bronze_blob_list: (_ for _ in ()).throw(RuntimeError("reconciliation boom")),
    )
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver.mdc, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.mdc, "write_error", lambda *_args, **_kwargs: None)

    assert silver.main() == 1
