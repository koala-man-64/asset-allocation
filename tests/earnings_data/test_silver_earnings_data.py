import pandas as pd
import pytest
from unittest.mock import patch
from core.pipeline import DataPaths
from tasks.earnings_data import silver_earnings_data as silver


def test_process_file_success():
    """
    Verifies process_file:
    1. Reads Bronze raw bytes (mocked)
    2. Cleans/normalizes
    3. Merges with history (mocked)
    4. Writes back to Silver (mocked)
    """
    blob_name = "earnings-data/TEST.json"

    # Mock bronze data
    bronze_json = '[{"Date": "2023-01-01", "Reported EPS": 1.5}]'

    # Mock history
    mock_history = pd.DataFrame([{"Date": pd.Timestamp("2022-01-01"), "Reported EPS": 1.0, "Symbol": "TEST"}])

    with (
        patch("core.core.read_raw_bytes", return_value=bronze_json.encode("utf-8")),
        patch("core.delta_core.load_delta", return_value=mock_history),
        patch("core.delta_core.store_delta") as mock_store,
    ):
        res = silver.process_file(blob_name)

        assert res is True
        mock_store.assert_called_once()
        df_saved = mock_store.call_args[0][0]

        # Should have 2 rows (old + new)
        assert len(df_saved) == 2
        assert "TEST" in df_saved["symbol"].values


def test_process_file_bad_json():
    blob_name = "earnings-data/BAD.json"
    with patch("core.core.read_raw_bytes", return_value=b"bad json"):
        res = silver.process_file(blob_name)
        assert res is False


def test_process_file_applies_backfill_start_cutoff():
    blob_name = "earnings-data/TEST.json"
    bronze_json = '[{"Date":"2023-12-31","Reported EPS":1.1},' '{"Date":"2024-01-10","Reported EPS":1.5}]'
    history = pd.DataFrame([{"Date": pd.Timestamp("2023-06-30"), "Reported EPS": 1.0, "Symbol": "TEST"}])

    with (
        patch("core.core.read_raw_bytes", return_value=bronze_json.encode("utf-8")),
        patch("core.delta_core.load_delta", return_value=history),
        patch("core.delta_core.store_delta") as mock_store,
        patch(
            "tasks.earnings_data.silver_earnings_data.get_backfill_range",
            return_value=(pd.Timestamp("2024-01-01"), None),
        ),
        patch("core.delta_core.vacuum_delta_table", return_value=0),
    ):
        assert silver.process_file(blob_name) is True
        df_saved = mock_store.call_args[0][0]
        assert pd.to_datetime(df_saved["date"]).min().date().isoformat() >= "2024-01-01"


def test_process_file_preserves_earnings_numeric_precision():
    blob_name = "earnings-data/TEST.json"
    bronze_json = '[{"Date":"2024-01-10","Reported EPS":1.234567}]'

    with (
        patch("core.core.read_raw_bytes", return_value=bronze_json.encode("utf-8")),
        patch("core.delta_core.load_delta", return_value=None),
        patch("core.delta_core.store_delta") as mock_store,
    ):
        assert silver.process_file(blob_name) is True
        df_saved = mock_store.call_args[0][0]
        assert df_saved.iloc[0]["reported_eps"] == pytest.approx(1.234567)


def test_write_alpha26_earnings_buckets_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_silver_earnings_bucket_path("A")
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")

    def _fake_get_schema(_container: str, path: str):
        captured["checked_paths"].append(path)
        return None

    def _fake_store(_df: pd.DataFrame, _container: str, _path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["mode"] = mode

    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", _fake_get_schema)
    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)

    written_symbols, index_path = silver._write_alpha26_earnings_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 0
    assert captured["checked_paths"] == [target_path]


def test_write_alpha26_earnings_buckets_writes_empty_bucket_when_schema_exists(monkeypatch):
    target_path = DataPaths.get_silver_earnings_bucket_path("A")
    existing_cols = ["date", "symbol", "reported_eps"]
    captured: dict[str, object] = {"store_calls": 0}

    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(
        silver.delta_core,
        "get_delta_schema_columns",
        lambda _container, path: existing_cols if path == target_path else None,
    )

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["df"] = df.copy()
        captured["path"] = path
        captured["mode"] = mode

    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)

    written_symbols, index_path = silver._write_alpha26_earnings_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 1
    assert captured["path"] == target_path
    assert captured["mode"] == "overwrite"
    df_written = captured["df"]
    assert isinstance(df_written, pd.DataFrame)
    assert df_written.empty
    assert list(df_written.columns) == silver._ALPHA26_EARNINGS_MIN_COLUMNS
