import pandas as pd
import pytest
from unittest.mock import patch
from tasks.earnings_data import silver_earnings_data as silver
from core.pipeline import DataPaths


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


def test_run_earnings_reconciliation_purges_silver_orphans(monkeypatch):
    class _FakeSilverClient:
        def __init__(self) -> None:
            self.deleted_paths: list[str] = []

        def delete_prefix(self, path: str) -> int:
            self.deleted_paths.append(path)
            return 2

    fake_client = _FakeSilverClient()
    monkeypatch.setattr(silver, "silver_client", fake_client)
    monkeypatch.setattr(
        silver,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL", "MSFT"},
    )
    monkeypatch.setattr(silver, "get_backfill_range", lambda: (None, None))

    orphan_count, deleted_blobs = silver._run_earnings_reconciliation(
        bronze_blob_list=[
            {"name": "earnings-data/AAPL.json"},
            {"name": "earnings-data/whitelist.csv"},
        ]
    )

    assert orphan_count == 1
    assert deleted_blobs == 2
    assert fake_client.deleted_paths == [DataPaths.get_earnings_path("MSFT")]


def test_run_earnings_reconciliation_applies_cutoff_sweep(monkeypatch):
    class _FakeSilverClient:
        def delete_prefix(self, _path: str) -> int:
            return 0

    fake_client = _FakeSilverClient()
    captured: dict = {}

    monkeypatch.setattr(silver, "silver_client", fake_client)
    monkeypatch.setattr(
        silver,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL", "MSFT"},
    )
    monkeypatch.setattr(
        silver,
        "enforce_backfill_cutoff_on_tables",
        lambda **kwargs: captured.update(kwargs)
        or type(
            "_Stats",
            (),
            {"tables_scanned": 0, "tables_rewritten": 0, "deleted_blobs": 0, "rows_dropped": 0, "errors": 0},
        )(),
    )
    monkeypatch.setattr(silver, "get_backfill_range", lambda: (pd.Timestamp("2016-01-01"), None))

    silver._run_earnings_reconciliation(bronze_blob_list=[{"name": "earnings-data/AAPL.json"}])

    assert captured["symbols"] == {"AAPL"}
    assert captured["backfill_start"] == pd.Timestamp("2016-01-01")


def test_run_earnings_reconciliation_requires_storage_client(monkeypatch):
    monkeypatch.setattr(silver, "silver_client", None)

    with pytest.raises(RuntimeError, match="requires silver storage client"):
        silver._run_earnings_reconciliation(bronze_blob_list=[])
