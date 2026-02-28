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

def test_silver_processing(unique_ticker):
    """
    Verifies Silver Processing:
    1. Mocks reading raw from Bronze.
    2. Calls silver.process_file.
    3. Verifies Delta Write to Silver.
    """
    symbol = unique_ticker
    blob_name = f"market-data/{symbol}.csv"
    
    csv_content = b"Date,Open,High,Low,Close,Adj Close,Volume\n2023-01-01,100,105,95,102,102,1000"
    
    with patch('core.core.read_raw_bytes') as mock_read, \
         patch('core.delta_core.store_delta') as mock_store_delta, \
         patch('core.delta_core.load_delta') as mock_load_delta:
         
        mock_read.return_value = csv_content
        
        # Mock existing history (None)
        mock_load_delta.return_value = None
        
        # Call
        silver.process_file(blob_name)
        
        # Verify
        mock_read.assert_called_with(blob_name, client=silver.bronze_client) 
        
        mock_store_delta.assert_called_once()
        args, kwargs = mock_store_delta.call_args
        df_saved = args[0]
        container = args[1]
        path = args[2]
        
        assert container == cfg.AZURE_CONTAINER_SILVER
        assert path == DataPaths.get_market_data_path(symbol)
        assert len(df_saved) == 1
        assert df_saved.iloc[0]["close"] == 102


def test_silver_processing_accepts_alpha_vantage_timestamp(unique_ticker):
    symbol = unique_ticker
    blob_name = f"market-data/{symbol}.csv"

    csv_content = b"timestamp,open,high,low,close,volume\n2024-01-03,10.5,12,10,11.0,150\n"

    with patch("core.core.read_raw_bytes") as mock_read, patch(
        "core.delta_core.store_delta"
    ) as mock_store_delta, patch("core.delta_core.load_delta") as mock_load_delta:
        mock_read.return_value = csv_content
        mock_load_delta.return_value = None

        silver.process_file(blob_name)

        mock_store_delta.assert_called_once()


def test_silver_processing_includes_supplemental_market_metrics(unique_ticker):
    symbol = unique_ticker
    blob_name = f"market-data/{symbol}.csv"
    csv_content = (
        b"Date,Open,High,Low,Close,Volume,short_interest,short_volume\n"
        b"2024-01-03,10.5,12,10,11.0,150,1200,500\n"
    )

    with patch("core.core.read_raw_bytes") as mock_read, patch(
        "core.delta_core.store_delta"
    ) as mock_store_delta, patch("core.delta_core.load_delta") as mock_load_delta:
        mock_read.return_value = csv_content
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
    blob_name = f"market-data/{symbol}.csv"
    csv_content = b"Date,Open,High,Low,Close,Volume\n2024-01-03,10.5,12,10,11.0,150\n"
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

    with patch("core.core.read_raw_bytes", return_value=csv_content), patch(
        "core.delta_core.store_delta"
    ) as mock_store, patch(
        "core.delta_core.load_delta", return_value=history
    ):
        assert silver.process_file(blob_name) is True

        df_saved = mock_store.call_args[0][0]
        assert "symbol_2" not in df_saved.columns
        assert "symbol" in df_saved.columns
        assert set(df_saved["symbol"].dropna().astype(str).unique()) == {symbol}


def test_silver_processing_repairs_legacy_symbol_suffix_columns(unique_ticker):
    symbol = unique_ticker
    blob_name = f"market-data/{symbol}.csv"
    csv_content = b"Date,Open,High,Low,Close,Volume\n2024-01-03,10.5,12,10,11.0,150\n"
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

    with patch("core.core.read_raw_bytes", return_value=csv_content), patch(
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
    blob_name = f"market-data/{symbol}.csv"
    csv_content = b"Date,Open,High,Low,Close,Volume\n2024-01-03,10.5,12,10,11.0,150\n"
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

    with patch("core.core.read_raw_bytes", return_value=csv_content), patch(
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
    blob_name = f"market-data/{symbol}.csv"
    csv_content = (
        b"Date,Open,High,Low,Close,Volume\n"
        b"2023-12-31,100,105,95,102,1000\n"
        b"2024-01-03,101,106,96,103,1100\n"
    )
    history = pd.DataFrame(
        [
            {"Date": pd.Timestamp("2023-12-30"), "Open": 99, "High": 104, "Low": 94, "Close": 101, "Volume": 900, "Symbol": symbol}
        ]
    )

    with patch("core.core.read_raw_bytes", return_value=csv_content), patch(
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


def test_run_market_reconciliation_purges_silver_orphans(monkeypatch):
    class _FakeSilverClient:
        def __init__(self) -> None:
            self.deleted_paths: list[str] = []

        def delete_prefix(self, path: str) -> int:
            self.deleted_paths.append(path)
            return 3

    fake_client = _FakeSilverClient()
    monkeypatch.setattr(silver, "silver_client", fake_client)
    monkeypatch.setattr(
        silver,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL", "MSFT"},
    )
    monkeypatch.setattr(silver, "get_backfill_range", lambda: (None, None))

    orphan_count, deleted_blobs = silver._run_market_reconciliation(
        bronze_blob_list=[
            {"name": "market-data/AAPL.csv"},
            {"name": "market-data/blacklist.csv"},
        ]
    )

    assert orphan_count == 1
    assert deleted_blobs == 3
    assert fake_client.deleted_paths == [DataPaths.get_market_data_path("MSFT")]


def test_run_market_reconciliation_applies_cutoff_sweep(monkeypatch):
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

    silver._run_market_reconciliation(bronze_blob_list=[{"name": "market-data/AAPL.csv"}])

    assert captured["symbols"] == {"AAPL"}
    assert captured["backfill_start"] == pd.Timestamp("2016-01-01")


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

    def _fake_enforce_backfill_cutoff_on_tables(**kwargs):
        dirty = pd.DataFrame(
            {
                "date": [pd.Timestamp("2024-01-10")],
                "symbol": ["AAPL"],
                "close": [101.0],
                "__index_level_0__": [8],
            }
        )
        dirty.index = pd.Index([12])
        kwargs["store_table"](dirty, DataPaths.get_market_data_path("AAPL"))
        return type(
            "_Stats",
            (),
            {"tables_scanned": 1, "tables_rewritten": 1, "deleted_blobs": 0, "rows_dropped": 1, "errors": 0},
        )()

    monkeypatch.setattr(silver, "enforce_backfill_cutoff_on_tables", _fake_enforce_backfill_cutoff_on_tables)

    silver._run_market_reconciliation(bronze_blob_list=[{"name": "market-data/AAPL.csv"}])

    assert "__index_level_0__" not in captured["df"].columns
    assert isinstance(captured["df"].index, pd.RangeIndex)
    assert captured["df"].index.start == 0
    assert captured["df"].index.step == 1
