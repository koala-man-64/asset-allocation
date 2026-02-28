import pytest
import uuid
from datetime import date
from io import BytesIO
from unittest.mock import MagicMock, patch
import pandas as pd

from tasks.market_data import bronze_market_data as bronze


@pytest.fixture
def unique_ticker():
    return f"TEST_MKT_{uuid.uuid4().hex[:8].upper()}"


def test_bronze_ingestion(unique_ticker):
    """
    Verifies Bronze Ingestion:
      1) Mocks API gateway CSV response (Massive via API).
      2) Calls bronze.download_and_save_raw.
      3) Verifies data is written to the Bronze container with canonical schema.
    """
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_daily_time_series_csv.return_value = (
        "timestamp,open,high,low,close,volume\n"
        "2024-01-02,10,11,9,10.5,100\n"
        "2024-01-03,10.5,12,10,11,150\n"
    )
    mock_massive.get_short_interest.return_value = {
        "results": [
            {"date": "2024-01-02", "short_interest": 1000},
            {"date": "2024-01-03", "short_interest": 1200},
        ]
    }
    mock_massive.get_short_volume.return_value = {
        "results": [
            {"date": "2024-01-03", "short_volume": 500},
        ]
    }

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=b"",
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        bronze.download_and_save_raw(symbol, mock_massive)

        mock_massive.get_daily_time_series_csv.assert_called_once()
        _, fetch_kwargs = mock_massive.get_daily_time_series_csv.call_args
        assert fetch_kwargs["from_date"] == "1970-01-01"
        assert fetch_kwargs["adjusted"] is True
        mock_store.assert_called_once()
        args, kwargs = mock_store.call_args
        assert args[1] == f"market-data/{symbol}.csv"
        df = pd.read_csv(BytesIO(args[0]))
        assert list(df.columns) == [
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "ShortInterest",
            "ShortVolume",
        ]
        assert float(df["ShortInterest"].iloc[-1]) == pytest.approx(1200.0)
        assert float(df["ShortVolume"].iloc[0]) == pytest.approx(500.0)


def test_download_populates_short_interest_short_volume_for_aapl():
    symbol = "AAPL"
    mock_massive = MagicMock()
    mock_massive.get_daily_time_series_csv.return_value = (
        "timestamp,open,high,low,close,volume\n"
        "2024-01-02,190.1,193.0,189.8,192.6,1200000\n"
        "2024-01-03,192.6,194.2,191.9,193.8,1400000\n"
    )
    mock_massive.get_short_interest.return_value = {
        "results": [
            {"date": "2024-01-03", "short_interest": 18_500_000},
        ]
    }
    mock_massive.get_short_volume.return_value = {
        "results": [
            {"date": "2024-01-03", "short_volume": 5_200_000},
        ]
    }

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=b"",
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data._utc_today",
        return_value=date(2024, 1, 3),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        bronze.download_and_save_raw(symbol, mock_massive)

        mock_massive.get_daily_time_series_csv.assert_called_once()
        mock_massive.get_short_interest.assert_called_once()
        mock_massive.get_short_volume.assert_called_once()

        _, si_kwargs = mock_massive.get_short_interest.call_args
        assert si_kwargs["symbol"] == symbol
        assert si_kwargs["settlement_date_gte"] == "1970-01-01"
        assert si_kwargs["settlement_date_lte"] == "2024-01-03"

        _, sv_kwargs = mock_massive.get_short_volume.call_args
        assert sv_kwargs["symbol"] == symbol
        assert sv_kwargs["date_gte"] == "1970-01-01"
        assert sv_kwargs["date_lte"] == "2024-01-03"

        args, _ = mock_store.call_args
        df = pd.read_csv(BytesIO(args[0]))
        assert list(df.columns) == [
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "ShortInterest",
            "ShortVolume",
        ]
        assert df["ShortInterest"].notna().all()
        assert df["ShortVolume"].notna().all()
        assert float(df.loc[df["Date"] == "2024-01-03", "ShortInterest"].iloc[0]) == pytest.approx(18_500_000.0)
        assert float(df.loc[df["Date"] == "2024-01-03", "ShortVolume"].iloc[0]) == pytest.approx(5_200_000.0)


def test_header_only_csv_blacklists_symbol(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_daily_time_series_csv.return_value = "Date,Open,High,Low,Close,Volume\n'"

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=b"",
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(bronze.MassiveGatewayNotFoundError):
            bronze.download_and_save_raw(symbol, mock_massive)

        mock_list_manager.add_to_blacklist.assert_called_once_with(symbol)
        mock_store.assert_not_called()


def test_header_only_with_existing_data_does_not_blacklist(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_daily_time_series_csv.return_value = "Date,Open,High,Low,Close,Volume\n"
    existing_csv = (
        "Date,Open,High,Low,Close,Volume,ShortInterest,ShortVolume\n"
        "2024-01-03,10,11,9,10.5,100,1000,500\n"
    ).encode("utf-8")

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=existing_csv,
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False
        bronze.download_and_save_raw(symbol, mock_massive)

        mock_list_manager.add_to_blacklist.assert_not_called()
        mock_list_manager.add_to_whitelist.assert_called_once_with(symbol)
        mock_store.assert_not_called()


def test_download_uses_existing_data_window_and_merges(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_daily_time_series_csv.return_value = (
        "timestamp,open,high,low,close,volume\n"
        "2024-01-03,20,21,19,20.5,200\n"
        "2024-01-04,21,22,20,21.5,250\n"
    )
    mock_massive.get_short_interest.return_value = {"results": [{"date": "2024-01-04", "short_interest": 1500}]}
    mock_massive.get_short_volume.return_value = {"results": [{"date": "2024-01-04", "short_volume": 700}]}

    existing_csv = (
        "Date,Open,High,Low,Close,Volume,ShortInterest,ShortVolume\n"
        "2024-01-02,10,11,9,10.5,100,1000,500\n"
        "2024-01-03,11,12,10,11.5,120,1000,500\n"
    ).encode("utf-8")

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=existing_csv,
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data._utc_today",
        return_value=date(2024, 1, 4),
    ):
        mock_list_manager.is_blacklisted.return_value = False
        bronze.download_and_save_raw(symbol, mock_massive)

        _, fetch_kwargs = mock_massive.get_daily_time_series_csv.call_args
        assert fetch_kwargs["from_date"] == "2024-01-03"
        assert fetch_kwargs["to_date"] == "2024-01-04"

        args, _ = mock_store.call_args
        df = pd.read_csv(BytesIO(args[0]))
        assert df["Date"].tolist() == ["2024-01-02", "2024-01-03", "2024-01-04"]
        assert float(df.loc[df["Date"] == "2024-01-03", "Close"].iloc[0]) == pytest.approx(20.5)
        assert float(df.loc[df["Date"] == "2024-01-04", "ShortInterest"].iloc[0]) == pytest.approx(1500.0)


def test_download_keeps_rows_without_configurable_backfill_cutoff(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_daily_time_series_csv.return_value = (
        "timestamp,open,high,low,close,volume\n"
        "2024-01-01,10,11,9,10.5,100\n"
        "2024-01-03,20,21,19,20.5,200\n"
        "2024-01-04,21,22,20,21.5,250\n"
    )
    mock_massive.get_short_interest.return_value = {}
    mock_massive.get_short_volume.return_value = {}

    existing_csv = (
        "Date,Open,High,Low,Close,Volume,ShortInterest,ShortVolume\n"
        "2024-01-01,9,10,8,9.5,90,1000,500\n"
        "2024-01-02,10,11,9,10.5,100,1000,500\n"
    ).encode("utf-8")

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=existing_csv,
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data._utc_today",
        return_value=date(2024, 1, 4),
    ):
        mock_list_manager.is_blacklisted.return_value = False
        bronze.download_and_save_raw(symbol, mock_massive)

        _, fetch_kwargs = mock_massive.get_daily_time_series_csv.call_args
        assert fetch_kwargs["from_date"] == "2024-01-02"
        assert fetch_kwargs["to_date"] == "2024-01-04"

        args, _ = mock_store.call_args
        df = pd.read_csv(BytesIO(args[0]))
        assert df["Date"].tolist() == ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]


def test_download_does_not_delete_blob_without_backfill_cutoff(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_daily_time_series_csv.return_value = (
        "timestamp,open,high,low,close,volume\n"
        "2024-01-01,10,11,9,10.5,100\n"
        "2024-01-02,10.5,12,10,11,150\n"
    )
    mock_massive.get_short_interest.return_value = {}
    mock_massive.get_short_volume.return_value = {}

    existing_csv = (
        "Date,Open,High,Low,Close,Volume,ShortInterest,ShortVolume\n"
        "2023-12-29,9,10,8,9.5,90,1000,500\n"
        "2023-12-30,10,11,9,10.5,100,1000,500\n"
    ).encode("utf-8")

    mock_bronze_client = MagicMock()

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=existing_csv,
    ), patch(
        "tasks.market_data.bronze_market_data.bronze_client",
        mock_bronze_client,
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data._utc_today",
        return_value=date(2024, 1, 4),
    ):
        mock_list_manager.is_blacklisted.return_value = False
        bronze.download_and_save_raw(symbol, mock_massive)

        _, fetch_kwargs = mock_massive.get_daily_time_series_csv.call_args
        assert fetch_kwargs["from_date"] == "2023-12-30"
        assert fetch_kwargs["to_date"] == "2024-01-04"
        mock_store.assert_called_once()
        mock_bronze_client.delete_file.assert_not_called()
        mock_list_manager.add_to_whitelist.assert_called_once_with(symbol)


class _FakeClientManager:
    def __init__(self) -> None:
        self.reset_current_calls = 0

    def get_client(self):
        return object()

    def reset_current(self) -> None:
        self.reset_current_calls += 1


def test_download_with_recovery_retries_three_attempts(monkeypatch):
    symbol = "RETRYME"
    manager = _FakeClientManager()
    call_count = {"count": 0}
    sleep_calls: list[float] = []

    def _fake_download(sym, _client, *, snapshot_row=None):
        assert sym == symbol
        call_count["count"] += 1
        if call_count["count"] < 3:
            raise bronze.MassiveGatewayError("API gateway call failed: ConnectError: boom")

    monkeypatch.setattr(bronze, "download_and_save_raw", _fake_download)
    monkeypatch.setattr(bronze.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    bronze._download_and_save_raw_with_recovery(symbol, manager, max_attempts=3, sleep_seconds=0.25)

    assert call_count["count"] == 3
    assert manager.reset_current_calls == 2
    assert sleep_calls == [0.25, 0.25]


def test_download_with_recovery_does_not_retry_not_found(monkeypatch):
    symbol = "MISSING"
    manager = _FakeClientManager()
    sleep_calls: list[float] = []

    def _fake_download(_sym, _client, *, snapshot_row=None):
        raise bronze.MassiveGatewayNotFoundError("No data")

    monkeypatch.setattr(bronze, "download_and_save_raw", _fake_download)
    monkeypatch.setattr(bronze.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    with pytest.raises(bronze.MassiveGatewayNotFoundError):
        bronze._download_and_save_raw_with_recovery(symbol, manager, max_attempts=3, sleep_seconds=0.25)

    assert manager.reset_current_calls == 0
    assert sleep_calls == []


def test_download_with_recovery_does_not_retry_non_recoverable_gateway_error(monkeypatch):
    symbol = "AAPL"
    manager = _FakeClientManager()
    sleep_calls: list[float] = []
    call_count = {"count": 0}

    def _fake_download(_sym, _client, *, snapshot_row=None):
        call_count["count"] += 1
        raise bronze.MassiveGatewayError(
            "API gateway error (status=400).",
            status_code=400,
        )

    monkeypatch.setattr(bronze, "download_and_save_raw", _fake_download)
    monkeypatch.setattr(bronze.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    with pytest.raises(bronze.MassiveGatewayError):
        bronze._download_and_save_raw_with_recovery(symbol, manager, max_attempts=3, sleep_seconds=0.25)

    assert call_count["count"] == 1
    assert manager.reset_current_calls == 0
    assert sleep_calls == []


def test_fetch_snapshot_daily_rows_chunks_requests(monkeypatch):
    symbols = [f"SYM{i:03d}" for i in range(300)]
    requested_chunks: list[list[str]] = []

    class _FakeClient:
        def get_unified_snapshot(self, *, symbols, asset_type="stocks"):
            requested_chunks.append(list(symbols))
            return {
                "results": [
                    {
                        "ticker": symbol,
                        "session": {
                            "date": "2024-01-03",
                            "open": 10.0,
                            "high": 11.0,
                            "low": 9.0,
                            "close": 10.5,
                            "volume": 1000,
                        },
                    }
                    for symbol in symbols
                ]
            }

        def close(self):
            return None

    monkeypatch.setattr(bronze.MassiveGatewayClient, "from_env", staticmethod(lambda: _FakeClient()))
    rows = bronze._fetch_snapshot_daily_rows(symbols)

    assert len(requested_chunks) == 2
    assert len(requested_chunks[0]) == 250
    assert len(requested_chunks[1]) == 50
    assert rows["SYM000"]["Date"] == "2024-01-03"
    assert rows["SYM299"]["Close"] == pytest.approx(10.5)


def test_download_uses_snapshot_row_when_incremental_window_allows(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_short_interest.return_value = {}
    mock_massive.get_short_volume.return_value = {}

    existing_csv = (
        "Date,Open,High,Low,Close,Volume,ShortInterest,ShortVolume\n"
        "2024-01-02,10,11,9,10.5,100,1000,500\n"
    ).encode("utf-8")
    snapshot_row = {
        "Date": "2024-01-03",
        "Open": 11.0,
        "High": 12.0,
        "Low": 10.0,
        "Close": 11.5,
        "Volume": 150.0,
    }

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=existing_csv,
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data._utc_today",
        return_value=date(2024, 1, 3),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        bronze.download_and_save_raw(symbol, mock_massive, snapshot_row=snapshot_row)

        mock_massive.get_daily_time_series_csv.assert_not_called()
        args, _ = mock_store.call_args
        df = pd.read_csv(BytesIO(args[0]))
        assert df["Date"].tolist() == ["2024-01-02", "2024-01-03"]
        assert float(df.loc[df["Date"] == "2024-01-03", "Close"].iloc[0]) == pytest.approx(11.5)


def test_download_skips_when_snapshot_is_not_newer(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_daily_time_series_csv.return_value = (
        "timestamp,open,high,low,close,volume\n"
        "2024-01-03,11,12,10,11.5,150\n"
    )
    mock_massive.get_short_interest.return_value = {}
    mock_massive.get_short_volume.return_value = {}

    existing_csv = (
        "Date,Open,High,Low,Close,Volume,ShortInterest,ShortVolume\n"
        "2024-01-03,10,11,9,10.5,100,1000,500\n"
    ).encode("utf-8")
    snapshot_row = {
        "Date": "2024-01-02",
        "Open": 9.5,
        "High": 10.5,
        "Low": 9.0,
        "Close": 10.0,
        "Volume": 95.0,
    }

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=existing_csv,
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data._utc_today",
        return_value=date(2024, 1, 3),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        bronze.download_and_save_raw(symbol, mock_massive, snapshot_row=snapshot_row)

        mock_massive.get_daily_time_series_csv.assert_not_called()
        mock_massive.get_short_interest.assert_not_called()
        mock_massive.get_short_volume.assert_not_called()
        mock_store.assert_not_called()
        mock_list_manager.add_to_whitelist.assert_called_once_with(symbol)


def test_download_skips_when_no_new_daily_rows_and_supplementals_complete(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_daily_time_series_csv.return_value = (
        "timestamp,open,high,low,close,volume\n"
        "2024-01-03,10,11,9,10.5,100\n"
    )
    mock_massive.get_short_interest.return_value = {}
    mock_massive.get_short_volume.return_value = {}

    existing_csv = (
        "Date,Open,High,Low,Close,Volume,ShortInterest,ShortVolume\n"
        "2024-01-03,10,11,9,10.5,100,1000,500\n"
    ).encode("utf-8")

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=existing_csv,
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data._utc_today",
        return_value=date(2024, 1, 3),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        bronze.download_and_save_raw(symbol, mock_massive, snapshot_row=None)

        mock_massive.get_daily_time_series_csv.assert_called_once()
        mock_massive.get_short_interest.assert_not_called()
        mock_massive.get_short_volume.assert_not_called()
        mock_store.assert_not_called()
        mock_list_manager.add_to_whitelist.assert_called_once_with(symbol)
