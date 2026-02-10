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
    mock_massive.get_float.return_value = {
        "results": [
            {"date": "2024-01-03", "float_shares": 1000000},
        ]
    }

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=b"",
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data.get_backfill_range",
        return_value=(None, None),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        bronze.download_and_save_raw(symbol, mock_massive)

        mock_massive.get_daily_time_series_csv.assert_called_once()
        _, fetch_kwargs = mock_massive.get_daily_time_series_csv.call_args
        assert fetch_kwargs["from_date"] == "1900-01-01"
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
            "FloatShares",
        ]
        assert float(df["ShortInterest"].iloc[-1]) == pytest.approx(1200.0)
        assert float(df["ShortVolume"].iloc[0]) == pytest.approx(500.0)
        assert float(df["FloatShares"].iloc[0]) == pytest.approx(1_000_000.0)


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
        "Date,Open,High,Low,Close,Volume,ShortInterest,ShortVolume,FloatShares\n"
        "2024-01-03,10,11,9,10.5,100,1000,500,1000000\n"
    ).encode("utf-8")

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=existing_csv,
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data.get_backfill_range",
        return_value=(None, None),
    ):
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
    mock_massive.get_float.return_value = {"results": [{"date": "2024-01-04", "float_shares": 1100000}]}

    existing_csv = (
        "Date,Open,High,Low,Close,Volume,ShortInterest,ShortVolume,FloatShares\n"
        "2024-01-02,10,11,9,10.5,100,1000,500,1000000\n"
        "2024-01-03,11,12,10,11.5,120,1000,500,1000000\n"
    ).encode("utf-8")

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "core.core.read_raw_bytes",
        return_value=existing_csv,
    ), patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data.get_backfill_range",
        return_value=(None, None),
    ), patch(
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


class _FakeClientManager:
    def __init__(self) -> None:
        self.reset_calls = 0

    def get_client(self):
        return object()

    def reset_all(self) -> None:
        self.reset_calls += 1


def test_download_with_recovery_retries_three_attempts(monkeypatch):
    symbol = "RETRYME"
    manager = _FakeClientManager()
    call_count = {"count": 0}
    sleep_calls: list[float] = []

    def _fake_download(sym, _client):
        assert sym == symbol
        call_count["count"] += 1
        if call_count["count"] < 3:
            raise bronze.MassiveGatewayError("API gateway call failed: ConnectError: boom")

    monkeypatch.setattr(bronze, "download_and_save_raw", _fake_download)
    monkeypatch.setattr(bronze.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    bronze._download_and_save_raw_with_recovery(symbol, manager, max_attempts=3, sleep_seconds=0.25)

    assert call_count["count"] == 3
    assert manager.reset_calls == 2
    assert sleep_calls == [0.25, 0.25]


def test_download_with_recovery_does_not_retry_not_found(monkeypatch):
    symbol = "MISSING"
    manager = _FakeClientManager()
    sleep_calls: list[float] = []

    def _fake_download(_sym, _client):
        raise bronze.MassiveGatewayNotFoundError("No data")

    monkeypatch.setattr(bronze, "download_and_save_raw", _fake_download)
    monkeypatch.setattr(bronze.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    with pytest.raises(bronze.MassiveGatewayNotFoundError):
        bronze._download_and_save_raw_with_recovery(symbol, manager, max_attempts=3, sleep_seconds=0.25)

    assert manager.reset_calls == 0
    assert sleep_calls == []
