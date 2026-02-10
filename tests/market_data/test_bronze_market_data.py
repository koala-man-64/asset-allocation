import pytest
import uuid
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
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        bronze.download_and_save_raw(symbol, mock_massive)

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
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(bronze.MassiveGatewayNotFoundError):
            bronze.download_and_save_raw(symbol, mock_massive)

        mock_list_manager.add_to_blacklist.assert_called_once_with(symbol)
        mock_store.assert_not_called()
