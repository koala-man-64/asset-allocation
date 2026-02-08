import pytest
import uuid
from unittest.mock import MagicMock, patch

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

    with patch("core.core.store_raw_bytes") as mock_store, patch(
        "tasks.market_data.bronze_market_data.list_manager"
    ) as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        bronze.download_and_save_raw(symbol, mock_massive)

        mock_store.assert_called_once()
        args, kwargs = mock_store.call_args
        assert args[1] == f"market-data/{symbol}.csv"
        assert b"Date,Open,High,Low,Close,Volume" in args[0]
