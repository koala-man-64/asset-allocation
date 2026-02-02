import uuid
from unittest.mock import MagicMock, patch

import pytest

from tasks.finance_data import bronze_finance_data as bronze


@pytest.fixture
def unique_ticker():
    return f"TEST_FIN_{uuid.uuid4().hex[:8].upper()}"


def test_fetch_and_save_raw_writes_json(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.fetch.return_value = {"symbol": symbol, "quarterlyReports": [{"fiscalDateEnding": "2024-01-01"}]}

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob_client

    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "function": "BALANCE_SHEET",
    }

    with patch("tasks.finance_data.bronze_finance_data.bronze_client", mock_bronze_client), patch(
        "tasks.finance_data.bronze_finance_data.list_manager"
    ) as mock_list_manager, patch("core.core.store_raw_bytes") as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(symbol, report, mock_av)
        assert wrote is True

        mock_store.assert_called_once()
        args, kwargs = mock_store.call_args
        assert args[1] == f"finance-data/Balance Sheet/{symbol}_quarterly_balance-sheet.json"
        assert b"quarterlyReports" in args[0]

