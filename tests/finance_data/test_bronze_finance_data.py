import uuid
import json
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from tasks.finance_data import bronze_finance_data as bronze


@pytest.fixture
def unique_ticker():
    return f"TEST_FIN_{uuid.uuid4().hex[:8].upper()}"


def test_fetch_and_save_raw_writes_json(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_finance_report.return_value = {
        "symbol": symbol,
        "quarterlyReports": [{"fiscalDateEnding": "2024-01-01"}],
    }

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob_client

    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
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


def test_fetch_and_save_raw_blacklists_empty_quarterly_reports(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_finance_report.return_value = {"symbol": symbol, "quarterlyReports": []}

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob_client

    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }

    with patch("tasks.finance_data.bronze_finance_data.bronze_client", mock_bronze_client), patch(
        "tasks.finance_data.bronze_finance_data.list_manager"
    ) as mock_list_manager, patch("core.core.store_raw_bytes") as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(bronze.AlphaVantageGatewayInvalidSymbolError):
            bronze.fetch_and_save_raw(symbol, report, mock_av)

        mock_list_manager.add_to_blacklist.assert_called_once_with(symbol)
        mock_store.assert_not_called()


def test_fetch_and_save_raw_blacklists_empty_overview_payload(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_finance_report.return_value = {"Symbol": symbol}

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob_client

    report = {
        "folder": "Valuation",
        "file_suffix": "quarterly_valuation_measures",
        "report": "overview",
    }

    with patch("tasks.finance_data.bronze_finance_data.bronze_client", mock_bronze_client), patch(
        "tasks.finance_data.bronze_finance_data.list_manager"
    ) as mock_list_manager, patch("core.core.store_raw_bytes") as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(bronze.AlphaVantageGatewayInvalidSymbolError):
            bronze.fetch_and_save_raw(symbol, report, mock_av)

        mock_list_manager.add_to_blacklist.assert_called_once_with(symbol)
        mock_store.assert_not_called()


def test_fetch_and_save_raw_applies_backfill_start_cutoff(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_finance_report.return_value = {
        "symbol": symbol,
        "quarterlyReports": [
            {"fiscalDateEnding": "2023-12-31", "reportedCurrency": "USD"},
            {"fiscalDateEnding": "2024-03-31", "reportedCurrency": "USD"},
        ],
        "annualReports": [
            {"fiscalDateEnding": "2022-12-31", "reportedCurrency": "USD"},
            {"fiscalDateEnding": "2024-12-31", "reportedCurrency": "USD"},
        ],
    }

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob_client

    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }

    with patch("tasks.finance_data.bronze_finance_data.bronze_client", mock_bronze_client), patch(
        "tasks.finance_data.bronze_finance_data.list_manager"
    ) as mock_list_manager, patch("core.core.store_raw_bytes") as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(symbol, report, mock_av, backfill_start=date(2024, 1, 1))
        assert wrote is True

        args, _ = mock_store.call_args
        payload = json.loads(args[0].decode("utf-8"))
        assert [row["fiscalDateEnding"] for row in payload["quarterlyReports"]] == ["2024-03-31"]
        assert [row["fiscalDateEnding"] for row in payload["annualReports"]] == ["2024-12-31"]


def test_fetch_and_save_raw_deletes_blob_when_cutoff_removes_all_rows(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_finance_report.return_value = {
        "symbol": symbol,
        "quarterlyReports": [
            {"fiscalDateEnding": "2023-12-31", "reportedCurrency": "USD"},
        ],
        "annualReports": [
            {"fiscalDateEnding": "2023-12-31", "reportedCurrency": "USD"},
        ],
    }

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = True
    mock_blob_client.get_blob_properties.return_value = MagicMock(
        last_modified=datetime.now(timezone.utc) - timedelta(days=20)
    )
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob_client

    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }

    with patch("tasks.finance_data.bronze_finance_data.bronze_client", mock_bronze_client), patch(
        "tasks.finance_data.bronze_finance_data.list_manager"
    ) as mock_list_manager, patch("core.core.store_raw_bytes") as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(symbol, report, mock_av, backfill_start=date(2024, 1, 1))
        assert wrote is True

        mock_store.assert_not_called()
        mock_bronze_client.delete_file.assert_called_once_with(
            f"finance-data/Balance Sheet/{symbol}_quarterly_balance-sheet.json"
        )
        mock_list_manager.add_to_whitelist.assert_called_with(symbol)
