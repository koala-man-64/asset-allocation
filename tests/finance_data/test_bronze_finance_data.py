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


def test_fetch_and_save_raw_skips_write_when_no_new_finance_dates(unique_ticker):
    symbol = unique_ticker
    payload = {
        "symbol": symbol,
        "quarterlyReports": [
            {"fiscalDateEnding": "2024-03-31", "reportedCurrency": "USD"},
            {"fiscalDateEnding": "2023-12-31", "reportedCurrency": "USD"},
        ],
    }
    mock_av = MagicMock()
    mock_av.get_finance_report.return_value = payload

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = True
    mock_blob_client.get_blob_properties.return_value = MagicMock(
        last_modified=datetime.now(timezone.utc) - timedelta(days=20)
    )
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob_client
    existing_raw = json.dumps(payload).encode("utf-8")

    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }

    with patch("tasks.finance_data.bronze_finance_data.bronze_client", mock_bronze_client), patch(
        "tasks.finance_data.bronze_finance_data.list_manager"
    ) as mock_list_manager, patch(
        "core.core.read_raw_bytes",
        return_value=existing_raw,
    ), patch("core.core.store_raw_bytes") as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(symbol, report, mock_av)
        assert wrote is False
        mock_store.assert_not_called()
        mock_list_manager.add_to_whitelist.assert_called_with(symbol)


def test_process_symbol_with_recovery_retries_transient_report(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_av
    attempts: dict[str, int] = {"balance_sheet": 0}

    def _fake_fetch(symbol_arg, report, av_client, *, backfill_start=None):
        assert symbol_arg == symbol
        assert av_client is mock_av
        report_name = report["report"]
        if report_name == "balance_sheet":
            attempts["balance_sheet"] += 1
            if attempts["balance_sheet"] == 1:
                raise bronze.AlphaVantageGatewayThrottleError("throttled", status_code=429)
            return True
        return False

    with patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch), patch(
        "tasks.finance_data.bronze_finance_data.time.sleep"
    ) as mock_sleep:
        wrote, blacklisted, failures = bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=3,
            sleep_seconds=0.5,
        )

    assert wrote == 1
    assert blacklisted is False
    assert failures == []
    assert attempts["balance_sheet"] == 2
    manager.reset_current.assert_called_once()
    mock_sleep.assert_called_once_with(0.5)


def test_process_symbol_with_recovery_continues_after_nonrecoverable_report_failure(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_av
    seen_reports: list[str] = []

    def _fake_fetch(symbol_arg, report, av_client, *, backfill_start=None):
        assert symbol_arg == symbol
        assert av_client is mock_av
        report_name = report["report"]
        seen_reports.append(report_name)
        if report_name == "cash_flow":
            raise bronze.AlphaVantageGatewayError("bad request", status_code=400)
        return True

    with patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch):
        wrote, blacklisted, failures = bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=3,
            sleep_seconds=0.0,
        )

    assert blacklisted is False
    assert wrote == 3
    assert len(failures) == 1
    assert failures[0][0] == "cash_flow"
    manager.reset_current.assert_not_called()
    assert seen_reports == [report["report"] for report in bronze.REPORTS]


def test_process_symbol_with_recovery_stops_after_invalid_symbol(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_av
    seen_reports: list[str] = []

    def _fake_fetch(symbol_arg, report, av_client, *, backfill_start=None):
        assert symbol_arg == symbol
        assert av_client is mock_av
        report_name = report["report"]
        seen_reports.append(report_name)
        if report_name == "balance_sheet":
            raise bronze.AlphaVantageGatewayInvalidSymbolError("invalid", status_code=404)
        return True

    with patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch):
        wrote, blacklisted, failures = bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=3,
            sleep_seconds=0.0,
        )

    assert wrote == 0
    assert blacklisted is True
    assert len(failures) == 1
    assert failures[0][0] == "balance_sheet"
    manager.reset_current.assert_not_called()
    assert seen_reports == ["balance_sheet"]
