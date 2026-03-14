import asyncio
import json
import uuid
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tasks.finance_data import bronze_finance_data as bronze


@pytest.fixture
def unique_ticker():
    return f"TEST_FIN_{uuid.uuid4().hex[:8].upper()}"


def _sync_result() -> bronze.symbol_availability.SyncResult:
    return bronze.symbol_availability.SyncResult(
        provider="massive",
        source_column="source_massive",
        listed_count=1,
        inserted_count=0,
        disabled_count=0,
        duration_ms=1,
        lock_wait_ms=0,
    )


def _statement_payload(*, period_end: str, total_assets: float) -> dict:
    return {
        "results": [
            {
                "period_end": period_end,
                "financials": {
                    "balance_sheet": {
                        "total_assets": total_assets,
                        "total_current_assets": total_assets / 2.0,
                        "total_current_liabilities": total_assets / 4.0,
                        "long_term_debt_and_capital_lease_obligations": total_assets / 5.0,
                    }
                },
            }
        ]
    }


def test_fetch_and_save_raw_writes_canonical_v2_balance_sheet_row(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_finance_report.side_effect = [
        _statement_payload(period_end="2024-03-31", total_assets=1000.0),
        _statement_payload(period_end="2024-12-31", total_assets=1200.0),
    ]
    row_store: dict[tuple[str, str], dict[str, object]] = {}

    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }

    with patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            report,
            mock_massive,
            alpha26_mode=True,
            alpha26_rows=row_store,
        )

    assert wrote is True
    stored = row_store[(symbol, "balance_sheet")]
    payload = json.loads(str(stored["payload_json"]))
    assert payload["schema_version"] == 2
    assert payload["provider"] == "massive"
    assert payload["report_type"] == "balance_sheet"
    assert payload["rows"] == [
        {
            "date": "2024-03-31",
            "timeframe": "quarterly",
            "long_term_debt": 200.0,
            "total_assets": 1000.0,
            "current_assets": 500.0,
            "current_liabilities": 250.0,
            "shares_outstanding": None,
        },
        {
            "date": "2024-12-31",
            "timeframe": "annual",
            "long_term_debt": 240.0,
            "total_assets": 1200.0,
            "current_assets": 600.0,
            "current_liabilities": 300.0,
            "shares_outstanding": None,
        },
    ]
    assert stored["source_min_date"] == "2024-03-31"
    assert stored["source_max_date"] == "2024-12-31"


def test_fetch_and_save_raw_marks_empty_valuation_payload_as_coverage_unavailable(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_finance_report.return_value = {"results": []}

    report = {
        "folder": "Valuation",
        "file_suffix": "quarterly_valuation_measures",
        "report": "valuation",
    }

    with patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(bronze.BronzeCoverageUnavailableError) as exc_info:
            bronze.fetch_and_save_raw(symbol, report, mock_massive, alpha26_mode=True, alpha26_rows={})

        assert exc_info.value.reason_code == "empty_finance_payload"
        assert exc_info.value.payload == {"symbol": symbol, "report": "valuation"}
        mock_list_manager.add_to_blacklist.assert_not_called()


def test_fetch_and_save_raw_applies_backfill_cutoff_to_canonical_rows(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_finance_report.side_effect = [
        {
            "results": [
                {
                    "period_end": "2023-12-31",
                    "financials": {"balance_sheet": {"total_assets": 900.0}},
                },
                {
                    "period_end": "2024-03-31",
                    "financials": {"balance_sheet": {"total_assets": 1000.0}},
                },
            ]
        },
        {
            "results": [
                {
                    "period_end": "2024-12-31",
                    "financials": {"balance_sheet": {"total_assets": 1200.0}},
                }
            ]
        },
    ]
    row_store: dict[tuple[str, str], dict[str, object]] = {}

    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }

    with patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            report,
            mock_massive,
            backfill_start=date(2024, 1, 1),
            alpha26_mode=True,
            alpha26_rows=row_store,
        )

    assert wrote is True
    stored = row_store[(symbol, "balance_sheet")]
    payload = json.loads(str(stored["payload_json"]))
    assert [row["date"] for row in payload["rows"]] == ["2024-03-31", "2024-12-31"]


def test_fetch_and_save_raw_coverage_gap_overrides_fresh_current_payload(unique_ticker):
    symbol = unique_ticker
    existing_payload = {
        "schema_version": 2,
        "provider": "massive",
        "report_type": "balance_sheet",
        "rows": [
            {
                "date": "2025-01-01",
                "timeframe": "quarterly",
                "total_assets": 100.0,
            }
        ],
    }
    existing_row = bronze._build_finance_bucket_row(
        symbol=symbol,
        report_type="balance_sheet",
        payload=existing_payload,
        source_min_date=date(2025, 1, 1),
        source_max_date=date(2025, 1, 1),
    )
    existing_row["ingested_at"] = datetime.now(timezone.utc).isoformat()

    mock_massive = MagicMock()
    mock_massive.get_finance_report.side_effect = [
        _statement_payload(period_end="2023-12-31", total_assets=90.0),
        _statement_payload(period_end="2025-03-31", total_assets=110.0),
    ]
    row_store = {(symbol, "balance_sheet"): dict(existing_row)}
    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }
    coverage_summary = bronze._empty_coverage_summary()

    with patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager, patch(
        "tasks.finance_data.bronze_finance_data.load_coverage_marker",
        return_value=None,
    ), patch("tasks.finance_data.bronze_finance_data._mark_coverage") as mock_mark_coverage:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            report,
            mock_massive,
            backfill_start=date(2024, 1, 1),
            coverage_summary=coverage_summary,
            alpha26_mode=True,
            alpha26_existing_row=dict(existing_row),
            alpha26_rows=row_store,
        )

    assert wrote is True
    assert coverage_summary["coverage_checked"] == 1
    assert coverage_summary["coverage_forced_refetch"] == 1
    assert mock_massive.get_finance_report.call_count == 2
    mock_mark_coverage.assert_called_once()


def test_process_symbol_with_recovery_retries_transient_report(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_massive
    attempts: dict[str, int] = {"balance_sheet": 0}

    def _fake_fetch(symbol_arg, report, massive_client, *, backfill_start=None, coverage_summary=None):
        assert symbol_arg == symbol
        assert massive_client is mock_massive
        del backfill_start, coverage_summary
        report_name = report["report"]
        if report_name == "balance_sheet":
            attempts["balance_sheet"] += 1
            if attempts["balance_sheet"] == 1:
                raise bronze.MassiveGatewayRateLimitError("throttled", status_code=429)
            return True
        return False

    with patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch), patch(
        "tasks.finance_data.bronze_finance_data.time.sleep"
    ) as mock_sleep:
        result = bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=3,
            sleep_seconds=0.5,
        )

    assert result.wrote == 1
    assert result.invalid_candidate is False
    assert result.valid_symbol is True
    assert result.failures == []
    assert attempts["balance_sheet"] == 2
    assert result.coverage_summary["coverage_checked"] == 0
    manager.reset_current.assert_called_once()
    mock_sleep.assert_called_once_with(0.5)


def test_process_symbol_with_recovery_continues_after_single_invalid_core_report(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_massive
    seen_reports: list[str] = []

    def _fake_fetch(symbol_arg, report, massive_client, *, backfill_start=None, coverage_summary=None):
        assert symbol_arg == symbol
        assert massive_client is mock_massive
        del backfill_start, coverage_summary
        report_name = report["report"]
        seen_reports.append(report_name)
        if report_name == "balance_sheet":
            raise bronze.MassiveGatewayNotFoundError("invalid", status_code=404)
        return True

    with patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch):
        result = bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=3,
            sleep_seconds=0.0,
        )

    assert result.wrote == len(bronze.REPORTS) - 1
    assert result.invalid_candidate is False
    assert result.valid_symbol is True
    assert result.coverage_unavailable is True
    assert result.failures == []
    assert [name for name, _ in result.invalid_evidence] == ["balance_sheet"]
    assert result.coverage_summary["coverage_checked"] == 0
    manager.reset_current.assert_not_called()
    assert seen_reports == [report["report"] for report in bronze.REPORTS]


def test_process_symbol_with_recovery_emits_invalid_candidate_only_after_all_core_invalid(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_massive
    seen_reports: list[str] = []

    def _fake_fetch(symbol_arg, report, massive_client, *, backfill_start=None, coverage_summary=None):
        assert symbol_arg == symbol
        assert massive_client is mock_massive
        del backfill_start, coverage_summary
        report_name = report["report"]
        seen_reports.append(report_name)
        if bronze._is_core_finance_report(report_name):
            raise bronze.MassiveGatewayNotFoundError("invalid", status_code=404)
        return False

    with patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch):
        result = bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=3,
            sleep_seconds=0.0,
        )

    assert result.wrote == 0
    assert result.invalid_candidate is True
    assert result.valid_symbol is False
    assert result.coverage_unavailable is False
    assert result.failures == []
    assert {name for name, _ in result.invalid_evidence} == set(bronze._CORE_FINANCE_REPORTS)
    assert seen_reports == [report["report"] for report in bronze.REPORTS]


def test_main_async_returns_success_when_symbol_is_only_invalid_candidate(unique_ticker):
    symbol = unique_ticker
    client_manager = MagicMock()
    coverage_summary = bronze._empty_coverage_summary()
    invalid_error = bronze.MassiveGatewayNotFoundError("invalid", status_code=404)

    async def run_test():
        with patch("tasks.finance_data.bronze_finance_data._validate_environment"), patch(
            "tasks.finance_data.bronze_finance_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.finance_data.bronze_finance_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.finance_data.bronze_finance_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.finance_data.bronze_finance_data.bronze_bucketing.is_alpha26_mode",
            return_value=True,
        ), patch(
            "tasks.finance_data.bronze_finance_data._load_alpha26_finance_row_map",
            return_value={},
        ), patch(
            "tasks.finance_data.bronze_finance_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.finance_data.bronze_finance_data._ThreadLocalMassiveClientManager",
            return_value=client_manager,
        ), patch(
            "tasks.finance_data.bronze_finance_data._process_symbol_with_recovery",
            return_value=bronze._FinanceSymbolOutcome(
                wrote=0,
                valid_symbol=False,
                invalid_candidate=True,
                coverage_unavailable=False,
                invalid_evidence=[
                    ("balance_sheet", invalid_error),
                    ("cash_flow", invalid_error),
                    ("income_statement", invalid_error),
                ],
                failures=[],
                coverage_summary=coverage_summary,
            ),
        ), patch(
            "tasks.finance_data.bronze_finance_data.record_invalid_symbol_candidate",
            return_value={"promoted": False, "observedRunCount": 1, "blacklistPath": None},
        ) as mock_record_invalid, patch(
            "tasks.finance_data.bronze_finance_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.finance_data.bronze_finance_data._write_alpha26_finance_buckets",
            return_value=(0, "index", len(bronze._BUCKET_COLUMNS)),
        ), patch(
            "tasks.finance_data.bronze_finance_data._delete_flat_finance_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.finance_data.bronze_finance_data.bronze_client"
        ) as mock_bronze_client, patch(
            "tasks.finance_data.bronze_finance_data.create_bronze_finance_manifest",
            return_value=None,
        ), patch(
            "tasks.finance_data.bronze_finance_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.finance_data.bronze_finance_data.mdc.write_line"
        ), patch(
            "tasks.finance_data.bronze_finance_data.mdc.write_warning"
        ):
            mock_bronze_client.list_blob_infos.return_value = []
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_record_invalid.assert_called_once()
        mock_list_manager.add_to_blacklist.assert_not_called()
        mock_list_manager.flush.assert_called_once()
        client_manager.close_all.assert_called_once()

    asyncio.run(run_test())
