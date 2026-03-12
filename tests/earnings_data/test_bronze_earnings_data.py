import asyncio
import json
import threading
import uuid
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tasks.earnings_data import bronze_earnings_data as bronze

@pytest.fixture
def unique_ticker():
    return f"TEST_EARN_{uuid.uuid4().hex[:8].upper()}"


def _make_canonical_existing_rows(symbol: str, rows: list[dict]) -> bytes:
    df = pd.DataFrame([{**row, "symbol": symbol} for row in rows])
    return bronze._stamp_canonical_earnings_frame(df).to_json(orient="records").encode("utf-8")


def test_fetch_and_save_raw(unique_ticker):
    """
    Verifies fetch_and_save_raw:
    1. Checks blacklist (mocked).
    2. Calls API gateway client (mocked).
    3. Saves to Bronze as JSON (Raw Bytes).
    """
    symbol = unique_ticker

    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2024-01-01",
                "reportedEPS": "1.6",
                "estimatedEPS": "1.5",
                "surprisePercentage": "10.0",
            }
        ],
    }

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager, patch(
        "core.core.store_raw_bytes"
    ) as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(symbol, mock_av)
        assert wrote is True

        mock_store.assert_called_once()
        args, kwargs = mock_store.call_args
        assert args[1] == f"earnings-data/{symbol}.json"
        mock_list_manager.add_to_whitelist.assert_called_with(symbol)


def test_fetch_and_save_raw_applies_backfill_start_cutoff(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2023-12-31",
                "reportedEPS": "1.4",
                "estimatedEPS": "1.2",
                "surprisePercentage": "5.0",
            },
            {
                "fiscalDateEnding": "2024-03-31",
                "reportedEPS": "1.8",
                "estimatedEPS": "1.7",
                "surprisePercentage": "3.0",
            },
        ],
    }

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager, patch(
        "core.core.store_raw_bytes"
    ) as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(symbol, mock_av, backfill_start=pd.Timestamp("2024-01-01"))
        assert wrote is True

        args, _ = mock_store.call_args
        payload = json.loads(args[0].decode("utf-8"))
        parsed_dates = [pd.to_datetime(row["date"], unit="ms").date().isoformat() for row in payload]
        assert parsed_dates == ["2024-03-31"]


def test_fetch_and_save_raw_deletes_blob_when_cutoff_removes_all_rows(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2023-12-31",
                "reportedEPS": "1.4",
                "estimatedEPS": "1.2",
                "surprisePercentage": "5.0",
            },
        ],
    }

    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.get_blob_properties.return_value = MagicMock(
        last_modified=datetime.now(timezone.utc) - timedelta(days=20)
    )
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob

    with patch("tasks.earnings_data.bronze_earnings_data.bronze_client", mock_bronze_client), patch(
        "tasks.earnings_data.bronze_earnings_data.list_manager"
    ) as mock_list_manager, patch(
        "core.core.store_raw_bytes"
    ) as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(symbol, mock_av, backfill_start=pd.Timestamp("2024-01-01"))
        assert wrote is True

        mock_store.assert_not_called()
        mock_bronze_client.delete_file.assert_called_once_with(f"earnings-data/{symbol}.json")
        mock_list_manager.add_to_whitelist.assert_called_with(symbol)


def test_fetch_and_save_raw_skips_write_when_no_new_earnings_dates(unique_ticker):
    symbol = unique_ticker
    payload = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2024-03-31",
                "reportedEPS": "1.8",
                "estimatedEPS": "1.7",
                "surprisePercentage": "3.0",
            },
            {
                "fiscalDateEnding": "2023-12-31",
                "reportedEPS": "1.4",
                "estimatedEPS": "1.2",
                "surprisePercentage": "5.0",
            },
        ],
    }
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = payload

    existing_raw = _make_canonical_existing_rows(
        symbol,
        [
            {
                "date": "2023-12-31",
                "report_date": None,
                "fiscal_date_ending": "2023-12-31",
                "reported_eps": 1.4,
                "eps_estimate": 1.2,
                "surprise": 0.05,
                "record_type": "actual",
            },
            {
                "date": "2024-03-31",
                "report_date": None,
                "fiscal_date_ending": "2024-03-31",
                "reported_eps": 1.8,
                "eps_estimate": 1.7,
                "surprise": 0.03,
                "record_type": "actual",
            },
        ],
    )

    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.get_blob_properties.return_value = MagicMock(
        last_modified=datetime.now(timezone.utc) - timedelta(days=20)
    )
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob

    with patch("tasks.earnings_data.bronze_earnings_data.bronze_client", mock_bronze_client), patch(
        "tasks.earnings_data.bronze_earnings_data.list_manager"
    ) as mock_list_manager, patch(
        "core.core.read_raw_bytes",
        return_value=existing_raw,
    ), patch("core.core.store_raw_bytes") as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(symbol, mock_av)
        assert wrote is False
        mock_store.assert_not_called()
        mock_list_manager.add_to_whitelist.assert_called_with(symbol)


def test_fetch_and_save_raw_invalid_payload_attaches_payload(unique_ticker):
    symbol = unique_ticker
    payload = {
        "symbol": symbol,
        "quarterlyEarnings": [],
        "note": "missing earnings history",
    }
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = payload

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(bronze.AlphaVantageGatewayInvalidSymbolError) as exc_info:
            bronze.fetch_and_save_raw(symbol, mock_av)

    assert exc_info.value.payload == payload


def test_fetch_and_save_raw_merges_scheduled_calendar_rows(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2024-12-31",
                "reportedDate": "2025-02-10",
                "reportedEPS": "1.6",
                "estimatedEPS": "1.5",
                "surprisePercentage": "6.0",
            }
        ],
    }
    calendar_rows = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "name": "Test Co",
                "report_date": pd.Timestamp("2026-05-07"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "estimate": 1.7,
                "currency": "USD",
                "time_of_the_day": "post-market",
            }
        ]
    )

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager, patch(
        "core.core.store_raw_bytes"
    ) as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(symbol, mock_av, calendar_rows=calendar_rows)

        assert wrote is True
        payload = json.loads(mock_store.call_args[0][0].decode("utf-8"))
        assert len(payload) == 2
        scheduled = next(row for row in payload if row["record_type"] == "scheduled")
        assert pd.to_datetime(scheduled["date"], unit="ms").date().isoformat() == "2026-05-07"
        assert pd.to_datetime(scheduled["report_date"], unit="ms").date().isoformat() == "2026-05-07"
        assert scheduled["reported_eps"] is None
        assert scheduled["calendar_time_of_day"] == "post-market"
        assert scheduled["calendar_currency"] == "USD"


def test_fetch_and_save_raw_replaces_stale_scheduled_row_when_calendar_date_moves(unique_ticker):
    symbol = unique_ticker
    existing_raw = _make_canonical_existing_rows(
        symbol,
        [
            {
                "date": "2026-05-01",
                "report_date": "2026-05-01",
                "fiscal_date_ending": "2026-03-31",
                "reported_eps": None,
                "eps_estimate": 1.7,
                "surprise": None,
                "record_type": "scheduled",
                "calendar_time_of_day": "post-market",
                "calendar_currency": "USD",
            }
        ],
    )
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {"symbol": symbol, "quarterlyEarnings": []}
    calendar_rows = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "name": "Test Co",
                "report_date": pd.Timestamp("2026-05-08"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "estimate": 1.8,
                "currency": "USD",
                "time_of_the_day": "post-market",
            }
        ]
    )

    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.get_blob_properties.return_value = MagicMock(
        last_modified=datetime.now(timezone.utc) - timedelta(days=20)
    )
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob

    with patch("tasks.earnings_data.bronze_earnings_data.bronze_client", mock_bronze_client), patch(
        "tasks.earnings_data.bronze_earnings_data.list_manager"
    ) as mock_list_manager, patch(
        "core.core.read_raw_bytes",
        return_value=existing_raw,
    ), patch("core.core.store_raw_bytes") as mock_store, patch(
        "tasks.earnings_data.bronze_earnings_data._utc_today",
        return_value=pd.Timestamp("2026-05-02"),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(symbol, mock_av, calendar_rows=calendar_rows)

        assert wrote is True
        payload = json.loads(mock_store.call_args[0][0].decode("utf-8"))
        assert len(payload) == 1
        row = payload[0]
        assert row["record_type"] == "scheduled"
        assert pd.to_datetime(row["report_date"], unit="ms").date().isoformat() == "2026-05-08"
        assert pd.to_datetime(row["fiscal_date_ending"], unit="ms").date().isoformat() == "2026-03-31"


def test_fetch_and_save_raw_actual_replaces_scheduled_row_for_same_fiscal_period(unique_ticker):
    symbol = unique_ticker
    existing_raw = _make_canonical_existing_rows(
        symbol,
        [
            {
                "date": "2026-05-08",
                "report_date": "2026-05-08",
                "fiscal_date_ending": "2026-03-31",
                "reported_eps": None,
                "eps_estimate": 1.8,
                "surprise": None,
                "record_type": "scheduled",
                "calendar_time_of_day": "post-market",
                "calendar_currency": "USD",
            }
        ],
    )
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2026-03-31",
                "reportedDate": "2026-05-09",
                "reportedEPS": "1.9",
                "estimatedEPS": "1.8",
                "surprisePercentage": "5.5",
            }
        ],
    }

    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.get_blob_properties.return_value = MagicMock(
        last_modified=datetime.now(timezone.utc) - timedelta(days=20)
    )
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob

    with patch("tasks.earnings_data.bronze_earnings_data.bronze_client", mock_bronze_client), patch(
        "tasks.earnings_data.bronze_earnings_data.list_manager"
    ) as mock_list_manager, patch(
        "core.core.read_raw_bytes",
        return_value=existing_raw,
    ), patch("core.core.store_raw_bytes") as mock_store, patch(
        "tasks.earnings_data.bronze_earnings_data._utc_today",
        return_value=pd.Timestamp("2026-05-10"),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(symbol, mock_av)

        assert wrote is True
        payload = json.loads(mock_store.call_args[0][0].decode("utf-8"))
        assert len(payload) == 1
        row = payload[0]
        assert row["record_type"] == "actual"
        assert pd.to_datetime(row["fiscal_date_ending"], unit="ms").date().isoformat() == "2026-03-31"
        assert pd.to_datetime(row["report_date"], unit="ms").date().isoformat() == "2026-05-09"
        assert row["reported_eps"] == pytest.approx(1.9)


def test_fetch_and_save_raw_coverage_gap_overrides_freshness(unique_ticker):
    symbol = unique_ticker
    payload = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2024-03-31",
                "reportedEPS": "1.8",
                "estimatedEPS": "1.7",
                "surprisePercentage": "3.0",
            },
        ],
    }
    existing_df = pd.DataFrame(
        [
            {
                "Date": pd.Timestamp("2025-03-31"),
                "Symbol": symbol,
                "Reported EPS": 1.9,
                "EPS Estimate": 1.7,
                "Surprise": 0.03,
            },
        ]
    )
    existing_raw = existing_df.to_json(orient="records").encode("utf-8")
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = payload

    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.get_blob_properties.return_value = MagicMock(last_modified=datetime.now(timezone.utc))
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob
    coverage_summary = bronze._empty_coverage_summary()

    with patch("tasks.earnings_data.bronze_earnings_data.bronze_client", mock_bronze_client), patch(
        "tasks.earnings_data.bronze_earnings_data.list_manager"
    ) as mock_list_manager, patch(
        "core.core.read_raw_bytes",
        return_value=existing_raw,
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.load_coverage_marker",
        return_value=None,
    ), patch(
        "tasks.earnings_data.bronze_earnings_data._mark_coverage"
    ) as mock_mark_coverage, patch(
        "core.core.store_raw_bytes"
    ) as mock_store:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            backfill_start=date(2024, 1, 1),
            coverage_summary=coverage_summary,
        )

    assert wrote is True
    assert coverage_summary["coverage_checked"] == 1
    assert coverage_summary["coverage_forced_refetch"] == 1
    mock_av.get_earnings.assert_called_once()
    mock_store.assert_called_once()
    mock_mark_coverage.assert_called_once()


@pytest.mark.parametrize(
    ("payload", "expected_status", "expected_earliest"),
    [
        (
            {
                "quarterlyEarnings": [
                    {
                        "fiscalDateEnding": "2023-12-31",
                        "reportedEPS": "1.4",
                        "estimatedEPS": "1.2",
                        "surprisePercentage": "5.0",
                    },
                    {
                        "fiscalDateEnding": "2025-03-31",
                        "reportedEPS": "1.9",
                        "estimatedEPS": "1.8",
                        "surprisePercentage": "2.0",
                    },
                ]
            },
            "covered",
            date(2023, 12, 31),
        ),
        (
            {
                "quarterlyEarnings": [
                    {
                        "fiscalDateEnding": "2025-03-31",
                        "reportedEPS": "1.9",
                        "estimatedEPS": "1.8",
                        "surprisePercentage": "2.0",
                    },
                ]
            },
            "limited",
            date(2025, 3, 31),
        ),
    ],
)
def test_fetch_and_save_raw_marks_coverage_status_from_source_payload(
    unique_ticker,
    payload,
    expected_status,
    expected_earliest,
):
    symbol = unique_ticker
    payload = {"symbol": symbol, **payload}
    existing_df = pd.DataFrame(
        [
            {
                "Date": pd.Timestamp("2025-01-01"),
                "Symbol": symbol,
                "Reported EPS": 1.9,
                "EPS Estimate": 1.8,
                "Surprise": 0.02,
            },
        ]
    )
    existing_raw = existing_df.to_json(orient="records").encode("utf-8")
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = payload

    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.get_blob_properties.return_value = MagicMock(
        last_modified=datetime.now(timezone.utc) - timedelta(days=20)
    )
    mock_bronze_client = MagicMock()
    mock_bronze_client.get_blob_client.return_value = mock_blob
    coverage_summary = bronze._empty_coverage_summary()

    with patch("tasks.earnings_data.bronze_earnings_data.bronze_client", mock_bronze_client), patch(
        "tasks.earnings_data.bronze_earnings_data.list_manager"
    ) as mock_list_manager, patch(
        "core.core.read_raw_bytes",
        return_value=existing_raw,
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.load_coverage_marker",
        return_value=None,
    ), patch(
        "tasks.earnings_data.bronze_earnings_data._mark_coverage"
    ) as mock_mark_coverage, patch(
        "core.core.store_raw_bytes"
    ):
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            backfill_start=date(2024, 1, 1),
            coverage_summary=coverage_summary,
        )

    assert wrote is True
    assert coverage_summary["coverage_forced_refetch"] == 1
    _, kwargs = mock_mark_coverage.call_args
    assert kwargs["status"] == expected_status
    assert kwargs["earliest_available"] == expected_earliest


def test_main_async_logs_invalid_payload_preview(unique_ticker):
    symbol = unique_ticker
    payload = {"detail": "X" * 700}
    expected_preview = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)[:500] + "..."
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.get_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.is_alpha26_mode",
            return_value=False,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=bronze.AlphaVantageGatewayInvalidSymbolError("invalid", payload=payload),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ) as mock_write_warning, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_list_manager.add_to_blacklist.assert_called_with(symbol)
        warning_messages = [call.args[0] for call in mock_write_warning.call_args_list if call.args]
        assert any(
            message
            == (
                f"Invalid earnings payload for {symbol}; automatic blacklist updates are disabled "
                f"for job runs. payload_preview={expected_preview}"
            )
            for message in warning_messages
        )

    asyncio.run(run_test())


def test_main_async_logs_invalid_payload_detail_preview_when_payload_missing(unique_ticker):
    symbol = unique_ticker
    detail = "X" * 700
    expected_payload = {"status_code": 404, "detail": detail, "message": "invalid"}
    expected_preview = json.dumps(expected_payload, separators=(",", ":"), ensure_ascii=False)[:500] + "..."
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.get_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.is_alpha26_mode",
            return_value=False,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=bronze.AlphaVantageGatewayInvalidSymbolError(
                "invalid",
                status_code=404,
                detail=detail,
            ),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ) as mock_write_warning, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_list_manager.add_to_blacklist.assert_called_with(symbol)
        warning_messages = [call.args[0] for call in mock_write_warning.call_args_list if call.args]
        assert any(
            message
            == (
                f"Invalid earnings payload for {symbol}; automatic blacklist updates are disabled "
                f"for job runs. payload_preview={expected_preview}"
            )
            for message in warning_messages
        )

    asyncio.run(run_test())


def test_thread_local_alpha_vantage_client_manager_reuses_per_thread_and_closes_all():
    created: list[MagicMock] = []

    def make_client() -> MagicMock:
        client = MagicMock()
        created.append(client)
        return client

    manager = bronze._ThreadLocalAlphaVantageClientManager(factory=make_client)
    first = manager.get_client()
    second = manager.get_client()
    assert first is second

    observed_from_thread: list[MagicMock] = []

    def worker() -> None:
        observed_from_thread.append(manager.get_client())

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()

    assert len(created) == 2
    assert observed_from_thread[0] is not first

    manager.close_all()

    for client in created:
        client.close.assert_called_once_with()
