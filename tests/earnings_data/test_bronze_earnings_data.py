import pytest
import uuid
import json
import pandas as pd
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from tasks.earnings_data import bronze_earnings_data as bronze

@pytest.fixture
def unique_ticker():
    return f"TEST_EARN_{uuid.uuid4().hex[:8].upper()}"


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
        parsed_dates = [pd.to_datetime(row["Date"], unit="ms").date().isoformat() for row in payload]
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

    existing_df = pd.DataFrame(
        [
            {
                "Date": pd.Timestamp("2023-12-31"),
                "Symbol": symbol,
                "Reported EPS": 1.4,
                "EPS Estimate": 1.2,
                "Surprise": 0.05,
            },
            {
                "Date": pd.Timestamp("2024-03-31"),
                "Symbol": symbol,
                "Reported EPS": 1.8,
                "EPS Estimate": 1.7,
                "Surprise": 0.03,
            },
        ]
    )
    existing_raw = existing_df.to_json(orient="records").encode("utf-8")

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
