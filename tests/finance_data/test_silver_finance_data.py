import pandas as pd
import json
import pytest
from unittest.mock import patch

from tasks.finance_data import silver_finance_data as silver


def test_silver_finance_processes_alpha_vantage_json_quarterly_reports():
    blob_name = "finance-data/Balance Sheet/TEST_quarterly_balance-sheet.json"
    payload = {
        "symbol": "TEST",
        "quarterlyReports": [
            {
                "fiscalDateEnding": "2024-01-01",
                "totalAssets": "1000",
                "totalLiabilities": "500",
            }
        ],
    }
    raw_bytes = json.dumps(payload).encode("utf-8")

    with patch("core.core.read_raw_bytes") as mock_read, patch("core.delta_core.store_delta") as mock_store, patch(
        "core.delta_core.get_delta_schema_columns", return_value=None
    ):
        mock_read.return_value = raw_bytes

        result = silver.process_blob({"name": blob_name}, desired_end=pd.Timestamp("2024-01-01"))
        assert result.status == "ok"

        mock_store.assert_called_once()
        df = mock_store.call_args.args[0]
        assert "Date" in df.columns
        assert "Symbol" in df.columns
        assert "totalAssets" in df.columns
        assert df.iloc[-1]["Symbol"] == "TEST"


def test_silver_finance_builds_valuation_timeseries_from_overview_and_prices():
    blob_name = "finance-data/Valuation/TEST_quarterly_valuation_measures.json"
    payload = {
        "Symbol": "TEST",
        "MarketCapitalization": "1000",
        "PERatio": "10",
        "ForwardPE": "12",
        "EVToEBITDA": "8",
        "EVToRevenue": "4",
    }
    raw_bytes = json.dumps(payload).encode("utf-8")

    df_prices = pd.DataFrame(
        {
            "Date": ["2024-01-01", "2024-01-02"],
            "Close": [50.0, 100.0],
        }
    )

    with patch("core.core.read_raw_bytes", return_value=raw_bytes), patch(
        "core.delta_core.load_delta", return_value=df_prices
    ), patch("core.delta_core.store_delta") as mock_store, patch(
        "core.delta_core.get_delta_schema_columns", return_value=None
    ):
        result = silver.process_blob({"name": blob_name}, desired_end=pd.Timestamp("2024-01-02"))
        assert result.status == "ok"

        mock_store.assert_called_once()
        df = mock_store.call_args.args[0].sort_values("Date").reset_index(drop=True)
        assert df["Date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-01", "2024-01-02"]

        assert df.loc[0, "market_cap"] == pytest.approx(500.0)
        assert df.loc[1, "market_cap"] == pytest.approx(1000.0)
        assert df.loc[0, "pe_ratio"] == pytest.approx(5.0)
        assert df.loc[1, "pe_ratio"] == pytest.approx(10.0)
        assert df.loc[0, "forward_pe"] == pytest.approx(6.0)
        assert df.loc[1, "forward_pe"] == pytest.approx(12.0)
        assert df.loc[0, "ev_ebitda"] == pytest.approx(4.0)
        assert df.loc[1, "ev_ebitda"] == pytest.approx(8.0)
        assert df.loc[0, "ev_revenue"] == pytest.approx(2.0)
        assert df.loc[1, "ev_revenue"] == pytest.approx(4.0)

        assert df.loc[0, "shares_outstanding"] == pytest.approx(10.0)
        assert df.loc[1, "shares_outstanding"] == pytest.approx(10.0)


def test_silver_finance_prefers_json_when_csv_and_json_target_same_silver_path():
    blobs = [
        {"name": "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.csv"},
        {"name": "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.json"},
        {"name": "finance-data/Cash Flow/MSFT_quarterly_cash-flow.csv"},
    ]

    selected = silver._select_preferred_blob_candidates(blobs)
    selected_names = sorted(item["name"] for item in selected)

    assert "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.json" in selected_names
    assert "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.csv" not in selected_names
    assert "finance-data/Cash Flow/MSFT_quarterly_cash-flow.csv" in selected_names


def test_silver_finance_main_parallel_aggregates_failures_and_updates_watermarks(monkeypatch):
    class BronzeClientStub:
        def __init__(self, blobs):
            self._blobs = blobs

        def list_blob_infos(self, name_starts_with=None):
            assert name_starts_with == "finance-data/"
            return list(self._blobs)

    blobs = [
        {"name": "finance-data/Balance Sheet/OK_quarterly_balance-sheet.json"},
        {"name": "finance-data/Cash Flow/SKIP_quarterly_cash-flow.json"},
        {"name": "finance-data/Valuation/FAIL_quarterly_valuation_measures.json"},
    ]

    monkeypatch.setattr(silver, "bronze_client", BronzeClientStub(blobs))
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver, "_get_ingest_max_workers", lambda: 4)
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-01-31"))

    initial_watermarks = {"preexisting": {"etag": "keep"}}
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: dict(initial_watermarks))

    saved = {}

    def fake_save_watermarks(key, items):
        saved["key"] = key
        saved["items"] = dict(items)

    monkeypatch.setattr(silver, "save_watermarks", fake_save_watermarks)

    def fake_process_blob(blob, *, desired_end, watermarks=None):
        name = blob["name"]
        if name.endswith("OK_quarterly_balance-sheet.json"):
            return silver.BlobProcessResult(
                blob_name=name,
                silver_path="finance-data/balance_sheet/OK_quarterly_balance-sheet",
                ticker="OK",
                status="ok",
                rows_written=7,
                watermark_signature={
                    "etag": "etag-ok",
                    "last_modified": "2026-01-31T00:00:00+00:00",
                    "updated_at": "2026-01-31T00:00:01+00:00",
                },
            )
        if name.endswith("SKIP_quarterly_cash-flow.json"):
            return silver.BlobProcessResult(
                blob_name=name,
                silver_path="finance-data/cash_flow/SKIP_quarterly_cash-flow",
                ticker="SKIP",
                status="skipped",
            )
        return silver.BlobProcessResult(
            blob_name=name,
            silver_path="finance-data/valuation/FAIL_quarterly_valuation_measures",
            ticker="FAIL",
            status="failed",
            error="simulated failure",
        )

    monkeypatch.setattr(silver, "process_blob", fake_process_blob)

    exit_code = silver.main()

    assert exit_code == 1
    assert saved["key"] == "bronze_finance_data"
    assert saved["items"]["preexisting"] == {"etag": "keep"}
    assert (
        saved["items"]["finance-data/Balance Sheet/OK_quarterly_balance-sheet.json"]["etag"]
        == "etag-ok"
    )
    assert "finance-data/Valuation/FAIL_quarterly_valuation_measures.json" not in saved["items"]
