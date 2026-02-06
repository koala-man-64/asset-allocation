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
