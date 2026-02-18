import pytest
import uuid
from unittest.mock import patch

from tasks.market_data import silver_market_data as silver
from core import config as cfg
from core.pipeline import DataPaths

@pytest.fixture
def unique_ticker():
    return f"TEST_MKT_{uuid.uuid4().hex[:8].upper()}"

def test_silver_processing(unique_ticker):
    """
    Verifies Silver Processing:
    1. Mocks reading raw from Bronze.
    2. Calls silver.process_file.
    3. Verifies Delta Write to Silver.
    """
    symbol = unique_ticker
    blob_name = f"market-data/{symbol}.csv"
    
    csv_content = b"Date,Open,High,Low,Close,Adj Close,Volume\n2023-01-01,100,105,95,102,102,1000"
    
    with patch('core.core.read_raw_bytes') as mock_read, \
         patch('core.delta_core.store_delta') as mock_store_delta, \
         patch('core.delta_core.load_delta') as mock_load_delta:
         
        mock_read.return_value = csv_content
        
        # Mock existing history (None)
        mock_load_delta.return_value = None
        
        # Call
        silver.process_file(blob_name)
        
        # Verify
        mock_read.assert_called_with(blob_name, client=silver.bronze_client) 
        
        mock_store_delta.assert_called_once()
        args, kwargs = mock_store_delta.call_args
        df_saved = args[0]
        container = args[1]
        path = args[2]
        
        assert container == cfg.AZURE_CONTAINER_SILVER
        assert path == DataPaths.get_market_data_path(symbol)
        assert len(df_saved) == 1
        assert df_saved.iloc[0]["close"] == 102


def test_silver_processing_accepts_alpha_vantage_timestamp(unique_ticker):
    symbol = unique_ticker
    blob_name = f"market-data/{symbol}.csv"

    csv_content = b"timestamp,open,high,low,close,volume\n2024-01-03,10.5,12,10,11.0,150\n"

    with patch("core.core.read_raw_bytes") as mock_read, patch(
        "core.delta_core.store_delta"
    ) as mock_store_delta, patch("core.delta_core.load_delta") as mock_load_delta:
        mock_read.return_value = csv_content
        mock_load_delta.return_value = None

        silver.process_file(blob_name)

        mock_store_delta.assert_called_once()


def test_silver_processing_includes_supplemental_market_metrics(unique_ticker):
    symbol = unique_ticker
    blob_name = f"market-data/{symbol}.csv"
    csv_content = (
        b"Date,Open,High,Low,Close,Volume,short_interest,short_volume,float_shares\n"
        b"2024-01-03,10.5,12,10,11.0,150,1200,500,1000000\n"
    )

    with patch("core.core.read_raw_bytes") as mock_read, patch(
        "core.delta_core.store_delta"
    ) as mock_store_delta, patch("core.delta_core.load_delta") as mock_load_delta:
        mock_read.return_value = csv_content
        mock_load_delta.return_value = None

        silver.process_file(blob_name)

        mock_store_delta.assert_called_once()
        args, _ = mock_store_delta.call_args
        df_saved = args[0]

        assert "short_interest" in df_saved.columns
        assert "short_volume" in df_saved.columns
        assert "float_shares" in df_saved.columns
        assert float(df_saved.iloc[0]["short_interest"]) == pytest.approx(1200.0)
        assert float(df_saved.iloc[0]["short_volume"]) == pytest.approx(500.0)
        assert float(df_saved.iloc[0]["float_shares"]) == pytest.approx(1_000_000.0)
