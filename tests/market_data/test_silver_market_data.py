import pytest
import pandas as pd
import uuid
import os
from unittest.mock import MagicMock, patch

from scripts.market_data import silver_market_data as silver
from scripts.common import config as cfg
from scripts.common.pipeline import DataPaths

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
    
    with patch('scripts.common.core.read_raw_bytes') as mock_read, \
         patch('scripts.common.delta_core.store_delta') as mock_store_delta, \
         patch('scripts.common.delta_core.load_delta') as mock_load_delta:
         
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
        assert df_saved.iloc[0]['Close'] == 102
