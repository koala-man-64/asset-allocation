
import pytest
import pandas as pd
import uuid
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from io import BytesIO

from scripts.market_data import bronze_market_data as bronze
from scripts.market_data import silver_market_data as silver
from scripts.common import config as cfg
from scripts.common import core as mdc
from scripts.common import delta_core
from scripts.common.pipeline import DataPaths

# --- Helpers ---

@pytest.fixture
def unique_ticker():
    return f"TEST_MKT_{uuid.uuid4().hex[:8].upper()}"

@pytest.fixture
def clean_env():
    # Patch env vars to redirect to test-containers
    with patch.dict(os.environ, {
        "AZURE_CONTAINER_BRONZE": "test-bronze",
        "AZURE_CONTAINER_SILVER": "test-silver"
    }):
        yield

# --- Bronze Tests ---

@pytest.mark.asyncio
async def test_bronze_ingestion(unique_ticker):
    """
    Verifies Bronze Ingestion:
    1. Mocks Playwright download (returning bytes or temp file).
    2. Calls bronze.download_and_save_raw.
    3. Verifies data is written to Bronze Container.
    """
    symbol = unique_ticker
    
    # Mock Page
    mock_page = AsyncMock()
    
    # Mock pl.download_yahoo_price_data (returns raw bytes in the new implementation? 
    # Let's check bronze_market_data source. It calls pl.download_yahoo_price_data_async which returns a file path usually, 
    # OR returns bytes if refactored?
    # Bronze script: 
    #   csv_path = await pl.download_yahoo_price_data_async(page, ticker, ...)
    #   with open(csv_path, 'rb') as f: raw = f.read()
    #   mdc.store_raw_bytes(raw, ...)
    
    mock_csv_content = b"Date,Open,High,Low,Close,Adj Close,Volume\n2023-01-01,100,105,95,102,102,1000"
    
    with patch('scripts.common.playwright_lib.download_yahoo_price_data_async') as mock_dl, \
         patch('scripts.common.core.store_raw_bytes') as mock_store, \
         patch('builtins.open', new_callable=MagicMock) as mock_open_func, \
         patch('os.remove') as mock_remove:
         
        # Mock download returning a fake path
        fake_path = "temp_download.csv"
        mock_dl.return_value = fake_path
        
        # Mock file read
        mock_file = MagicMock()
        mock_file.__enter__.return_value.read.return_value = mock_csv_content
        mock_open_func.return_value = mock_file
        
        # Call
        await bronze.download_and_save_raw(symbol, mock_page)
        
        # Verify
        mock_store.assert_called_once()
        args, kwargs = mock_store.call_args
        # args[0] is raw_bytes
        assert args[0] == mock_csv_content
        # args[1] is blob path
        assert args[1] == f"market-data/{symbol}.csv"

# --- Silver Tests ---

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
        mock_read.assert_called_with(blob_name, client=silver.bronze_client) # accessed via module
        
        mock_store_delta.assert_called_once()
        args, kwargs = mock_store_delta.call_args
        df_saved = args[0]
        container = args[1]
        path = args[2]
        
        assert container == cfg.AZURE_CONTAINER_SILVER
        assert path == DataPaths.get_market_data_path(symbol)
        assert len(df_saved) == 1
        assert df_saved.iloc[0]['Close'] == 102
