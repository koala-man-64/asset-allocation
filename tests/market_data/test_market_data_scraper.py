import pytest
import pandas as pd
import uuid
from unittest.mock import MagicMock, AsyncMock, patch
from scripts.market_data import core as mdc_core
from scripts.common import config as cfg
from scripts.common import core as mdc
from scripts.common import delta_core
import os

# --- Helpers ---

@pytest.fixture
def unique_ticker():
    """Generates a unique ticker to avoid collisions in shared storage."""
    return f"TEST_MKT_{uuid.uuid4().hex[:8].upper()}"

@pytest.fixture
def storage_cleanup(unique_ticker):
    """
    Yields the ticker for the test, and cleans up associated blobs after.
    """
    # Setup: Ensure container exists (Safe fallback)
    container = cfg.AZURE_CONTAINER_MARKET
    mdc.get_storage_client(container) 
    
    yield unique_ticker
    
    # Teardown
    print(f"\nCleaning up storage for {unique_ticker}...")
    print(f"\nCleaning up storage for {unique_ticker}...")
    prefix = f"{unique_ticker}"
    
    try:
        client = mdc.get_storage_client(container)
        blobs = client.list_files(name_starts_with=prefix)
        
        for blob in blobs:
            try:
                client.delete_file(blob)
            except Exception:
                pass
                
        # Cleanup Delta Log
        delta_log = f"{prefix}/_delta_log"
        if client.file_exists(delta_log):
             client.delete_file(delta_log)
             
    except Exception as e:
        print(f"Error during cleanup: {e}")

# --- Integration Tests ---

@pytest.mark.asyncio
@patch('scripts.common.playwright_lib.download_yahoo_price_data_async')
async def test_download_and_process_integration(mock_download, unique_ticker, storage_cleanup, tmp_path):
    """
    Verifies that download_and_process_yahoo_data correctly:
    1. Reads a (mocked) downloaded CSV.
    2. Cleans/Transforms it.
    3. Writes it to Azure as Delta (Redirected to local in conftest).
    """
    symbol = unique_ticker
    
    # 1. Prepare Dummy Host CSV
    csv_content = """Date,Open,High,Low,Close,Adj Close,Volume
2023-01-01,100,105,95,102,102,1000
2023-01-02,102,108,100,105,105,1500
"""
    temp_file = tmp_path / f"{symbol}.csv"
    temp_file.write_text(csv_content)
    
    # Mock return so scraper uses this file
    mock_download.return_value = str(temp_file)
    
    # Mock Page (methods used: goto, title)
    mock_page = AsyncMock()
    mock_page.title.return_value = "Yahoo Finance"
    
    # 2. Setup Inputs
    df_ticker = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol'])
    ticker_file_path = f"{symbol}"
    period1 = 1672531200 # Dummy timestamp

    # 3. Execute
    print(f"Executing download_and_process for {symbol}...")
    
    # Mock global list_manager
    mock_list_manager = MagicMock()
    mock_list_manager.is_whitelisted.return_value = False
    mock_list_manager.is_blacklisted.return_value = False
    
    with patch('scripts.market_data.core.list_manager', mock_list_manager):
        res_df, res_path = await mdc_core.download_and_process_yahoo_data(
            symbol, df_ticker, ticker_file_path, mock_page, period1
        )
    
    # 4. Verify Local Result
    assert res_df is not None
    assert len(res_df) == 2
    assert res_df.iloc[0]['Symbol'] == symbol
    assert res_path == ticker_file_path
    
    # 5. Verify Persistence (Delta) - Should be redirected to local via conftest
    print(f"Verifying Delta table at {ticker_file_path}...")
    loaded_df = delta_core.load_delta(cfg.AZURE_CONTAINER_MARKET, ticker_file_path)
    
    assert loaded_df is not None
    assert len(loaded_df) == 2
    assert 'Close' in loaded_df.columns
    assert loaded_df.iloc[1]['Close'] == 105.0

