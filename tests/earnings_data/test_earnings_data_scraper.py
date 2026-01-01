
import pytest
import pandas as pd
import uuid
import os
from unittest.mock import MagicMock, AsyncMock, patch
from scripts.earnings_data import scraper as earn_scraper
from scripts.common import config as cfg
from scripts.common import core as mdc
from scripts.common import delta_core

# --- Helpers ---

@pytest.fixture
def unique_ticker():
    return f"TEST_EARN_{uuid.uuid4().hex[:8].upper()}"

@pytest.fixture
def storage_cleanup(unique_ticker):
    """
    Cleaner for earnings data.
    """
    container = cfg.AZURE_CONTAINER_EARNINGS
    mdc.get_storage_client(container) 
    
    yield unique_ticker
    
    print(f"\nCleaning up storage for {unique_ticker}...")
    prefix = f"yahoo/earnings/{unique_ticker}"
    
    try:
        client = mdc.get_storage_client(container)
        blobs = client.list_files(name_starts_with=prefix)
        for blob in blobs:
             client.delete_file(blob)
             
        # Cleanup Delta Log
        delta_log = f"{prefix}/_delta_log"
        if client.file_exists(delta_log):
             client.delete_file(delta_log)
             
    except Exception as e:
        print(f"Error during cleanup: {e}")

# --- Integration Tests ---

@pytest.mark.asyncio
@patch('scripts.earnings_data.scraper.pl.get_yahoo_earnings_data')
async def test_earnings_migration_integration(mock_get_data, unique_ticker, storage_cleanup):
    """
    Verifies the new earnings scraper loop:
    1. Checks Delta freshness (mocked empty first).
    2. Calls Playwright helper (mocked).
    3. Saves to Delta.
    """
    symbol = unique_ticker
    
    # 1. Mock Data
    mock_df = pd.DataFrame({
        'Symbol': [symbol],
        'Earnings Date': ['2023-01-01'],
        'EPS Estimate': [1.5],
        'Reported EPS': [1.6],
        'Surprise(%)': [0.1]
    })
    
    mock_get_data.return_value = mock_df
    
    # 2. Setup Inputs
    df_symbols = pd.DataFrame({'Symbol': [symbol]})
    
    # 3. Execute
    # We mock 'pl.get_playwright_browser' to avoid real browser launch, 
    # but we want 'main_async' to run the logic.
    # main_async initializes playwright, so we mock that too.
    
    with patch('scripts.earnings_data.scraper.pl.get_playwright_browser') as mock_browser_init:
        # Mock browser components
        mock_page = AsyncMock()
        mock_context = AsyncMock()
        mock_context.new_page.return_value = mock_page
        mock_browser = AsyncMock()
        mock_playwright = AsyncMock()
        
        mock_browser_init.return_value = (mock_playwright, mock_browser, mock_context, mock_page)
        
        # Also need to mock cookies load to prevent file error if not exists
        with patch('scripts.earnings_data.scraper.pl.pw_load_cookies_async') as mock_cookies, \
             patch('scripts.common.config.DEBUG_SYMBOLS', []):
             await earn_scraper.main_async(df_symbols)

    # 4. Verify Cloud Persistence
    cloud_path = f"yahoo/earnings/{symbol}"
    print(f"Verifying read from {cloud_path}...")
    
    loaded_df = delta_core.load_delta(cfg.AZURE_CONTAINER_EARNINGS, cloud_path)
    
    assert loaded_df is not None
    assert not loaded_df.empty
    assert loaded_df.iloc[0]['Symbol'] == symbol
    assert loaded_df.iloc[0]['Reported EPS'] == 1.6
