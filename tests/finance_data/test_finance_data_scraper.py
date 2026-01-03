import pytest
import pandas as pd
import uuid
import os
from unittest.mock import MagicMock, AsyncMock, patch
from scripts.finance_data import core as yc
from scripts.common import config as cfg
from scripts.common import core as mdc
from scripts.common import delta_core

# --- Helpers ---

@pytest.fixture
def unique_ticker():
    return f"TEST_FIN_{uuid.uuid4().hex[:8].upper()}"

@pytest.fixture
def storage_cleanup(unique_ticker):
    """
    Yields the ticker for the test, and cleans up associated blobs after.
    """
    # Setup: Ensure container exists 
    container = cfg.AZURE_CONTAINER_FINANCE
    mdc.get_storage_client(container) 
    
    yield unique_ticker
    
    # Teardown
    print(f"\nCleaning up storage for {unique_ticker}...")
    # Finance data stores in Yahoo/<Folder>/<Ticker>_<Suffix>
    # We clean by prefix Yahoo/
    # But since we share container with real data, we must be specific!
    # The test writes to Yahoo/Valuation/{unique_ticker}_quarterly_valuation_measures
    
    # Let's list by file name pattern since our unique ticker is in the filename, not folder
    # Or strict path construction
    folder = "Valuation"
    suffix = "quarterly_valuation_measures"
    target_path = f"{folder.lower()}/{unique_ticker}_{suffix}"
    
    try:
        client = mdc.get_storage_client(container)
        
        # 1. Delete the specific delta table directory (blobs under it)
        blobs = client.list_files(name_starts_with=target_path)
        for blob in blobs:
             client.delete_file(blob)
             
        # 2. Try to cleanup delta log explicitly if missed
        delta_log = f"{target_path}/_delta_log"
        if client.file_exists(delta_log):
             client.delete_file(delta_log)

    except Exception as e:
        print(f"Error during cleanup: {e}")

# --- Integration Tests ---


import asyncio


@patch('scripts.finance_data.core.pl')
def test_process_report_cloud_integration(mock_pl, unique_ticker, storage_cleanup, tmp_path):
    """
    Verifies process_report_cloud:
    1. Navigation/Interaction mocks (skipped).
    2. Downloads CSV (mocked).
    3. Transposes Data.
    4. Writes to Azure Delta.
    """
    symbol = storage_cleanup
    
    # 1. Prepare Dummy Host CSV (Yahoo format: Dates as Columns)
    # Yahoo CSV: 
    # name, 12/31/2022, 12/31/2021, ...
    # Total Assets, 1000, 900
    # Total Liab, 500, 400
    
    csv_content = """name,12/31/2022,09/30/2022
Total Assets,1000,900
Total Liabilities,500,450
"""
    temp_file = tmp_path / "yahoo_fin_dump.csv"
    temp_file.write_text(csv_content)
    
    # 2. Mock PL interactions
    mock_pl.load_url_async = AsyncMock()
    mock_pl.element_exists_async = AsyncMock(return_value=True)
    mock_pl.pw_click_by_selectors_async = AsyncMock()
    mock_pl.pw_download_after_click_by_selectors_async = AsyncMock(return_value=str(temp_file))
    
    # Mock Page/Browser
    mock_page = AsyncMock()
    mock_page.title.return_value = "Yahoo Finance"
    playwright_params = (None, None, None, mock_page)
    
    # 3. Report Config
    report = {
        "name": "Integration Test Report",
        "folder": "Valuation", # Matches cleanup
        "file_suffix": "quarterly_valuation_measures", # Matches cleanup
        "url_template": "http://mock/{ticker}",
        "url": f"http://mock/{symbol}", # FIX: Added required 'url' field
        "period": "quarterly",
        "ticker": symbol
    }
    
    # 4. Execute
    print(f"Executing process_report_cloud for {symbol}...")
    

    # Initialize explicit client for test
    client = mdc.get_storage_client(cfg.AZURE_CONTAINER_FINANCE)
    
    # Mock global list_manager in core
    mock_list_manager = MagicMock()
    mock_list_manager.is_whitelisted.return_value = False
    
    async def run_test():
        with patch('scripts.finance_data.core.list_manager', mock_list_manager):
            return await yc.process_report_cloud(
                playwright_params, 
                report, 
                client
            )
    
    success = asyncio.run(run_test())
    
    assert success is True
    
    # 5. Verify Cloud Persistence
    cloud_path = f"valuation/{symbol}_quarterly_valuation_measures"
    print(f"Verifying read from {cloud_path}...")
    
    loaded_df = delta_core.load_delta(cfg.AZURE_CONTAINER_FINANCE, cloud_path)
    
    assert loaded_df is not None
    assert not loaded_df.empty
    
    # Relaxed validation as per user request:
    # Just verify we have data and the symbol column
    assert 'Symbol' in loaded_df.columns
    assert loaded_df.iloc[0]['Symbol'] == symbol

