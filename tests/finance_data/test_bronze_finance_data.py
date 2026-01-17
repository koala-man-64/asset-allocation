import pytest
import uuid
from unittest.mock import MagicMock, AsyncMock, patch
from scripts.finance_data import bronze_finance_data as bfs
from scripts.common import config as cfg
from scripts.common import core as mdc
from scripts.common.pipeline import DataPaths

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
    container = cfg.AZURE_CONTAINER_BRONZE
    mdc.get_storage_client(container) 
    
    yield unique_ticker
    
    # Teardown
    print(f"\nCleaning up storage for {unique_ticker}...")
    folder = "Valuation"
    target_path = f"finance-data/{folder}/{unique_ticker}_quarterly_valuation_measures.csv"
    
    try:
        client = mdc.get_storage_client(container)
        if client:
            blob_client = client.get_blob_client(target_path)
            if blob_client.exists():
                blob_client.delete_blob()

    except Exception as e:
        print(f"Error during cleanup: {e}")

# --- Integration Tests ---

import asyncio

@patch('scripts.finance_data.bronze_finance_data.pl')
@patch('scripts.finance_data.bronze_finance_data.list_manager')
def test_download_report_cloud_integration(mock_list_manager, mock_pl, unique_ticker, storage_cleanup, tmp_path):
    """
    Verifies download_report:
    1. Navigation/Interaction mocks.
    2. Downloads CSV (mocked).
    3. Writes to Azure Blob (Bronze).
    """
    symbol = storage_cleanup
    
    # 1. Prepare Dummy Host CSV
    csv_content = b"""name,12/31/2022,09/30/2022
Total Assets,1000,900
Total Liabilities,500,450
"""
    temp_file = tmp_path / "yahoo_fin_dump.csv"
    temp_file.write_bytes(csv_content) # Write bytes
    
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
        "folder": "Valuation",
        "file_suffix": "quarterly_valuation_measures",
        "url_template": "http://mock/{ticker}",
        "url": f"http://mock/{symbol}",
        "period": "quarterly",
        "ticker": symbol
    }
    
    # 4. Execute
    print(f"Executing download_report for {symbol}...")
    
    # Initialize explicit client for test
    client = MagicMock() # Mock client directly since mdc.get_storage_client returns None in tests
    
    # Mock whitelisting
    mock_list_manager.is_whitelisted.return_value = False
    
    async def run_test():
        await bfs.download_report(
            playwright_params, 
            symbol,
            report, 
            client
        )
    
    asyncio.run(run_test())
    
    # 5. Verify Cloud Persistence
    target_path = f"finance-data/Valuation/{symbol}_quarterly_valuation_measures.csv"
    print(f"Verifying existence of {target_path}...")
    
    # helper to match bytes content
    # client.upload_data(remote_path, data, overwrite=overwrite)
    client.upload_data.assert_called()
    args, kwargs = client.upload_data.call_args
    # args[0] is path, args[1] is data
    assert args[0] == target_path
    assert args[1] == csv_content
