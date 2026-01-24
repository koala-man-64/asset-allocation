import pytest
import uuid
from unittest.mock import MagicMock, AsyncMock, patch
from tasks.market_data import bronze_market_data as bronze
from core import config as cfg
from core import core as mdc
import os

@pytest.fixture
def unique_ticker():
    return f"TEST_MKT_{uuid.uuid4().hex[:8].upper()}"

import asyncio

def test_bronze_ingestion(unique_ticker):
    """
    Verifies Bronze Ingestion:
    1. Mocks Playwright download (returning bytes or temp file).
    2. Calls bronze.download_and_save_raw.
    3. Verifies data is written to Bronze Container.
    """
    symbol = unique_ticker
    
    # Mock Page
    mock_page = AsyncMock()
    
    async def run_test():
        with patch('core.playwright_lib.download_yahoo_price_data_async') as mock_dl, \
             patch('core.core.store_raw_bytes') as mock_store, \
             patch('builtins.open', new_callable=MagicMock) as mock_open_func, \
             patch('os.remove') as mock_remove, \
             patch('os.path.exists') as mock_exists, \
             patch('tasks.market_data.bronze_market_data.list_manager') as mock_list_manager:
             
            # Mock download returning a fake path
            fake_path = "temp_download.csv"
            mock_dl.return_value = fake_path
            
            # Mock wrapper for open/read
            mock_file = MagicMock()
            mock_file.__enter__.return_value.read.return_value = b"test_data"
            mock_open_func.return_value = mock_file
            
            # Mock whitelisting (called inside download_and_save_raw)
            mock_list_manager.is_whitelisted.return_value = False
            mock_list_manager.is_blacklisted.return_value = False
            
            # Mock file existence check
            mock_exists.return_value = True
            
            # Mock page interactions
            mock_page.goto = AsyncMock()
            mock_page.title = AsyncMock(return_value="Yahoo Finance")

            # Call
            await bronze.download_and_save_raw(symbol, mock_page)
            
            # Verify
            mock_store.assert_called_once()
            args, kwargs = mock_store.call_args
            # args[0] is raw_bytes
            assert args[0] == b"test_data"
            # args[1] is blob path
            assert args[1] == f"market-data/{symbol}.csv"

    asyncio.run(run_test())
