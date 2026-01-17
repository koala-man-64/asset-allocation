import pytest
import pandas as pd
import uuid
import os
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from scripts.earnings_data import bronze_earnings_data as bronze
from scripts.common import config as cfg
from scripts.common import core as mdc

@pytest.fixture
def unique_ticker():
    return f"TEST_EARN_{uuid.uuid4().hex[:8].upper()}"


def test_fetch_and_save_raw(unique_ticker):
    """
    Verifies fetch_and_save_raw:
    1. Checks blacklist (mocked).
    2. Calls Playwright helper (mocked).
    3. Saves to Bronze as JSON (Raw Bytes).
    """
    symbol = unique_ticker
    
    # Mock Context/Page
    mock_context = AsyncMock()
    mock_page = AsyncMock()
    mock_context.new_page.return_value = mock_page
    
    # Mock Data
    mock_df = pd.DataFrame({
        'Symbol': [symbol],
        'Earnings Date': ['2023-01-01'],
        'EPS Estimate': [1.5],
        'Reported EPS': [1.6],
        'Surprise(%)': [0.1]
    })
    
    semaphore = asyncio.Semaphore(1)
    
    async def run_test():
        with patch('scripts.common.playwright_lib.get_yahoo_earnings_data', return_value=mock_df) as mock_get_data, \
             patch('scripts.earnings_data.bronze_earnings_data.list_manager') as mock_list_manager, \
             patch('scripts.common.core.store_raw_bytes') as mock_store:
             
            mock_list_manager.is_blacklisted.return_value = False
            
            # Execute
            await bronze.fetch_and_save_raw(symbol, mock_context, semaphore)
            
            # Verify
            mock_get_data.assert_called()
            
            mock_store.assert_called_once()
            args, kwargs = mock_store.call_args
            # args[1] should be path
            assert args[1] == f"earnings-data/{symbol}.json"
            
            mock_list_manager.add_to_whitelist.assert_called_with(symbol)

    asyncio.run(run_test())
