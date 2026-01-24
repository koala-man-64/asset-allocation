import pytest
import pandas as pd
import asyncio
import uuid
import os
from unittest.mock import MagicMock, patch

from asset_allocation.tasks.price_target_data import bronze_price_target_data as bronze
from asset_allocation.core import config as cfg
from asset_allocation.core import core as mdc
from asset_allocation.core.pipeline import DataPaths

# --- Helpers ---

@pytest.fixture
def unique_ticker():
    return f"TEST_INT_{uuid.uuid4().hex[:8].upper()}"

@pytest.fixture
def storage_cleanup(unique_ticker):
    container = cfg.AZURE_CONTAINER_BRONZE
    mdc.get_storage_client(container) 
    yield unique_ticker

# --- Integration Tests ---


@patch('asset_allocation.tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('asset_allocation.tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('asset_allocation.tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze(mock_list_manager, mock_client, mock_nasdaq, unique_ticker, storage_cleanup):
    symbol = unique_ticker
    
    # 1. Mock Blob checks (return False -> Stale -> Fetch)
    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_client.get_blob_client.return_value = mock_blob_client
    
    # 2. Mock API return
    mock_api_df = pd.DataFrame({
        'ticker': [symbol],
        'obs_date': [pd.Timestamp('2023-01-01')],
        'tp_mean_est': [50.0]
    })
    mock_nasdaq.get_table.return_value = mock_api_df
    
    # 3. Execute
    semaphore = asyncio.Semaphore(1)
    
    async def run_test():
        # We patch store_raw_bytes to verify write
        with patch('asset_allocation.core.core.store_raw_bytes') as mock_store:
            await bronze.process_batch_bronze([symbol], semaphore)
            
            # 4. Verify
            # Check API called
            mock_nasdaq.get_table.assert_called()
            
            # Check Store Raw
            mock_store.assert_called_once()
            args, kwargs = mock_store.call_args
            # args[1] should be path
            assert args[1] == f"price-target-data/{symbol}.parquet"
            
            # Check Whitelist updated
            mock_list_manager.add_to_whitelist.assert_called_with(symbol)

    asyncio.run(run_test())
