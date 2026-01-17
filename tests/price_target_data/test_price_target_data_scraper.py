
import pytest
import pandas as pd
import numpy as np
import uuid
import time
import asyncio
from unittest.mock import MagicMock, patch
from scripts.price_target_data import silver_price_target_data as pta
from scripts.common import config as cfg
from scripts.common import core as mdc
from scripts.common import delta_core
from scripts.common.pipeline import DataPaths

# --- Helpers ---

@pytest.fixture
def unique_ticker():
    """Generates a unique ticker to avoid collisions in shared storage."""
    return f"TEST_INT_{uuid.uuid4().hex[:8].upper()}"

@pytest.fixture
def storage_cleanup(unique_ticker):
    """
    Yields the ticker for the test, and cleans up associated blobs after.
    """
    # Setup: Ensure container exists (Safe fallback for non-existent containers during tests)
    container = cfg.AZURE_CONTAINER_BRONZE
    mdc.get_storage_client(container) # This initializes Client which auto-checks/creates container
    
    yield unique_ticker
    
    # Teardown
    print(f"\nCleaning up storage for {unique_ticker}...")
    container = cfg.AZURE_CONTAINER_BRONZE
    prefix = DataPaths.get_price_target_path(unique_ticker)
    
    try:
        client = mdc.get_storage_client(container)
        if client:
            blobs = client.list_files(name_starts_with=prefix)
            blobs.sort(key=len, reverse=True)
            
            for blob in blobs:
                try:
                    client.delete_file(blob)
                except Exception as e:
                    print(f"Warning: Failed to delete {blob}: {e}")
            
            dirs_to_delete = [f"{prefix}/_delta_log", prefix]
            for d in dirs_to_delete:
                try:
                    if client.file_exists(d):
                        client.delete_file(d)
                except Exception:
                    pass
    except Exception as e:
        print(f"Error during cleanup of {prefix}: {e}")

# --- Integration Tests ---

def test_transform_symbol_data_integration(storage_cleanup):
    symbol = storage_cleanup
    existing_df = pd.DataFrame()
    
    # Input Data
    dates = pd.date_range('2023-01-01', periods=3)
    target_price_data = pd.DataFrame({
        'obs_date': dates, 
        'tp_mean_est': [10.0, 11.0, 12.0]
    })
    
    # Execution (Real Write)
    print(f"Writing integration test data for {symbol}...")
    res = pta.transform_symbol_data(symbol, target_price_data, existing_df)
    
    # Assertions on Result
    assert res is not None
    assert not res.empty
    assert res.iloc[0]['symbol'] == symbol
    
    # Verify Data Persistence (Real Read)
    path = DataPaths.get_price_target_path(symbol)
    print(f"Verifying read from {path}...")
    loaded_df = delta_core.load_delta(cfg.AZURE_CONTAINER_BRONZE, path)
    
    assert loaded_df is not None
    assert not loaded_df.empty
    assert 'tp_mean_est' in loaded_df.columns
    assert len(loaded_df) >= 3

@patch('scripts.price_target_data.silver_price_target_data.nasdaqdatalink')
def test_process_symbols_batch_fresh_integration(mock_nasdaq, storage_cleanup):
    # Scenario: Symbol exists and is fresh.
    symbol = storage_cleanup
    
    # 1. Setup: Write "Fresh" Data
    path = DataPaths.get_price_target_path(symbol)
    
    # Create dummy DataFrame
    df = pd.DataFrame({
        'symbol': [symbol],
        'obs_date': [pd.Timestamp.now()],
        'tp_mean_est': [100.0]
    })
    
    print(f"Pre-seeding fresh data for {symbol}...")
    delta_core.store_delta(df, cfg.AZURE_CONTAINER_BRONZE, path)
    
    # 2. Execute Batch (Async wrapper)
    async def run_test():
        semaphore = asyncio.Semaphore(1)
        return await pta.process_batch_async([symbol], semaphore)

    res = asyncio.run(run_test())
    
    # 3. Verify
    # Should find it fresh and return it WITHOUT calling API
    assert len(res) == 1
    assert res[0] == symbol
    mock_nasdaq.get_table.assert_not_called()

@patch('scripts.price_target_data.silver_price_target_data.nasdaqdatalink')
def test_process_symbols_batch_stale_integration(mock_nasdaq, storage_cleanup):
    # Scenario: Symbol is missing (stale by default), should write new data.
    symbol = storage_cleanup
    
    # 1. Mock API return for this symbol
    mock_api_df = pd.DataFrame({
        'ticker': [symbol],
        'obs_date': [pd.Timestamp('2023-01-01')],
        'tp_mean_est': [50.0]
    })
    # The async code calls run_in_executor which calls the sync get_table
    mock_nasdaq.get_table.return_value = mock_api_df
    
    # 2. Execute (Async wrapper)
    print(f"Running batch for stale/missing symbol {symbol}...")
    
    async def run_test():
        semaphore = asyncio.Semaphore(1)
        return await pta.process_batch_async([symbol], semaphore)

    res = asyncio.run(run_test())
    
    # 3. Verify
    assert len(res) == 1
    assert res[0] == symbol
    mock_nasdaq.get_table.assert_called()
    
    # Verify data was written to cloud
    path = DataPaths.get_price_target_path(symbol)
    loaded_df = delta_core.load_delta(cfg.AZURE_CONTAINER_BRONZE, path)
    assert loaded_df is not None
    assert not loaded_df.empty
    assert loaded_df.iloc[0]['tp_mean_est'] == 50.0

