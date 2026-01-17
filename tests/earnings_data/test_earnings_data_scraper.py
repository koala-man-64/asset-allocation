
import pytest
import pandas as pd
import uuid
import os
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from scripts.earnings_data import silver_earnings_data as earn_core
from scripts.earnings_data import config as cfg
from scripts.common import core as mdc
from scripts.common import delta_core

# --- Helpers ---

@pytest.fixture
def unique_ticker():
    return f"TEST_EARN_{uuid.uuid4().hex[:8].upper()}"

@pytest.fixture
def storage_cleanup(unique_ticker):
    """
    Mock cleanup (no-op for unit test with mocks).
    """
    yield unique_ticker
    print(f"\n[Mock] Cleaned up storage for {unique_ticker}")

# --- Integration Tests ---

@pytest.mark.asyncio
@patch('scripts.earnings_data.silver_earnings_data.pl.get_yahoo_earnings_data')
@patch('scripts.common.core.get_storage_client')
@patch('scripts.common.delta_core.write_deltalake')
@patch('scripts.common.delta_core._ensure_container_exists')
async def test_earnings_migration_integration(mock_ensure_container, mock_write_delta, mock_get_storage, mock_get_data, unique_ticker, storage_cleanup):
    """
    Verifies the new earnings scraper loop with MOCKED storage:
    1. Checks freshness (mocked).
    2. Calls Playwright helper (mocked).
    3. Saves to Delta (mocked via write_deltalake).
    """
    # Setup Mock Client (still needed for whitelist/blacklist checks)
    mock_client = MagicMock()
    # Mock file_exists to return False so it thinks it needs to fetch
    mock_client.file_exists.return_value = False
    mock_client.read_parquet.return_value = None
    mock_client.list_files.return_value = []
    
    # Setup mock for csv operations (whitelist/blacklist)
    mock_client.read_csv.return_value = pd.DataFrame(columns=['Symbol'])

    mock_get_storage.return_value = mock_client
    
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
    with patch('scripts.earnings_data.silver_earnings_data.pl.get_playwright_browser') as mock_browser_init:
        # Mock browser components
        mock_page = AsyncMock()
        mock_context = AsyncMock()
        mock_context.new_page.return_value = mock_page
        mock_browser = AsyncMock()
        mock_playwright = AsyncMock()
        
        mock_browser_init.return_value = (mock_playwright, mock_browser, mock_context, mock_page)
        
        # Mock the new centralized authentication
        with patch('scripts.earnings_data.silver_earnings_data.pl.authenticate_yahoo_async') as mock_auth, \
             patch('scripts.earnings_data.config.DEBUG_SYMBOLS', []):
             await earn_core.run_earnings_refresh(df_symbols)

    # 4. Verify Cloud Persistence (Mock calls)
    cloud_path = f"{symbol}"
    print(f"Verifying write to {cloud_path}...")
    
    # Assert get_storage_client was called
    mock_get_storage.assert_called()
    
    # Assert delta write happened via write_deltalake
    assert mock_write_delta.called, "write_deltalake should have been called"
    
    # Verify arguments to ensure correct path/symbol
    args, kwargs = mock_write_delta.call_args
    # args[0] is uri
    uri_arg = args[0]
    assert symbol in uri_arg, f"Expected {symbol} in write_deltalake URI: {uri_arg}"
    
    print("Test passed (Write to storage verified via mock).")

