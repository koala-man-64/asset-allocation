import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch, ANY
from scripts.price_target_data import scraper as pta

# --- Test Logic ---

@patch('scripts.price_target_data.scraper.delta_core')
@patch('scripts.price_target_data.scraper.mdc')
def test_transform_symbol_data(mock_mdc, mock_delta_core):
    # Setup inputs
    symbol = "TEST"
    existing_df = pd.DataFrame()
    
    # Input Data (API format simulation)
    # The code expects 'obs_date'
    dates = pd.date_range('2023-01-01', periods=3)
    target_price_data = pd.DataFrame({
        'obs_date': dates, 
        'tp_mean_est': [10.0, 11.0, 12.0]
    })
    
    # Execution
    res = pta.transform_symbol_data(symbol, target_price_data, existing_df)
    
    # Assertions
    assert res is not None
    assert not res.empty
    assert 'tp_mean_est' in res.columns
    assert 'ticker' in res.columns
    assert res.iloc[0]['ticker'] == symbol
    
    # Verify save called
    # expected path: price_targets/TEST (directory)
    mock_delta_core.store_delta.assert_called()
    args, _ = mock_delta_core.store_delta.call_args
    # args[0] is df, args[1] is container, args[2] is path
    assert "price_targets/TEST" in args[2]

@patch('scripts.price_target_data.scraper.delta_core')
@patch('scripts.price_target_data.scraper.mdc')
@patch('scripts.price_target_data.scraper.nasdaqdatalink')
def test_process_symbols_batch_fresh(mock_nasdaq, mock_mdc, mock_delta_core):
    # Scenario: Symbol is fresh in cloud, should NOT call API
    
    # Mock fresh check: return a recent timestamp (seconds since epoch)
    now_ts = pd.Timestamp.utcnow().timestamp()
    mock_delta_core.get_delta_last_commit.return_value = now_ts
    
    # Mock load_delta to return something
    mock_delta_core.load_delta.return_value = pd.DataFrame({'ticker': ['TEST'], 'obs_date': [pd.Timestamp('2023-01-01')]})
    
    res = pta.process_symbols_batch(['TEST'])
    
    assert len(res) == 1
    mock_nasdaq.get_table.assert_not_called()

@patch('scripts.price_target_data.scraper.delta_core')
@patch('scripts.price_target_data.scraper.mdc')
@patch('scripts.price_target_data.scraper.nasdaqdatalink')
def test_process_symbols_batch_stale(mock_nasdaq, mock_mdc, mock_delta_core):
    # Scenario: Symbol is stale/missing, SHOULD call API
    
    # Mock stale check: return None (missing file)
    mock_delta_core.get_delta_last_commit.return_value = None
    mock_delta_core.load_delta.return_value = None 
    
    # Mock API return
    mock_api_df = pd.DataFrame({
        'ticker': ['TEST'],
        'obs_date': [pd.Timestamp('2023-01-01')],
        'tp_mean_est': [50.0]
    })
    mock_nasdaq.get_table.return_value = mock_api_df
    
    res = pta.process_symbols_batch(['TEST'])
    
    assert len(res) == 1
    assert res[0] == 'TEST'
    mock_nasdaq.get_table.assert_called()
    
    # Verify save was called implicitly via transform_symbol_data
    mock_delta_core.store_delta.assert_called()
