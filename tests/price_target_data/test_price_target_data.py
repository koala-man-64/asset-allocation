import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch, ANY
from scripts.price_target_data import price_target_data as pta

# --- Test Logic ---

@patch('scripts.price_target_data.price_target_data.mdc')
def test_transform_symbol_data(mock_mdc):
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
    # expected path: price_targets/TEST.parquet
    mock_mdc.store_parquet.assert_called()
    args, _ = mock_mdc.store_parquet.call_args
    assert "price_targets/TEST.parquet" in args[1]

@patch('scripts.price_target_data.price_target_data.mdc')
@patch('scripts.price_target_data.price_target_data.nasdaqdatalink')
def test_process_symbols_batch_fresh(mock_nasdaq, mock_mdc):
    # Scenario: Symbol is fresh in cloud, should NOT call API
    
    # Mock stale check: return a recent date
    mock_mdc.storage_client.get_last_modified.return_value = pd.Timestamp.utcnow()
    
    # Mock load_parquet to return something
    mock_mdc.load_parquet.return_value = pd.DataFrame({'ticker': ['TEST'], 'obs_date': [pd.Timestamp('2023-01-01')]})
    
    res = pta.process_symbols_batch(['TEST'])
    
    assert len(res) == 1
    mock_nasdaq.get_table.assert_not_called()

@patch('scripts.price_target_data.price_target_data.mdc')
@patch('scripts.price_target_data.price_target_data.nasdaqdatalink')
def test_process_symbols_batch_stale(mock_nasdaq, mock_mdc):
    # Scenario: Symbol is stale/missing, SHOULD call API
    
    # Mock stale check: return None (missing file) or old date
    mock_mdc.storage_client.get_last_modified.return_value = None
    mock_mdc.load_parquet.return_value = None 
    
    # Mock API return
    mock_api_df = pd.DataFrame({
        'ticker': ['TEST'],
        'obs_date': [pd.Timestamp('2023-01-01')],
        'tp_mean_est': [50.0]
    })
    mock_nasdaq.get_table.return_value = mock_api_df
    
    res = pta.process_symbols_batch(['TEST'])
    
    assert len(res) == 1
    assert res[0]['ticker'].iloc[0] == 'TEST'
    mock_nasdaq.get_table.assert_called()
