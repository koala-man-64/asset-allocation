import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from tasks.common import materialization

def test_load_ticker_universe():
    # Mock mdc.get_symbols
    mock_df = pd.DataFrame({'Symbol': ['AAPL', 'BRK.B', 'GOOG']})
    
    with patch('core.core.get_symbols', return_value=mock_df):
        tickers = materialization.load_ticker_universe()
        
    assert 'AAPL' in tickers
    assert 'GOOG' in tickers
    assert 'BRK.B' not in tickers # Should be excluded or normalized?
    # Logic was: if "." in symbol: continue
    assert len(tickers) == 2

def test_materialize_by_date():
    # Mock load_delta to return some frames
    mock_df_1 = pd.DataFrame({
        "Date": ["2023-01-15"], 
        "val": [1], 
        "Symbol": "A"
    })
    mock_df_2 = pd.DataFrame({
        "Date": ["2023-02-15"], # Out of range for jan
        "val": [2],
        "Symbol": "B"
    })
    
    container = "test-container"
    output_path = "test-output"
    year_month = "2023-01"
    
    with patch('tasks.common.materialization.load_ticker_universe', return_value=["A", "B"]), \
         patch('core.delta_core.load_delta', side_effect=[mock_df_1, mock_df_2]), \
         patch('core.delta_core.store_delta') as mock_store:
         
        res = materialization.materialize_by_date(
            container=container,
            output_path=output_path,
            year_month=year_month,
            get_source_path_func=lambda t: f"path/{t}"
        )
        
        assert res == 0
        mock_store.assert_called_once()
        df_out = mock_store.call_args[0][0]
        
        assert len(df_out) == 1
        assert df_out.iloc[0]["Symbol"] == "A"
        assert df_out.iloc[0]["year_month"] == "2023-01"
