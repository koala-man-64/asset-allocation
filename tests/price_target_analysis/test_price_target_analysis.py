import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch, ANY
from scripts.price_target_analysis import price_target_analysis as pta

# --- Test Utilities ---

def test_calculate_null_percentage():
    df = pd.DataFrame({
        'A': [1, 2, np.nan, 4],
        'B': [1, np.inf, 3, 4],
        'C': [1, 2, 3, 4]
    })
    
    # Capture output
    with patch('scripts.common.core.write_line') as mock_write:
        res = pta.calculate_null_percentage(df)
        
        # A: 1/4 = 25%
        # B: 1/4 = 25% (inf)
        # C: 0%
        
        assert res['A'] == 25.0
        assert res['B'] == 25.0
        assert res['C'] == 0.0

def test_normalize_column():
    s = pd.Series([10, 20, 30])
    res = pta.normalize_column(s)
    # expected: 0.0, 0.5, 1.0
    pd.testing.assert_series_equal(res, pd.Series([0.0, 0.5, 1.0]))
    
    # Test flat case
    s_flat = pd.Series([10, 10, 10])
    res_flat = pta.normalize_column(s_flat)
    pd.testing.assert_series_equal(res_flat, s_flat)

def test_remove_outliers():
    df = pd.DataFrame({
        'val': [1, 2, 3, 100] # 100 is outlier
    })
    cleaned = pta.remove_outliers(df, ['val'])
    assert len(cleaned) == 3
    assert 100 not in cleaned['val'].values

# --- Test Logic ---

@patch('scripts.price_target_analysis.price_target_exploration.nasdaqdatalink')
def test_fetch_and_save_target_price_data(mock_nasdaq, tmp_path):
    mock_nasdaq.get_table.return_value = pd.DataFrame({'obs_date': ['2023-01-01'], 'val': [100]})
    
    csv = tmp_path / "test.csv"
    res = pta.fetch_and_save_target_price_data('TEST', str(csv))
    
    assert not res.empty
    assert csv.exists()
    
    # Verify content
    saved = pd.read_csv(csv)
    assert len(saved) == 1

@patch('scripts.price_target_analysis.price_target_exploration.CSV_FOLDER')
@patch('scripts.price_target_analysis.price_target_exploration.nasdaqdatalink')
def test_process_symbol_new_data(mock_nasdaq, mock_csv_folder, tmp_path):
    # Setup mock folder
    mock_csv_folder.__truediv__.side_effect = lambda x: tmp_path / x
    
    # Mock API return
    dates = pd.date_range('2023-01-01', periods=3)
    mock_df = pd.DataFrame({
        'ticker': ['TEST']*3,
        'obs_date': dates,
        'tp_mean_est': [10, 11, 12]
        # other cols will be filled with nan by logic
    })
    mock_nasdaq.get_table.return_value = mock_df
    
    res = pta.process_symbol('TEST')
    
    assert res is not None
    assert len(res) == 3
    assert 'tp_mean_est' in res.columns
    # Check if file created
    assert (tmp_path / "TEST.csv").exists()

@patch('scripts.price_target_analysis.price_target_exploration.CSV_FOLDER')
def test_process_symbol_existing_fresh(mock_csv_folder, tmp_path):
    # Setup fresh existing file
    f = tmp_path / "TEST.csv"
    f.touch()
    
    # Dummy data
    pd.DataFrame({'a': [1]}).to_csv(f)
    
    mock_csv_folder.__truediv__.return_value = f
    
    # Should load local and NOT call API
    with patch('scripts.price_target_analysis.price_target_exploration.nasdaqdatalink') as mock_nasdaq:
        res = pta.process_symbol('TEST')
        mock_nasdaq.get_table.assert_not_called()
        assert not res.empty

