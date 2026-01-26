import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from tasks.earnings_data import silver_earnings_data as silver
from core.pipeline import DataPaths

def test_process_file_success():
    """
    Verifies process_file:
    1. Reads Bronze raw bytes (mocked)
    2. Cleans/normalizes
    3. Merges with history (mocked)
    4. Writes back to Silver (mocked)
    """
    blob_name = "earnings-data/TEST.json"
    
    # Mock bronze data
    bronze_json = '[{"Date": "2023-01-01", "Reported EPS": 1.5}]'
    
    # Mock history
    mock_history = pd.DataFrame([
        {"Date": pd.Timestamp("2022-01-01"), "Reported EPS": 1.0, "Symbol": "TEST"}
    ])
    
    with patch('core.core.read_raw_bytes', return_value=bronze_json.encode('utf-8')), \
         patch('core.delta_core.load_delta', return_value=mock_history), \
         patch('core.delta_core.store_delta') as mock_store:
         
        res = silver.process_file(blob_name)
        
        assert res is True
        mock_store.assert_called_once()
        df_saved = mock_store.call_args[0][0]
        
        # Should have 2 rows (old + new)
        assert len(df_saved) == 2
        assert "TEST" in df_saved["Symbol"].values

def test_process_file_bad_json():
    blob_name = "earnings-data/BAD.json"
    with patch('core.core.read_raw_bytes', return_value=b'bad json'):
        res = silver.process_file(blob_name)
        assert res is False
