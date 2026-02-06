import pytest
import pandas as pd

from unittest.mock import patch
from datetime import datetime

from tasks.technical_analysis import technical_indicators as gc


@pytest.fixture
def sample_ohlcv_doji():
    """Returns a DataFrame that produces a Doji candle."""
    data = {
        "date": [datetime(2023, 1, 1)],
        "symbol": ["TEST"],
        "open": [100.0],
        "high": [105.0],
        "low": [95.0],
        "close": [100.05],  # Very small body
        "volume": [1000]
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_ohlcv_engulfing():
    """Returns a DataFrame with a Bullish Engulfing pattern."""
    data = {
        "date": [
            datetime(2022, 12, 30), # Context: Downtrend (added history)
            datetime(2022, 12, 31), # Context: Downtrend (added history)
            datetime(2023, 1, 1), # Context: Downtrend
            datetime(2023, 1, 2), # Context: Downtrend
            datetime(2023, 1, 3), # Candle 1: Bearish
            datetime(2023, 1, 4)  # Candle 2: Bullish Engulfing
        ],
        "symbol": ["TEST"] * 6,
        "open":  [115.0, 112.0, 110.0, 108.0, 105.0, 100.0],
        "high":  [116.0, 114.0, 112.0, 110.0, 106.0, 107.0],
        "low":   [112.0, 110.0, 108.0, 100.0, 100.0, 99.0],
        "close": [112.0, 110.0, 108.0, 105.0, 101.0, 106.0], 
        # Candle 1 (1/3): Open 105, Close 101 (Bearish)
        # Candle 2 (1/4): Open 100, Close 106 (Bullish) -> Fully engulfs
        "volume": [1000] * 6
    }
    return pd.DataFrame(data)

def test_compute_features_doji(sample_ohlcv_doji):
    df = gc.compute_features(sample_ohlcv_doji)
    row = df.iloc[0]
    
    # Assert Doji flag is set
    assert row["pat_doji"] == 1
    assert row["range"] == 10.0
    assert row["body"] == pytest.approx(0.05)

def test_compute_features_bullish_engulfing(sample_ohlcv_engulfing):
    df = gc.compute_features(sample_ohlcv_engulfing)
    
    # Check last row for pattern
    row = df.iloc[-1]
    
    assert row["pat_bullish_engulfing"] == 1
    assert row["is_bull"] == 1

def test_process_ticker_flow():
    """Test the end-to-end flow of _process_ticker with mocks."""
    ticker = "TEST_TICKER"
    raw_path = "market/silver/TEST_TICKER"
    gold_path = "technical-analysis/TEST_TICKER"
    silver_container = "silver-test"
    gold_container = "gold-test"
    
    # Mock data
    mock_df = pd.DataFrame({
        "date": [datetime(2023, 1, 1)],
        "symbol": [ticker],
        "open": [100.0],
        "high": [110.0],
        "low": [90.0],
        "close": [105.0],
        "volume": [1000]
    })
    
    with patch("core.delta_core.load_delta") as mock_load, \
         patch("core.delta_core.store_delta") as mock_store:
        
        mock_load.return_value = mock_df
        
        task = (ticker, raw_path, gold_path, silver_container, gold_container)
        result = gc._process_ticker(task)
        
        assert result["status"] == "ok"
        assert result["ticker"] == ticker
        assert result["rows"] == 1
        
        mock_load.assert_called_once_with(silver_container, raw_path)
        mock_store.assert_called_once()
        
        # Verify stored DF has features
        args, _ = mock_store.call_args
        stored_df = args[0]
        assert "pat_doji" in stored_df.columns
        assert "atr_14d" in stored_df.columns

def test_process_ticker_no_data():
    """Test _process_ticker handles missing/empty data gracefully."""
    with patch("core.delta_core.load_delta") as mock_load:
        mock_load.return_value = None # Or empty DF
        
        task = ("TEST", "path", "path", "c1", "c2")
        result = gc._process_ticker(task)
        
        assert result["status"] == "skipped_no_data"

def test_snake_case_conversion():
    assert gc._to_snake_case("Adj Close") == "adj_close"
    assert gc._to_snake_case("Volume") == "volume"
    assert gc._to_snake_case("Typical Price") == "typical_price"
