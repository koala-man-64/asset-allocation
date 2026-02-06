import pytest
from unittest.mock import patch
import pandas as pd
from tasks.market_data.materialize_silver_market_by_date import (
    MaterializeConfig,
    materialize_silver_market_by_date,
    _parse_year_month_bounds
)

@pytest.fixture
def mock_df():
    return pd.DataFrame({
        "Date": pd.to_datetime(["2024-01-01", "2024-01-15", "2024-02-01"]),
        "Close": [100.0, 105.0, 110.0]
    })

def test_parse_year_month_bounds():
    start, end = _parse_year_month_bounds("2024-01")
    assert start == pd.Timestamp("2024-01-01")
    assert end == pd.Timestamp("2024-02-01")

@patch("tasks.market_data.materialize_silver_market_by_date._try_load_tickers_from_silver_container")
@patch("tasks.market_data.materialize_silver_market_by_date.load_delta")
@patch("tasks.market_data.materialize_silver_market_by_date.store_delta")
def test_materialize_silver_market_by_date_success(mock_store, mock_load, mock_list, mock_df):
    # Setup
    mock_list.return_value = ["AAPL"]
    mock_load.return_value = mock_df
    
    cfg = MaterializeConfig(
        container="test-container",
        year_month="2024-01",
        output_path="test/output",
        max_tickers=None
    )
    
    # Execute
    result = materialize_silver_market_by_date(cfg)
    
    # Assert
    assert result == 0
    assert mock_store.called
    stored_df = mock_store.call_args[0][0]
    # Should only have Jan 2024 dates
    assert len(stored_df) == 2
    assert (stored_df["year_month"] == "2024-01").all()

@patch("tasks.market_data.materialize_silver_market_by_date._try_load_tickers_from_silver_container")
def test_materialize_silver_market_by_date_no_tickers(mock_list):
    mock_list.return_value = []
    
    cfg = MaterializeConfig(
        container="test-container",
        year_month="2024-01",
        output_path="test/output",
        max_tickers=None
    )
    
    result = materialize_silver_market_by_date(cfg)
    assert result == 0
