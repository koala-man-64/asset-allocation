import pytest
import pandas as pd
from tasks.earnings_data import gold_earnings_data as gold

def test_compute_features():
    """
    Verifies compute_features:
    1. Snake-casing columns.
    2. Calculating surprise %.
    3. Resampling to daily with ffill.
    4. Identifying earnings days.
    """
    df_raw = pd.DataFrame({
        "Date": ["2023-01-01", "2023-04-01"],
        "Symbol": ["TEST", "TEST"],
        "Reported EPS": [1.1, 1.2],
        "EPS Estimate": [1.0, 1.0],
    })
    
    res = gold.compute_features(df_raw)
    
    # Check Result
    # 1. Columns snake_cased
    assert "reported_eps" in res.columns
    assert "surprise_pct" in res.columns
    
    # 2. Daily resampling
    # 2023-01-01 to 2023-04-01 is ~90 days
    assert len(res) >= 90 
    
    # 3. Validation of a specific date
    row_jan1 = res[res["date"] == pd.Timestamp("2023-01-01")].iloc[0]
    assert row_jan1["is_earnings_day"] == 1.0
    assert row_jan1["surprise_pct"] == (1.1 - 1.0) / 1.0 # 0.1
    
    row_jan2 = res[res["date"] == pd.Timestamp("2023-01-02")].iloc[0]
    assert row_jan2["is_earnings_day"] == 0.0
    # Should be ffilled
    assert row_jan2["reported_eps"] == 1.1

def test_compute_features_missing_cols():
    df_raw = pd.DataFrame({"Date": []})
    with pytest.raises(ValueError, match="Missing required columns"):
        gold.compute_features(df_raw)
