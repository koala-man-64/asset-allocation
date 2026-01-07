import numpy as np
import pandas as pd
import pytest

from scripts.finance_data.feature_generator import compute_features

def _make_finance_df(rows: int = 8) -> pd.DataFrame:
    # Create quarterly dates
    dates = pd.date_range("2020-01-01", periods=rows, freq="3MS")
    symbol = ["AAPL"] * rows
    
    # Base metrics
    revenue = np.linspace(100, 200, rows)
    net_income = revenue * 0.2
    op_cash_flow = revenue * 0.25
    total_assets = np.full(rows, 1000)
    total_debt = np.full(rows, 300)
    current_assets = np.full(rows, 500)
    current_liabilities = np.full(rows, 250)
    shares = np.full(rows, 100)
    
    return pd.DataFrame({
        "Date": dates,
        "Symbol": symbol,
        "Total Revenue": revenue,
        "Net Income": net_income,
        "Operating Cash Flow": op_cash_flow,
        "Total Assets": total_assets,
        "Long Term Debt": total_debt,
        "Current Assets": current_assets,
        "Current Liabilities": current_liabilities,
        "Shares Outstanding": shares,
        "Gross Profit": revenue * 0.4,
    })

def test_compute_features_adds_expected_columns():
    df = _make_finance_df(8)
    out = compute_features(df)
    
    expected_cols = {
        "rev_qoq", "rev_yoy", "net_inc_yoy", 
        "gross_margin", "op_margin", 
        "piotroski_f_score", "roa_ttm"
    }
    assert expected_cols.issubset(set(out.columns))
    assert len(out) == 8

def test_piotroski_score_calculation():
    # Construct a scenario where we can predict the score
    # We need at least 5 rows to have 1 period of history (row 0) and 1 period of TTM (row 4 vs row 0)
    # Actually TTM needs 4 quarters. 
    # Let's create a DataFrame where metrics improve year-over-year
    
    dates = pd.date_range("2020-01-01", periods=8, freq="3MS")
    
    # Make everything improve
    base_assets = 1000.0
    
    df = pd.DataFrame({
        "Date": dates,
        "Symbol": ["TEST"] * 8,
        # TTM Sums need rolling 4
        # Period 0-3 sum vs Period 4-7 sum
        "Net Income": [10] * 8, # Positive ROA
        "Total Assets": [1000] * 8,
        "Operating Cash Flow": [20] * 8, # > Net Income (Accruals)
        "Long Term Debt": [500, 500, 500, 500, 400, 400, 400, 400], # Decreasing leverage
        "Current Assets": [200, 200, 200, 200, 300, 300, 300, 300], # Improving liquidity
        "Current Liabilities": [100] * 8,
        "Shares Outstanding": [100] * 8, # No new shares
        "Gross Profit": [50, 50, 50, 50, 60, 60, 60, 60], # Improving margins
        "Total Revenue": [100] * 8,
    })
    
    out = compute_features(df)
    
    # Check the last row
    # ROA positive: YES (10 > 0)
    # CFO positive: YES (20 > 0)
    # Delta ROA: Equal (0.01 vs 0.01) -> NO (Strict greater?) Logic says > lag. 
    #   roa_ttm = 40 / 1000 = 0.04. 
    #   lag(4) = 40 / 1000 = 0.04.
    #   0.04 > 0.04 is False.
    # Accruals: CFO > NI check. 20 > 10. Yes.
    # Leverage: 400 < 500. Yes.
    # Liquidity: 300/100 > 200/100. Yes.
    # Shares: 100 <= 100. Yes.
    # Gross Margin: 60/100 > 50/100. Yes.
    # Asset Turnover: 400/1000 > 400/1000. No (Equal).
    
    # Expected: 
    # pos_roa (1) + pos_cfo (1) + delta_roa (0) + accruals (1) + 
    # lev (1) + liq (1) + shares (1) + margin (1) + turnover (0)
    # Total = 7
    
    last = out.iloc[-1]
    
    assert last["piotroski_roa_pos"] == 1
    assert last["piotroski_cfo_pos"] == 1
    assert last["piotroski_leverage_decrease"] == 1
    
    # We expect a valid score
    assert 0 <= last["piotroski_f_score"] <= 9

def test_missing_required_columns():
    with pytest.raises(ValueError, match="Missing required columns"):
        compute_features(pd.DataFrame({"Date": ["2020-01-01"]}))

def test_parse_human_number_integration():
    # compute_features uses _resolve_column -> _coerce_numeric -> _parse_human_number
    # Verify it handles "10M" etc via the dataframe path
    df = pd.DataFrame({
        "Date": ["01/01/2020"], 
        "Symbol": ["AAPL"], 
        "Total Revenue": ["10M"],
        "Total Assets": [100]
    })
    out = compute_features(df)
    assert out.iloc[0]["total_revenue"] == 10_000_000.0
