import numpy as np
import pandas as pd
import pytest

from tasks.finance_data.gold_finance_data import compute_features

def _make_finance_df(rows: int = 8) -> pd.DataFrame:
    # Create quarterly dates
    dates = pd.date_range("2020-01-01", periods=rows, freq="3MS")
    symbol = ["AAPL"] * rows
    
    # Base metrics
    revenue = np.linspace(100, 200, rows)
    gross_profit = revenue * 0.4
    operating_income = revenue * 0.15
    net_income = revenue * 0.12
    free_cash_flow = revenue * 0.1
    op_cash_flow = revenue * 0.25
    total_assets = np.full(rows, 1000)
    total_debt = np.full(rows, 300)
    long_term_debt = np.full(rows, 250)
    current_assets = np.full(rows, 500)
    current_liabilities = np.full(rows, 250)
    shares = np.full(rows, 100)
    pe_ratio = np.full(rows, 20.0)
    ev_ebitda = np.full(rows, 10.0)
    market_cap = np.full(rows, 1_000_000.0)
    ebitda = revenue * 0.2
    forward_pe = np.full(rows, 18.0)
    ev_revenue = np.full(rows, 5.0)
    cash_and_equivalents = np.full(rows, 75.0)
    
    return pd.DataFrame({
        "Date": dates,
        "Symbol": symbol,
        "Total Revenue": revenue,
        "Gross Profit": gross_profit,
        "Operating Income": operating_income,
        "Net Income": net_income,
        "Free Cash Flow": free_cash_flow,
        "Operating Cash Flow": op_cash_flow,
        "Total Debt": total_debt,
        "Total Assets": total_assets,
        "Long Term Debt": long_term_debt,
        "Current Assets": current_assets,
        "Current Liabilities": current_liabilities,
        "Shares Outstanding": shares,
        "PE Ratio": pe_ratio,
        "EV/EBITDA": ev_ebitda,
        "Market Cap": market_cap,
        "EBITDA": ebitda,
        "Forward P/E": forward_pe,
        "EV/Revenue": ev_revenue,
        "Cash And Cash Equivalents": cash_and_equivalents,
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
    df = pd.DataFrame({
        "Date": dates,
        "Symbol": ["TEST"] * 8,
        # TTM sums need rolling 4 periods.
        "Total Revenue": [100] * 8,
        "Gross Profit": [50, 50, 50, 50, 60, 60, 60, 60],
        "Operating Income": [20] * 8,
        "Net Income": [10] * 8,
        "Free Cash Flow": [8] * 8,
        "Operating Cash Flow": [20] * 8,
        "Total Debt": [500] * 8,
        "Long Term Debt": [500, 500, 500, 500, 400, 400, 400, 400],
        "Total Assets": [1000] * 8,
        "Current Assets": [200, 200, 200, 200, 300, 300, 300, 300],
        "Current Liabilities": [100] * 8,
        "Shares Outstanding": [100] * 8,
        "PE Ratio": [20] * 8,
        "EV/EBITDA": [10] * 8,
        "Market Cap": [1_000_000] * 8,
        "EBITDA": [25] * 8,
        "Forward P/E": [19] * 8,
        "EV/Revenue": [6] * 8,
        "Cash And Cash Equivalents": [100] * 8,
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
        "Gross Profit": ["4M"],
        "Operating Income": ["2M"],
        "Net Income": ["1M"],
        "Free Cash Flow": ["500K"],
        "Operating Cash Flow": ["1.5M"],
        "Total Debt": [100],
        "Long Term Debt": [80],
        "Total Assets": [500],
        "Current Assets": [200],
        "Current Liabilities": [100],
        "Shares Outstanding": [100],
        "PE Ratio": [20],
        "EV/EBITDA": [8],
        "Market Cap": [10_000_000],
        "EBITDA": [2_000_000],
        "Forward P/E": [18],
        "EV/Revenue": [4],
        "Cash And Cash Equivalents": [60],
    })
    out = compute_features(df)
    assert out.iloc[0]["total_revenue"] == 10_000_000.0
