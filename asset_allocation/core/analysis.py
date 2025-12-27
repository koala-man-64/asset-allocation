import numpy as np
import pandas as pd
from typing import Dict, Any, Optional

# --- Indicator Functions (Pandas) ---

def calculate_sma(pdf: pd.DataFrame, target_col: str, period: int, date_col: str = 'Date') -> pd.DataFrame:
    """
    Compute Simple Moving Average for a single symbol's dataframe.
    """
    pdf = pdf.sort_values(date_col, kind="mergesort")
    col_name = f"SMA_{target_col}_{period}"
    s = pd.to_numeric(pdf[target_col], errors='coerce')
    pdf[col_name] = s.rolling(window=period).mean().round(2)
    return pdf

def calculate_range_percent(
    pdf: pd.DataFrame, 
    target_col: str, 
    day_span: int, 
    date_col: str = 'Date'
) -> pd.DataFrame:
    """
    Compute Range Percent for a single symbol.
    Range% = (value - rolling_low) / (rolling_high - rolling_low) * 100
    """
    pdf = pdf.sort_values(date_col, kind="mergesort")
    col_name = f"Range%_{target_col}_{day_span}"
    
    s = pd.to_numeric(pdf[target_col], errors='coerce')
    roll_high = s.rolling(window=day_span, min_periods=1).max()
    roll_low  = s.rolling(window=day_span, min_periods=1).min()

    denom = (roll_high - roll_low).replace(0, np.nan)
    pct = ((s - roll_low) / denom) * 100.0

    pdf[col_name] = (
        pct.fillna(0.0)
           .clip(lower=0.0, upper=100.0)
           .astype(np.float64)
           .round(2)
    )
    return pdf

def calculate_bollinger_range_pct(
    pdf: pd.DataFrame,
    target_col: str,
    period: int,
    std_mult: float,
    lag_days: int = 0,
    as_percent: bool = True,
    date_col: str = 'Date'
) -> pd.DataFrame:
    """
    Compute Bollinger Band Range Percentage.
    """
    # Create column name
    k_str = f"{str(std_mult).rstrip('0').rstrip('.')}"
    col_name = f"BB_RangePct_{target_col}_{period}_{k_str}x"
    if lag_days:
        col_name += f"_Lag{lag_days}"

    pdf = pdf.sort_values(date_col, kind="mergesort")
    s = pd.to_numeric(pdf[target_col], errors='coerce')

    mid = s.rolling(window=period, min_periods=period).mean()
    std = s.rolling(window=period, min_periods=period).std(ddof=1)
    # up  = mid + std_mult * std  # Unused explicitly, just need range
    # low = mid - std_mult * std
    
    # Range = (Up - Low) = (Mid + k*Std) - (Mid - k*Std) = 2*k*Std
    # Wait, the original code calculates (Upper - Lower) / Price
    # Upper - Lower = 2 * k * std
    
    rng = 2 * std_mult * std # Optimization
    
    with np.errstate(divide='ignore', invalid='ignore'):
        pct = rng / s
        if as_percent:
            pct = pct * 100.0

    if lag_days:
        pct = pct.shift(lag_days)

    pdf[col_name] = pct.astype(float)
    return pdf

# --- Performance Metrics ---

def calculate_daily_returns(pdf: pd.DataFrame, close_col: str, date_col: str, symbol_col: str) -> pd.DataFrame:
    """
    Compute daily returns for a symbol.
    """
    pdf = pdf.sort_values(date_col, kind="mergesort")
    c = pd.to_numeric(pdf[close_col], errors='coerce')
    r = c.pct_change().round(3)
    return pd.DataFrame({
        symbol_col: pdf[symbol_col].values,
        date_col:   pdf[date_col].values,
        'Daily_Return': r.values
    })

def calculate_series_metrics(
    s: pd.Series, 
    label: str, 
    rf_annual: float = 0.04, 
    trading_days: int = 252
) -> Dict[str, Any]:
    """
    Compute standard return/risk metrics from a daily-return Series.
    """
    if s is None or s.empty:
        return {} # Or return NaNs as per original, but empty dict is cleaner for callers to handle/fill

    # Replace infinities
    s = s.replace([np.inf, -np.inf], 0)
    
    rf_daily = (1.0 + rf_annual)**(1.0 / trading_days) - 1.0
    n_days = int(s.shape[0])
    
    # Returns
    cum_return = float((1.0 + s).prod() - 1.0)
    ann_return = float((1.0 + cum_return) ** (trading_days / max(n_days, 1)) - 1.0)
    
    # Volatility
    mean_daily = float(s.mean())
    vol_daily = float(s.std(ddof=0))
    ann_vol = float(vol_daily * np.sqrt(trading_days)) if np.isfinite(vol_daily) else np.nan
    
    # Sharpe
    sharpe = float(((mean_daily - rf_daily) / vol_daily) * np.sqrt(trading_days)) if vol_daily > 0 else np.nan
    
    # Sortino
    downside = np.minimum(0.0, s - rf_daily)
    downside_std = float(pd.Series(downside).std(ddof=0))
    sortino = float(((mean_daily - rf_daily) / downside_std) * np.sqrt(trading_days)) if downside_std > 0 else np.nan
    
    # Drawdown
    equity = (1.0 + s).cumprod()
    rolling_max = equity.cummax()
    drawdown = equity / rolling_max - 1.0
    
    if not drawdown.empty:
        max_drawdown_val = float(drawdown.min())
        # max_dd_date = drawdown.idxmin()
    else:
        max_drawdown_val = np.nan
        
    calmar = float(ann_return / abs(max_drawdown_val)) if (max_drawdown_val is not None and max_drawdown_val < 0) else np.nan
    
    return {
        f"{label}_cum_return": round(cum_return, 6),
        f"{label}_ann_return": round(ann_return, 6),
        f"{label}_ann_vol": round(ann_vol, 6),
        f"{label}_sharpe": round(sharpe, 6),
        f"{label}_sortino": round(sortino, 6),
        f"{label}_max_drawdown": round(max_drawdown_val, 6) if np.isfinite(max_drawdown_val) else np.nan,
        f"{label}_calmar": round(calmar, 6) if np.isfinite(calmar) else np.nan,
        f"{label}_hit_rate": round(float((s > 0).mean()), 6),
        f"{label}_best_day": round(float(s.max()), 6),
        f"{label}_worst_day": round(float(s.min()), 6),
        f"{label}_trading_days": int(n_days),
        f"{label}_start_used": s.index.min(),
        f"{label}_end_used": s.index.max(),
    }
