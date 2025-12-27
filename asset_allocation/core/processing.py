import numpy as np
import pandas as pd
import dask.dataframe as dd
from typing import Callable, Optional, Union, Dict, Any

# --- Dataframe Operations ---

def _gb_apply(gb, func, meta, **kwargs):
    """
    Helper to handle groupby.apply across different pandas/dask versions.
    Forces include_groups=False (or logic to handle it) if needed, 
    but for now we wrap the simple call.
    """
    # Dask's apply usually needs include_groups=False in newer pandas, 
    # but Dask itself manages this. 
    # We will pass kwargs through.
    return gb.apply(func, meta=meta, **kwargs)

def apply_to_symbols(
    ddf: dd.DataFrame, 
    func: Callable, 
    meta: Any, 
    symbol_col: str = 'Symbol', 
    date_col: str = 'Date',
    sort_within_partition: bool = True
) -> dd.DataFrame:
    """
    Apply a function to each symbol's data. 
    
    1. Shuffles by Symbol.
    2. Groups by Symbol.
    3. Applies func (which receives a pandas DataFrame).
    4. Resets index.
    5. Optionally sorts partitions by Date.
    """
    # Shuffle to ensure all rows for a symbol are in the same partition
    shuffled = ddf.shuffle(symbol_col)
    
    # Apply
    # Note: include_groups=True is often required for legacy code if the func expects the grouping col
    processed = _gb_apply(
        shuffled.groupby(symbol_col), 
        func, 
        meta=meta,
        include_groups=True 
    ).reset_index(drop=True)
    
    if sort_within_partition:
        # We assume the output has the grouping columns
        processed = processed.map_partitions(
            lambda pdf: pdf.sort_values([symbol_col, date_col], kind="mergesort") 
            if symbol_col in pdf.columns and date_col in pdf.columns else pdf
        )
        
    return processed

def filter_by_date(
    df: Union[pd.DataFrame, dd.DataFrame],
    start_date: Optional[Union[str, pd.Timestamp]] = None,
    end_date: Optional[Union[str, pd.Timestamp]] = None,
    date_col: str = 'Date'
):
    """
    Filter DataFrame by date range.
    """
    if start_date:
        df = df[df[date_col] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df[date_col] <= pd.to_datetime(end_date)]
    return df

def align_weights_processing(
    daily_returns_df: dd.DataFrame,
    fundamentals_df: dd.DataFrame,
    weight_col: str,
    symbol_col: str = 'Symbol',
    date_col: str = 'Date'
) -> pd.Series:
    """
    Calculate Value-Weighted Series using Daily Returns and Fundamentals.
    Arguments must be Dask DataFrames.
    Returns computed Pandas Series (Daily_Return).
    """
    # 1. Prepare fundamentals (slim, rename)
    wdf = fundamentals_df[[symbol_col, date_col, weight_col]].rename(columns={weight_col: 'Weight'})
    
    # 2. Forward fill weights within symbol
    def _ffill_weight(pdf):
        pdf = pdf.sort_values(date_col, kind="mergesort")
        pdf['Weight'] = pd.to_numeric(pdf['Weight'], errors='coerce').ffill()
        return pdf
    
    meta_w = wdf._meta.assign(Weight=np.float64())
    wdf_ff = apply_to_symbols(wdf, _ffill_weight, meta=meta_w, symbol_col=symbol_col, date_col=date_col)
    
    # 3. Merge
    merged = daily_returns_df.merge(wdf_ff, on=[symbol_col, date_col], how='left')
    
    # 4. Post-merge ffill (for dates in returns missing in fundamentals)
    meta_post = merged._meta.assign(Weight=np.float64())
    merged_ff = apply_to_symbols(merged, _ffill_weight, meta=meta_post, symbol_col=symbol_col, date_col=date_col)
    
    # 5. Compute Weighted Returns
    merged_ff = merged_ff.assign(
        WeightNum = dd.to_numeric(merged_ff['Weight'], errors='coerce'),
        WR = merged_ff['Daily_Return'] * dd.to_numeric(merged_ff['Weight'], errors='coerce')
    )
    
    sums = merged_ff.groupby(date_col)[['WR', 'WeightNum']].sum()
    vw_series = (sums['WR'] / sums['WeightNum']).rename('Daily_Return')
    
    return vw_series
