from __future__ import annotations

import pandas as pd
from typing import List, Callable, Optional, Iterable

from core import core as mdc
from core.core import write_line
from core.delta_core import load_delta, store_delta


def load_ticker_universe() -> List[str]:
    """
    Loads the list of symbols/tickers to process, excluding those with dots 
    (which usually indicate share classes or preferred stocks that might break paths).
    """
    df_symbols = mdc.get_symbols()
    df_symbols = df_symbols.dropna(subset=["Symbol"]).copy()

    tickers: List[str] = []
    for symbol in df_symbols["Symbol"].astype(str).tolist():
        if "." in symbol:
            continue
        tickers.append(symbol.replace(".", "-"))

    return list(dict.fromkeys(tickers))


def materialize_by_date(
    container: str,
    output_path: str,
    year_month: str,
    get_source_path_func: Callable[[str], str],
    date_col_candidates: List[str] = ["Date", "date"],
    ticker_col: str = "symbol",
    debug_symbols: Optional[List[str]] = None
) -> int:
    """
    Common logic to load many per-ticker Delta tables, filter by a specific month,
    consolidate them into a single DataFrame, and write to a partitioned Delta table.
    """
    
    # helper for bounds
    try:
        start = pd.Timestamp(f"{year_month}-01")
    except Exception as exc:
        raise ValueError(f"Invalid year_month '{year_month}'. Expected YYYY-MM.") from exc
    end = start + pd.offsets.MonthBegin(1)

    tickers = load_ticker_universe()
    if debug_symbols:
        tickers = [t for t in tickers if t in debug_symbols]
        write_line(f"DEBUG: Restricting materialization to {len(tickers)} symbols.")

    write_line(f"Materializing {year_month} for {len(tickers)} tickers -> {container}/{output_path}")

    frames = []
    
    # We could parallelize this load if needed, but for now serial is fine or use ProcessPool externally.
    # Given the IO nature, usually we sequentially verify presence.
    
    for ticker in tickers:
        src_path = get_source_path_func(ticker)
        
        # Optimization: verify exists? check_blob_exists? 
        # load_delta usually handles missing gracefully if we catch or it returns None
        df = load_delta(container, src_path)
        
        if df is None or df.empty:
            continue

        # Find date col
        valid_date_col = next((c for c in date_col_candidates if c in df.columns), None)
        if not valid_date_col:
            continue

        df = df.copy()
        df[valid_date_col] = pd.to_datetime(df[valid_date_col], errors="coerce").dt.normalize()
        df = df.dropna(subset=[valid_date_col])
        
        if df.empty:
            continue

        # Filter by date range
        mask = (df[valid_date_col] >= start) & (df[valid_date_col] < end)
        df = df[mask]
        
        if df.empty:
            continue

        # Inject symbol if missing
        if ticker_col not in df.columns and "Symbol" not in df.columns:
            df[ticker_col] = ticker

        # Ensure we have the partition col
        # If year_month is not in cols, add it? partition_by needs it in data?
        # Typically yes.
        df["year_month"] = year_month 
        
        frames.append(df)

    if not frames:
        write_line(f"No rows found for {year_month}; nothing to materialize.")
        return 0

    out = pd.concat(frames, ignore_index=True)
    
    # We need a primary date col for partitioning if desired
    # The caller typically expects the output to match the input date semantics
    # If we normalized to `valid_date_col`, we should standardize the name if we want consistent schema
    # But for flexibility, let's keep the found column or standardize to 'date' if requested. 
    # Current existing scripts keep original case (Date vs date).
    
    # Find the date col again in concatenated frame
    out_date_col = next((c for c in date_col_candidates if c in out.columns), "date")
    
    predicate = f"year_month = '{year_month}'"

    store_delta(
        out,
        container=container,
        path=output_path,
        mode="overwrite",
        partition_by=["year_month", out_date_col],
        merge_schema=True,
        predicate=predicate,
    )

    write_line(f"Materialized {len(out)} row(s) into {container}/{output_path} ({year_month}).")
    return 0
