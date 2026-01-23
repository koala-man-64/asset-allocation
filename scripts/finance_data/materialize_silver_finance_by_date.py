"""
Materialize a cross-sectional (by-date) Delta table from per-ticker Silver finance data tables.

Partitioned by year_month and Date, enabling efficient cross-sectional queries.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import pandas as pd

from scripts.common.core import write_line, write_warning
from scripts.common.delta_core import load_delta, store_delta
from scripts.common.pipeline import DataPaths


@dataclass(frozen=True)
class MaterializeConfig:
    container: str
    year_month: str
    output_path: str
    max_tickers: Optional[int]


def _parse_year_month_bounds(year_month: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    try:
        start = pd.Timestamp(f"{year_month}-01")
    except Exception as exc:
        raise ValueError(f"Invalid year_month '{year_month}'. Expected YYYY-MM.") from exc
    end = start + pd.offsets.MonthBegin(1)
    return start, end


def _load_ticker_universe() -> List[str]:
    from scripts.common import core as mdc

    df_symbols = mdc.get_symbols()
    df_symbols = df_symbols.dropna(subset=["Symbol"]).copy()

    tickers: List[str] = []
    for symbol in df_symbols["Symbol"].astype(str).tolist():
        if "." in symbol:
            continue
        tickers.append(symbol.replace(".", "-"))

    return list(dict.fromkeys(tickers))


def _extract_finance_table_roots_from_blobs(blob_names: Iterable[str]) -> set[str]:
    """
    Extract per-table roots that have a Delta log present under `finance-data/<folder>/<table>/_delta_log/`.

    This is used to avoid attempting to read tables that do not exist, which otherwise triggers noisy delta-rs warnings.
    """

    roots: set[str] = set()
    for blob_name in blob_names:
        parts = str(blob_name).strip("/").split("/")
        if len(parts) < 5:
            continue
        if parts[0] != "finance-data":
            continue

        if parts[3] != "_delta_log":
            continue

        log_file = parts[4]
        if not (log_file.endswith(".json") or log_file.endswith(".checkpoint.parquet")):
            continue

        roots.add("/".join(parts[:3]))

    return roots


def _try_load_finance_table_roots_from_container(container: str) -> Optional[set[str]]:
    """
    Attempt to list existing Silver finance tables from the target container (preferred).

    Returns:
      - set[str] (possibly empty) when listing succeeds.
      - None when listing is unavailable (e.g., no list permissions / no client).
    """

    from scripts.common import core as mdc

    client = mdc.get_storage_client(container)
    if client is None:
        return None

    try:
        blobs = client.container_client.list_blobs(name_starts_with="finance-data/")
        return _extract_finance_table_roots_from_blobs(b.name for b in blobs)
    except Exception as exc:
        write_warning(
            f"Unable to list finance-data tables in container={container}: {exc}. "
            "Falling back to symbol universe."
        )
        return None


def _build_config(argv: Optional[List[str]]) -> MaterializeConfig:
    parser = argparse.ArgumentParser(
        description="Materialize Silver finance data into a cross-sectional Delta table (partitioned by date)."
    )
    parser.add_argument("--container", help="Silver container (default: AZURE_CONTAINER_SILVER).")
    parser.add_argument("--year-month", required=True, help="Year-month partition to materialize (YYYY-MM).")
    parser.add_argument(
        "--output-path",
        default=DataPaths.get_finance_by_date_path(),
        help="Output Delta table path within the container.",
    )
    parser.add_argument("--max-tickers", type=int, default=None, help="Optional limit for debugging.")
    args = parser.parse_args(argv)

    container_raw = args.container or os.environ.get("AZURE_CONTAINER_SILVER")
    if container_raw is None or not str(container_raw).strip():
        raise ValueError("Missing silver container. Set AZURE_CONTAINER_SILVER or pass --container.")
    container = str(container_raw).strip()

    max_tickers = int(args.max_tickers) if args.max_tickers is not None else None
    if max_tickers is not None and max_tickers <= 0:
        max_tickers = None

    return MaterializeConfig(
        container=container,
        year_month=str(args.year_month).strip(),
        output_path=str(args.output_path).strip().lstrip("/"),
        max_tickers=max_tickers,
    )


def materialize_silver_finance_by_date(cfg: MaterializeConfig) -> int:
    start, end = _parse_year_month_bounds(cfg.year_month)

    available_table_roots = _try_load_finance_table_roots_from_container(cfg.container)
    if available_table_roots is None:
        tickers = _load_ticker_universe()
        ticker_source = "symbol_universe"
    else:
        tickers = sorted({root.split("/")[2].split("_", 1)[0] for root in available_table_roots})
        ticker_source = "container_listing"

    if cfg.max_tickers is not None:
        tickers = tickers[: cfg.max_tickers]

    write_line(
        f"Materializing finance-data-by-date for {cfg.year_month}: container={cfg.container} "
        f"tickers={len(tickers)} ticker_source={ticker_source} output_path={cfg.output_path}"
    )

    if not tickers:
        write_line(f"No per-ticker finance tables found (source={ticker_source}); nothing to materialize.")
        return 0

    frames = []
    # Finance data is unique; multiple tables per ticker (balance sheet, income, etc.)
    # Or is it? silver_finance_data.py::process_blob saves to DataPaths.get_finance_path(folder_name, ticker, suffix)
    # This means there are potentially 4 tables per ticker:
    # 1. Income Statement
    # 2. Balance Sheet
    # 3. Cash Flow
    # 4. Valuation
    
    # We need to decide: Do we materialize ALL of them into one huge wide table? Or just specific ones?
    # Or do we iterate over the 4 known types and stack them?
    # However, 'materialize by date' usually implies a single schema.
    # If we merge them, we need to handle column name collisions (though they are usually distinct enough or we prefix).
    
    # But wait, Silver Finance is stored as:
    # finance-data/balance_sheet/AAPL_quarterly_balance-sheet
    # This is partitioned by "Folder" (Statement Type).
    
    # If we want a SINGLE "silver finance by date" table, we probably want to JOIN these 4 tables per ticker first, 
    # then stack across tickers.
    
    # Let's inspect DataPaths.get_finance_path usage again.
    # folder: e.g. 'Balance Sheet' -> 'balance_sheet'
    # path: finance-data/balance_sheet/AAPL_quarterly_balance-sheet
    
    # This makes materialization trickier than market data which is 1 table per ticker.
    # We have 4 tables per ticker.
    
    # Strategy:
    # Iterate Tickers
    #   For each ticker:
    #     Load Income, Balance, Cash, Valuation
    #     Merge on Date (Outer Join)
    #     Add Symbol
    #     Filter by Date Range
    #     Append to frames
    
    known_types = [
        ("Income Statement", "quarterly_financials"),
        ("Balance Sheet", "quarterly_balance-sheet"),
        ("Cash Flow", "quarterly_cash-flow"),
        ("Valuation", "quarterly_valuation_measures")
    ]
    
    for ticker in tickers:
        ticker_frames = []
        for folder_name, suffix in known_types:
            src_path = DataPaths.get_finance_path(folder_name, ticker, suffix)
            if available_table_roots is not None and src_path not in available_table_roots:
                continue
            df = load_delta(cfg.container, src_path)
            
            if df is None or df.empty:
                continue
                
            # Date/date col normalization
            date_col = "Date" if "Date" in df.columns else ("date" if "date" in df.columns else None)
            if not date_col:
                continue
                
            df = df.copy()
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
            df = df.dropna(subset=[date_col])
            
            # Filter range early to save memory?
            # Or merge first?
            # Filter early is safer for memory.
            df = df[(df[date_col] >= start) & (df[date_col] < end)]
            if df.empty:
                continue
            
            # Set index for merging
            df = df.set_index(date_col)
            
            # Prefix columns? To avoid collisions? 
            # e.g. "Total Assets" might be unique to BalSheet, but "Net Income" might appear in others?
            # Usually strict accounting types are distinct. 
            # But let's keep them as is for now, assuming standard Yahoo Finance schema.
            # actually, duplicates might exist.
            # let's suffix with type if needed? No, too complex for now.
            # Just drop 'Symbol' and 'symbol' from columns before merge if they exist in non-index
            if "Symbol" in df.columns: df = df.drop(columns=["Symbol"])
            if "symbol" in df.columns: df = df.drop(columns=["symbol"])
            
            ticker_frames.append(df)
            
        if not ticker_frames:
            continue
            
        # Merge all available statements for this ticker
        # outer join on Date
        df_merged = pd.concat(ticker_frames, axis=1) # aligning on Date index
        
        # Reset index to get Date back
        df_merged = df_merged.reset_index()
        df_merged["Symbol"] = ticker
        
        # Rename 'index' to 'Date' if needed, but concat usually preserves index name if set
        if "Date" not in df_merged.columns and df_merged.index.name == "Date":
             df_merged = df_merged.reset_index()
        
        # Ensure we have a clean Date column
        # If concat(axis=1) produced duplicate columns (e.g. same metric in multiple statements), pandas handles it by duplicate names or suffixes?
        # pd.concat(axis=1) allows duplicate columns. Delta might fail with duplicates.
        # We must deduplicate columns.
        df_merged = df_merged.loc[:, ~df_merged.columns.duplicated()]
        
        frames.append(df_merged)

    if not frames:
        write_line(f"No Silver finance rows found for {cfg.year_month}; nothing to materialize.")
        return 0

    out = pd.concat(frames, ignore_index=True)
    
    # Ensure Date column is standard
    date_col = "Date" if "Date" in out.columns else "date"
    # Fill gaps? (We already ffilled in silver_finance_data, so basic sparsity is handled per ticker)
    
    out["year_month"] = out[date_col].dt.strftime("%Y-%m")
    out = out[out["year_month"] == cfg.year_month]
    if out.empty:
        write_line(f"No rows remain after year_month filter for {cfg.year_month}; nothing to materialize.")
        return 0

    predicate = f"year_month = '{cfg.year_month}'"

    store_delta(
        out,
        container=cfg.container,
        path=cfg.output_path,
        mode="overwrite",
        partition_by=["year_month", date_col],
        merge_schema=True,
        predicate=predicate,
    )

    write_line(f"Materialized {len(out)} row(s) into {cfg.container}/{cfg.output_path} ({cfg.year_month}).")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    cfg = _build_config(argv)
    return materialize_silver_finance_by_date(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
