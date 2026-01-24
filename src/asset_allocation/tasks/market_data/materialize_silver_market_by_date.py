"""
Materialize a cross-sectional (by-date) Delta table from per-ticker Silver market data tables.

Silver market data is stored per symbol (market-data/<ticker>). This script produces a single
table (default: market-data-by-date) that is partitioned by year_month and Date, enabling
efficient cross-sectional queries.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import pandas as pd

from asset_allocation.core.core import write_line, write_warning
from asset_allocation.core.delta_core import load_delta, store_delta
from asset_allocation.core.pipeline import DataPaths


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
    from asset_allocation.core import core as mdc

    df_symbols = mdc.get_symbols()
    df_symbols = df_symbols.dropna(subset=["Symbol"]).copy()

    tickers: List[str] = []
    for symbol in df_symbols["Symbol"].astype(str).tolist():
        if "." in symbol:
            continue
        tickers.append(symbol.replace(".", "-"))

    return list(dict.fromkeys(tickers))


def _extract_tickers_from_market_data_blobs(blob_names: Iterable[str]) -> List[str]:
    """
    Extract tickers that have a valid Delta log present under `market-data/<ticker>/_delta_log/`.

    This avoids attempting to read symbols that have no Silver Delta table (or only an empty placeholder),
    which otherwise triggers noisy delta-rs "No files in log segment" warnings.
    """

    tickers: set[str] = set()
    for blob_name in blob_names:
        parts = str(blob_name).strip("/").split("/")
        if len(parts) < 4:
            continue
        if parts[0] != "market-data":
            continue

        ticker = parts[1].strip()
        if not ticker:
            continue

        if parts[2] != "_delta_log":
            continue

        log_file = parts[3]
        if log_file.endswith(".json") or log_file.endswith(".checkpoint.parquet"):
            tickers.add(ticker)

    return sorted(tickers)


def _try_load_tickers_from_silver_container(container: str) -> Optional[List[str]]:
    """
    Attempt to list tickers from the Silver container (preferred).

    Returns:
      - List[str] (possibly empty) when listing succeeds.
      - None when listing is unavailable (e.g., no list permissions / no client).
    """

    from asset_allocation.core import core as mdc

    client = mdc.get_storage_client(container)
    if client is None:
        return None

    try:
        blobs = client.container_client.list_blobs(name_starts_with="market-data/")
        return _extract_tickers_from_market_data_blobs(b.name for b in blobs)
    except Exception as exc:
        write_warning(
            f"Unable to list Silver market-data tables in container={container}: {exc}. "
            "Falling back to symbol universe."
        )
        return None


def _build_config(argv: Optional[List[str]]) -> MaterializeConfig:
    parser = argparse.ArgumentParser(
        description="Materialize Silver market data into a cross-sectional Delta table (partitioned by date)."
    )
    parser.add_argument("--container", help="Silver container (default: AZURE_CONTAINER_SILVER).")
    parser.add_argument("--year-month", required=True, help="Year-month partition to materialize (YYYY-MM).")
    parser.add_argument(
        "--output-path",
        default=DataPaths.get_market_data_by_date_path(),
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


def materialize_silver_market_by_date(cfg: MaterializeConfig) -> int:
    start, end = _parse_year_month_bounds(cfg.year_month)

    tickers_from_container = _try_load_tickers_from_silver_container(cfg.container)
    if tickers_from_container is None:
        tickers = _load_ticker_universe()
        ticker_source = "symbol_universe"
    else:
        tickers = tickers_from_container
        ticker_source = "silver_container"

    if cfg.max_tickers is not None:
        tickers = tickers[: cfg.max_tickers]

    write_line(
        f"Materializing silver market-data-by-date for {cfg.year_month}: container={cfg.container} "
        f"tickers={len(tickers)} ticker_source={ticker_source} output_path={cfg.output_path}"
    )

    if not tickers:
        write_line(f"No Silver market-data tables found (source={ticker_source}); nothing to materialize.")
        return 0

    frames = []
    for ticker in tickers:
        src_path = DataPaths.get_market_data_path(ticker)
        df = load_delta(cfg.container, src_path)
        if df is None or df.empty:
            continue

        date_col = "Date" if "Date" in df.columns else ("date" if "date" in df.columns else None)
        if not date_col:
            continue

        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
        df = df.dropna(subset=[date_col])
        if df.empty:
            continue

        df = df[(df[date_col] >= start) & (df[date_col] < end)]
        if df.empty:
            continue

        df["year_month"] = df[date_col].dt.strftime("%Y-%m")
        df = df[df["year_month"] == cfg.year_month]
        if df.empty:
            continue

        frames.append(df)

    if not frames:
        write_line(f"No Silver market rows found for {cfg.year_month}; nothing to materialize.")
        return 0

    out = pd.concat(frames, ignore_index=True)
    date_col = "Date" if "Date" in out.columns else "date"
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
    return materialize_silver_market_by_date(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
