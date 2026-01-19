"""
Materialize a cross-sectional (by-date) Delta table from per-ticker finance feature tables.

Why:
- Per-ticker tables are convenient for lookups.
- Analysis often needs cross-sectional slices (all symbols for a given date).

This script builds a single Delta table partitioned by year_month and date.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd

from scripts.common.core import write_line
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


def _build_config(argv: Optional[List[str]]) -> MaterializeConfig:
    parser = argparse.ArgumentParser(
        description="Materialize finance features into a cross-sectional Delta table (partitioned by date)."
    )
    parser.add_argument("--container", help="Finance features container (default: AZURE_CONTAINER_GOLD).")
    parser.add_argument("--year-month", required=True, help="Year-month partition to materialize (YYYY-MM).")
    parser.add_argument(
        "--output-path",
        default=DataPaths.get_gold_finance_wide_path(),
        help="Output Delta table path within the container.",
    )
    parser.add_argument("--max-tickers", type=int, default=None, help="Optional limit for debugging.")
    args = parser.parse_args(argv)

    container = (args.container or os.environ.get("AZURE_CONTAINER_GOLD", "")).strip()
    if not container:
        # Fallback to FINANCE if GOLD is not set, as per some configs
        container = os.environ.get("AZURE_CONTAINER_FINANCE", "").strip()
        
    if not container:
        raise ValueError("Missing container. Set AZURE_CONTAINER_GOLD or AZURE_CONTAINER_FINANCE or pass --container.")

    max_tickers = int(args.max_tickers) if args.max_tickers is not None else None
    if max_tickers is not None and max_tickers <= 0:
        max_tickers = None

    return MaterializeConfig(
        container=container,
        year_month=str(args.year_month).strip(),
        output_path=str(args.output_path).strip().lstrip("/"),
        max_tickers=max_tickers,
    )


def materialize_finance_by_date(cfg: MaterializeConfig) -> int:
    start, end = _parse_year_month_bounds(cfg.year_month)

    tickers = _load_ticker_universe()
    if cfg.max_tickers is not None:
        tickers = tickers[: cfg.max_tickers]

    write_line(
        f"Materializing finance_by_date for {cfg.year_month}: container={cfg.container} "
        f"tickers={len(tickers)} output_path={cfg.output_path}"
    )

    frames = []
    for ticker in tickers:
        src_path = DataPaths.get_gold_finance_path(ticker)
        df = load_delta(
            cfg.container,
            src_path,
            filters=[("date", ">=", start.to_pydatetime()), ("date", "<", end.to_pydatetime())],
        )
        if df is None or df.empty:
            continue
        # Inject ticker col if missing? usually DataPaths implies ticker is key, but wide table needs it
        if "symbol" not in df.columns and "Symbol" not in df.columns:
            df["symbol"] = ticker
            
        frames.append(df)

    if not frames:
        write_line(f"No finance feature rows found for {cfg.year_month}; nothing to materialize.")
        return 0

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out = out.dropna(subset=["date"])
    if out.empty:
        write_line(f"No valid date rows found for {cfg.year_month}; nothing to materialize.")
        return 0

    out["year_month"] = out["date"].dt.strftime("%Y-%m")
    out = out[out["year_month"] == cfg.year_month].copy()
    if out.empty:
        write_line(f"No rows remain after year_month filter for {cfg.year_month}; nothing to materialize.")
        return 0

    predicate = f"year_month = '{cfg.year_month}'"
    store_delta(
        out,
        container=cfg.container,
        path=cfg.output_path,
        mode="overwrite",
        partition_by=["year_month", "date"],
        merge_schema=True,
        predicate=predicate,
    )

    write_line(f"Materialized {len(out)} row(s) into {cfg.container}/{cfg.output_path} ({cfg.year_month}).")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    cfg = _build_config(argv)
    return materialize_finance_by_date(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
