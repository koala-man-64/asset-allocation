"""
Materialize a cross-sectional (by-date) Delta table from per-ticker Silver price target data tables.

Partitioned by year_month and Date, enabling efficient cross-sectional queries.
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
        description="Materialize Silver price target data into a cross-sectional Delta table (partitioned by date)."
    )
    parser.add_argument("--container", help="Targets container (default: AZURE_CONTAINER_TARGETS).")
    parser.add_argument("--year-month", required=True, help="Year-month partition to materialize (YYYY-MM).")
    parser.add_argument(
        "--output-path",
        default=DataPaths.get_price_targets_by_date_path(),
        help="Output Delta table path within the container.",
    )
    parser.add_argument("--max-tickers", type=int, default=None, help="Optional limit for debugging.")
    args = parser.parse_args(argv)

    container = (args.container or os.environ.get("AZURE_CONTAINER_TARGETS", "")).strip()
    if not container:
        raise ValueError("Missing targets container. Set AZURE_CONTAINER_TARGETS or pass --container.")

    max_tickers = int(args.max_tickers) if args.max_tickers is not None else None
    if max_tickers is not None and max_tickers <= 0:
        max_tickers = None

    return MaterializeConfig(
        container=container,
        year_month=str(args.year_month).strip(),
        output_path=str(args.output_path).strip().lstrip("/"),
        max_tickers=max_tickers,
    )


def materialize_silver_targets_by_date(cfg: MaterializeConfig) -> int:
    start, end = _parse_year_month_bounds(cfg.year_month)

    tickers = _load_ticker_universe()
    if cfg.max_tickers is not None:
        tickers = tickers[: cfg.max_tickers]

    write_line(
        f"Materializing price-target-data-by-date for {cfg.year_month}: container={cfg.container} "
        f"tickers={len(tickers)} output_path={cfg.output_path}"
    )

    frames = []
    for ticker in tickers:
        src_path = DataPaths.get_price_target_path(ticker)
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
            
        # Inject symbol if missing
        if "Symbol" not in df.columns and "symbol" not in df.columns:
            df["symbol"] = ticker

        df["year_month"] = df[date_col].dt.strftime("%Y-%m")
        df = df[df["year_month"] == cfg.year_month]
        if df.empty:
            continue

        frames.append(df)

    if not frames:
        write_line(f"No Silver price target rows found for {cfg.year_month}; nothing to materialize.")
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
    return materialize_silver_targets_by_date(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
