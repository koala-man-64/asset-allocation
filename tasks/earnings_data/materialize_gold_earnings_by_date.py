"""
Materialize a cross-sectional (by-date) Delta table from per-ticker Gold earnings feature tables.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import List, Optional

from core.core import write_line
from core.pipeline import DataPaths
from tasks.common.materialization import materialize_by_date


@dataclass(frozen=True)
class MaterializeConfig:
    container: str
    year_month: str
    output_path: str


def _build_config(argv: Optional[List[str]]) -> MaterializeConfig:
    parser.add_argument("--max-tickers", type=int, default=None, help="Optional limit for debugging.")
    args = parser.parse_args(argv)

    container_raw = args.container or os.environ.get("AZURE_CONTAINER_GOLD")
    if container_raw is None or not str(container_raw).strip():
        # Fallback to EARNINGS if GOLD is not set, though GOLD is expected
        container_raw = os.environ.get("AZURE_CONTAINER_EARNINGS")

    if container_raw is None or not str(container_raw).strip():
        raise ValueError("Missing container. Set AZURE_CONTAINER_GOLD or pass --container.")
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


def materialize_earnings_by_date(cfg: MaterializeConfig) -> int:
    start, end = _parse_year_month_bounds(cfg.year_month)

    tickers_from_container = _try_load_tickers_from_container(cfg.container, root_prefix="earnings")
    if tickers_from_container is None:
        tickers = _load_ticker_universe()
        ticker_source = "symbol_universe"
    else:
        tickers = tickers_from_container
        ticker_source = "container_listing"

    if cfg.max_tickers is not None:
        tickers = tickers[: cfg.max_tickers]

    write_line(
        f"Materializing earnings_by_date for {cfg.year_month}: container={cfg.container} "
        f"tickers={len(tickers)} ticker_source={ticker_source} output_path={cfg.output_path}"
    )

    if not tickers:
        write_line(f"No per-ticker earnings feature tables found (source={ticker_source}); nothing to materialize.")
        return 0

    frames = []
    for ticker in tickers:
        src_path = DataPaths.get_gold_earnings_path(ticker)
        df = load_delta(
            cfg.container,
            src_path,
            filters=[("date", ">=", start.to_pydatetime()), ("date", "<", end.to_pydatetime())],
        )
        if df is None or df.empty:
            continue
            
        if "symbol" not in df.columns and "Symbol" not in df.columns:
            df["symbol"] = ticker

        frames.append(df)

    if not frames:
        write_line(f"No earnings feature rows found for {cfg.year_month}; nothing to materialize.")
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
    return materialize_earnings_by_date(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
