"""
Materialize a cross-sectional (by-date) Delta table from per-ticker Silver earnings data tables.

Partitioned by year_month and Date, enabling efficient cross-sectional queries.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import pandas as pd

from core.core import write_line, write_warning
from core.delta_core import load_delta, store_delta
from core.pipeline import DataPaths


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
    from core import core as mdc

    df_symbols = mdc.get_symbols()
    df_symbols = df_symbols.dropna(subset=["Symbol"]).copy()

    tickers: List[str] = []
    for symbol in df_symbols["Symbol"].astype(str).tolist():
        if "." in symbol:
            continue
        tickers.append(symbol.replace(".", "-"))

    return list(dict.fromkeys(tickers))


def _extract_tickers_from_delta_tables(blob_names: Iterable[str], root_prefix: str) -> List[str]:
    """
    Extract tickers that have a valid Delta log present under `<root_prefix>/<ticker>/_delta_log/`.

    Avoids attempting to read symbols that have no per-ticker Delta table, which otherwise triggers noisy
    delta-rs warnings.
    """

    root_prefix = root_prefix.strip("/")
    tickers: set[str] = set()
    for blob_name in blob_names:
        parts = str(blob_name).strip("/").split("/")
        if len(parts) < 4:
            continue
        if parts[0] != root_prefix:
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


def _try_load_tickers_from_container(container: str, root_prefix: str) -> Optional[List[str]]:
    """
    Attempt to list tickers from the target container (preferred).

    Returns:
      - List[str] (possibly empty) when listing succeeds.
      - None when listing is unavailable (e.g., no list permissions / no client).
    """

    from core import core as mdc


def _build_config(argv: Optional[List[str]]) -> MaterializeConfig:
    parser = argparse.ArgumentParser(description="Materialize Silver Earnings by Date")
    parser.add_argument("--container", required=True, help="Azure container name")
    parser.add_argument("--year-month", required=True, help="YYYY-MM to materialize")
    parser.add_argument("--output-path", required=True, help="Delta table path for output")
    
    args = parser.parse_args(argv)
    return MaterializeConfig(
        container=args.container,
        year_month=args.year_month,
        output_path=args.output_path,
    )


def materialize_silver_earnings_by_date(cfg: MaterializeConfig) -> int:
    return materialize_by_date(
        container=cfg.container,
        output_path=cfg.output_path,
        year_month=cfg.year_month,
        get_source_path_func=DataPaths.get_earnings_path,
        date_col_candidates=["Date", "date"], # Source has Date usually
        ticker_col="Symbol"
    )


def main(argv: Optional[List[str]] = None) -> int:
    cfg = _build_config(argv)
    return materialize_silver_earnings_by_date(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
