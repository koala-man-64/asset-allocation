"""
Materialize a cross-sectional (by-date) Delta table from per-ticker Gold earnings feature tables.

Why:
- Per-ticker tables are convenient for symbol-level analytics.
- Ranking/analytics need cross-sectional slices (all symbols for a given date).

This script builds a single Delta table partitioned by year_month and date.
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

    client = mdc.get_storage_client(container)
    if client is None:
        return None

    prefix = root_prefix.strip("/") + "/"
    try:
        blobs = client.container_client.list_blobs(name_starts_with=prefix)
        return _extract_tickers_from_delta_tables((b.name for b in blobs), root_prefix=root_prefix)
    except Exception as exc:
        write_warning(
            f"Unable to list per-ticker tables under {prefix} in container={container}: {exc}. "
            "Falling back to symbol universe."
        )
        return None


def _build_config(argv: Optional[List[str]]) -> MaterializeConfig:
    parser = argparse.ArgumentParser(
        description="Materialize earnings features into a cross-sectional Delta table (partitioned by date)."
    )
    parser.add_argument("--container", help="Earnings features container (default: AZURE_CONTAINER_GOLD).")
    parser.add_argument("--year-month", required=True, help="Year-month partition to materialize (YYYY-MM).")
    parser.add_argument(
        "--output-path",
        default=DataPaths.get_gold_earnings_by_date_path(),
        help="Output Delta table path within the container.",
    )
    parser.add_argument("--max-tickers", type=int, default=None, help="Optional limit for debugging.")
    args = parser.parse_args(argv)

    container_raw = args.container or os.environ.get("AZURE_CONTAINER_GOLD")
    if container_raw is None or not str(container_raw).strip():
        # Fallback to EARNINGS if GOLD is not set, though GOLD is expected
        container_raw = os.environ.get("AZURE_CONTAINER_EARNINGS")

    if container_raw is None or not str(container_raw).strip():
        raise ValueError("Missing container. Set AZURE_CONTAINER_GOLD or AZURE_CONTAINER_EARNINGS or pass --container.")
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

        date_col = "date" if "date" in df.columns else ("Date" if "Date" in df.columns else None)
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

        if "symbol" not in df.columns and "Symbol" not in df.columns:
            df["symbol"] = ticker

        df["year_month"] = df[date_col].dt.strftime("%Y-%m")
        df = df[df["year_month"] == cfg.year_month]
        if df.empty:
            continue

        frames.append(df)

    if not frames:
        write_line(f"No earnings feature rows found for {cfg.year_month}; nothing to materialize.")
        return 0

    out = pd.concat(frames, ignore_index=True)
    date_col = "date" if "date" in out.columns else "Date"
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
    return materialize_earnings_by_date(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
