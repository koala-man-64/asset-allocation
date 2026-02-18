"""
Materialize a cross-sectional (by-date) Delta table from per-ticker Silver finance data tables.

Partitioned by year_month and Date, enabling efficient cross-sectional queries.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dataclasses import dataclass
from datetime import datetime, timezone
import time
from typing import Iterable, List, Optional, Tuple

import pandas as pd

from core.core import write_line, write_warning
from core.delta_core import load_delta, store_delta
from core.pipeline import DataPaths
from tasks.common.silver_contracts import normalize_columns_to_snake_case


@dataclass(frozen=True)
class MaterializeConfig:
    container: str
    year_month: str
    output_path: str
    max_tickers: Optional[int]


@dataclass(frozen=True)
class TickerFrameResult:
    ticker: str
    frame: Optional[pd.DataFrame]
    has_data: bool

_DATE_COLUMN_CANDIDATES: Tuple[str, ...] = ("Date", "date")
_KNOWN_TYPES: Tuple[Tuple[str, str], ...] = (
    ("Income Statement", "quarterly_financials"),
    ("Balance Sheet", "quarterly_balance-sheet"),
    ("Cash Flow", "quarterly_cash-flow"),
    ("Valuation", "quarterly_valuation_measures"),
)


def _extract_ticker_from_finance_table_root(table_root: str) -> Optional[str]:
    parts = str(table_root).strip("/").split("/")
    if len(parts) < 3:
        return None
    if parts[0] != "finance-data":
        return None
    table_name = str(parts[2]).strip()
    if "_" not in table_name:
        return None
    ticker = table_name.split("_", 1)[0].strip()
    return ticker or None


def _normalize_object_columns(df: pd.DataFrame, *, exclude: set[str]) -> pd.DataFrame:
    """
    Delta/Arrow writes can fail when pandas `object` columns contain a mix of strings + floats
    (e.g., numeric strings with missing values). Finance tables intentionally keep many values
    as strings (human-formatted), so normalize object-ish columns to pandas string dtype.
    """
    out = df.copy()
    for col in out.columns:
        if col in exclude:
            continue
        series = out[col]
        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            out[col] = series.astype("string")
    return out


def _parse_year_month_bounds(year_month: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    try:
        start = pd.Timestamp(f"{year_month}-01")
    except Exception as exc:
        raise ValueError(f"Invalid year_month '{year_month}'. Expected YYYY-MM.") from exc
    end = start + pd.offsets.MonthBegin(1)
    return start, end


def _get_available_cpus() -> int:
    try:
        return max(1, len(os.sched_getaffinity(0)))  # type: ignore[attr-defined]
    except Exception:
        return max(1, os.cpu_count() or 1)


def _get_positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        parsed = int(str(raw).strip())
    except Exception:
        return default
    return parsed if parsed > 0 else default


def _get_by_date_max_workers() -> int:
    default_workers = min(4, _get_available_cpus())
    return _get_positive_int_env("SILVER_FINANCE_BY_DATE_MAX_WORKERS", default_workers)


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

    from core import core as mdc

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


def _resolve_container(container_raw: Optional[str]) -> str:
    container_raw = container_raw or os.environ.get("AZURE_CONTAINER_SILVER")
    if container_raw is None or not str(container_raw).strip():
        raise ValueError("Missing silver container. Set AZURE_CONTAINER_SILVER or pass --container.")
    return str(container_raw).strip()


def _load_first_available_date_projection(
    *, container: str, src_path: str
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    for date_col in _DATE_COLUMN_CANDIDATES:
        df = load_delta(container, src_path, columns=[date_col])
        if df is None:
            continue
        if date_col in df.columns:
            return date_col, df
    return None, None


def _load_month_filtered_frame(
    *,
    container: str,
    src_path: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    start_dt = start.to_pydatetime()
    end_dt = end.to_pydatetime()
    for date_col in _DATE_COLUMN_CANDIDATES:
        filters = [(date_col, ">=", start_dt), (date_col, "<", end_dt)]
        df = load_delta(container, src_path, filters=filters)
        if df is None or df.empty:
            continue
        if date_col in df.columns:
            return date_col, df

    # Fallback for connectors/files where predicate pushdown on date columns is unavailable.
    df = load_delta(container, src_path)
    if df is None or df.empty:
        return None, None
    date_col = "Date" if "Date" in df.columns else ("date" if "date" in df.columns else None)
    if date_col is None:
        return None, None
    return date_col, df


def _build_ticker_frame(
    *,
    ticker: str,
    container: str,
    available_table_roots: Optional[set[str]],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> TickerFrameResult:
    ticker_frames: list[pd.DataFrame] = []
    ticker_has_data = False

    for folder_name, suffix in _KNOWN_TYPES:
        src_path = DataPaths.get_finance_path(folder_name, ticker, suffix)
        if available_table_roots is not None and src_path not in available_table_roots:
            continue

        date_col, df = _load_month_filtered_frame(
            container=container,
            src_path=src_path,
            start=start,
            end=end,
        )
        if date_col is None or df is None or df.empty:
            continue

        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
        df = df.dropna(subset=[date_col])
        if df.empty:
            continue

        df = df[(df[date_col] >= start) & (df[date_col] < end)]
        if df.empty:
            continue
        ticker_has_data = True

        df = df.set_index(date_col)
        if "Symbol" in df.columns:
            df = df.drop(columns=["Symbol"])
        if "symbol" in df.columns:
            df = df.drop(columns=["symbol"])
        ticker_frames.append(df)

    if not ticker_frames:
        return TickerFrameResult(ticker=ticker, frame=None, has_data=ticker_has_data)

    merged = pd.concat(ticker_frames, axis=1).reset_index()
    merged["Symbol"] = ticker
    if "Date" not in merged.columns and merged.index.name == "Date":
        merged = merged.reset_index()
    merged = merged.loc[:, ~merged.columns.duplicated()]
    return TickerFrameResult(ticker=ticker, frame=merged, has_data=ticker_has_data)


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

    container = _resolve_container(args.container)

    max_tickers = int(args.max_tickers) if args.max_tickers is not None else None
    if max_tickers is not None and max_tickers <= 0:
        max_tickers = None

    return MaterializeConfig(
        container=container,
        year_month=str(args.year_month).strip(),
        output_path=str(args.output_path).strip().lstrip("/"),
        max_tickers=max_tickers,
    )


def discover_year_months_from_data(
    *, container: Optional[str] = None, max_tickers: Optional[int] = None
) -> List[str]:
    container = _resolve_container(container)
    available_table_roots = _try_load_finance_table_roots_from_container(container)

    if available_table_roots is None:
        tickers = _load_ticker_universe()
        if max_tickers is not None:
            tickers = tickers[: max_tickers]

        table_roots = [
            DataPaths.get_finance_path(folder_name, ticker, suffix)
            for ticker in tickers
            for folder_name, suffix in _KNOWN_TYPES
        ]
        source = "symbol_universe"
    else:
        table_roots = sorted(available_table_roots)
        source = "container_listing"
        if max_tickers is not None:
            keep_tickers = sorted(
                {ticker for root in table_roots if (ticker := _extract_ticker_from_finance_table_root(root)) is not None}
            )[:max_tickers]
            keep_ticker_set = set(keep_tickers)
            table_roots = [
                root for root in table_roots if _extract_ticker_from_finance_table_root(root) in keep_ticker_set
            ]

    if not table_roots:
        write_line(f"No Silver finance tables found (source={source}); no year_months discovered.")
        return []

    year_months: set[str] = set()
    for src_path in table_roots:
        date_col, df = _load_first_available_date_projection(container=container, src_path=src_path)
        if date_col is None or df is None or df.empty:
            continue

        dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
        if dates.empty:
            continue
        for value in dates.dt.strftime("%Y-%m").unique().tolist():
            if value:
                year_months.add(str(value))

    discovered = sorted(year_months)
    write_line(f"Discovered {len(discovered)} year_month(s) from silver finance data in {container}.")
    return discovered


def materialize_silver_finance_by_date(cfg: MaterializeConfig) -> int:
    phase_started = time.perf_counter()
    start, end = _parse_year_month_bounds(cfg.year_month)

    available_table_roots = _try_load_finance_table_roots_from_container(cfg.container)
    if available_table_roots is None:
        tickers = _load_ticker_universe()
        ticker_source = "symbol_universe"
    else:
        tickers = sorted(
            {
                ticker
                for root in available_table_roots
                if (ticker := _extract_ticker_from_finance_table_root(root)) is not None
            }
        )
        ticker_source = "container_listing"

    if cfg.max_tickers is not None:
        tickers = tickers[: cfg.max_tickers]

    source_tickers: set[str] = {
        ticker
        for root in (available_table_roots or [])
        if (ticker := _extract_ticker_from_finance_table_root(root)) is not None
    }

    max_workers = _get_by_date_max_workers()
    write_line(
        f"Materializing finance-data-by-date for {cfg.year_month}: container={cfg.container} "
        f"tickers={len(tickers)} ticker_source={ticker_source} output_path={cfg.output_path} "
        f"workers={max_workers}"
    )

    if not tickers:
        write_line(f"No per-ticker finance tables found (source={ticker_source}); nothing to materialize.")
        return 0

    build_started = time.perf_counter()
    ticker_results: list[TickerFrameResult] = []
    if max_workers <= 1:
        for ticker in tickers:
            ticker_results.append(
                _build_ticker_frame(
                    ticker=ticker,
                    container=cfg.container,
                    available_table_roots=available_table_roots,
                    start=start,
                    end=end,
                )
            )
    else:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="finance-by-date") as executor:
            futures = {
                executor.submit(
                    _build_ticker_frame,
                    ticker=ticker,
                    container=cfg.container,
                    available_table_roots=available_table_roots,
                    start=start,
                    end=end,
                ): ticker
                for ticker in tickers
            }
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    ticker_results.append(future.result())
                except Exception as exc:
                    write_warning(f"Finance by-date worker failed for ticker={ticker}: {exc}")
                    ticker_results.append(TickerFrameResult(ticker=ticker, frame=None, has_data=False))
    build_elapsed = time.perf_counter() - build_started

    frames = [result.frame for result in ticker_results if result.frame is not None]
    source_tickers_with_data = {
        result.ticker
        for result in ticker_results
        if result.has_data
    }

    if not frames:
        write_line(f"No Silver finance rows found for {cfg.year_month}; nothing to materialize.")
        return 0

    out = pd.concat(frames, ignore_index=True)
    date_col = "Date" if "Date" in out.columns else "date"
    out["year_month"] = out[date_col].dt.strftime("%Y-%m")
    out = out[out["year_month"] == cfg.year_month]
    if out.empty:
        write_line(f"No rows remain after year_month filter for {cfg.year_month}; nothing to materialize.")
        return 0

    out = normalize_columns_to_snake_case(out)
    date_col = "date" if "date" in out.columns else ("obs_date" if "obs_date" in out.columns else None)
    if date_col is None:
        write_line(f"No date column found for {cfg.year_month}; nothing to materialize.")
        return 0

    predicate = f"year_month = '{cfg.year_month}'"

    if "Symbol" in out.columns:
        out["Symbol"] = out["Symbol"].astype("string")
    if "symbol" in out.columns:
        out["symbol"] = out["symbol"].astype("string")
    out = _normalize_object_columns(out, exclude={date_col, "year_month", "Symbol", "symbol"})

    output_symbols = (
        {
            str(value).strip()
            for value in out.get("Symbol", pd.Series(dtype="object")).dropna().tolist()
        }
        if "Symbol" in out
        else set()
    )
    output_symbols = {symbol for symbol in output_symbols if symbol}
    expected_symbols = source_tickers_with_data or source_tickers

    expected_count = len(expected_symbols)
    output_count = len(output_symbols)
    if expected_count:
        ratio = output_count / expected_count
        if output_count < expected_count:
            write_warning(
                f"Finance by-date reconciliation mismatch for {cfg.year_month}: "
                f"expected={expected_count} source symbols, output={output_count}, ratio={ratio:.3f}."
            )
        else:
            write_line(
                f"Finance by-date reconciliation for {cfg.year_month}: "
                f"symbols={output_count}, ratio={ratio:.3f}"
            )

    store_started = time.perf_counter()
    store_delta(
        out,
        container=cfg.container,
        path=cfg.output_path,
        mode="overwrite",
        partition_by=["year_month", date_col],
        merge_schema=True,
        predicate=predicate,
    )
    store_elapsed = time.perf_counter() - store_started
    total_elapsed = time.perf_counter() - phase_started
    finished_at = datetime.now(timezone.utc).isoformat()

    write_line(
        f"Materialized {len(out)} row(s) into {cfg.container}/{cfg.output_path} ({cfg.year_month}) "
        f"buildSec={build_elapsed:.2f} writeSec={store_elapsed:.2f} totalSec={total_elapsed:.2f} "
        f"finishedAt={finished_at}"
    )
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    cfg = _build_config(argv)
    return materialize_silver_finance_by_date(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
