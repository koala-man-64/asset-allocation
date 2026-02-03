"""
Materialize a cross-sectional (by-date) Delta table from per-ticker Silver earnings data tables.

Partitioned by year_month and Date.
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


def _normalize_mixed_columns(
    df: pd.DataFrame,
    *,
    exclude: set[str],
    numeric_threshold: float = 0.8,
) -> pd.DataFrame:
    """
    Delta/Arrow writes can fail when a pandas `object` column mixes strings + floats
    (e.g., "0.37" and 0.42). Normalize those columns to either numeric or string.
    """
    out = df.copy()
    for col in out.columns:
        if col in exclude:
            continue

        series = out[col]
        if not (pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)):
            continue

        as_str = series.astype("string")
        cleaned = (
            as_str.str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.replace("(", "-", regex=False)
            .str.replace(")", "", regex=False)
            .str.strip()
        )
        cleaned = cleaned.replace(
            {
                "": pd.NA,
                "None": pd.NA,
                "nan": pd.NA,
                "NaN": pd.NA,
                "N/A": pd.NA,
                "null": pd.NA,
            }
        )

        numeric = pd.to_numeric(cleaned, errors="coerce")
        non_null = int(as_str.notna().sum())
        if non_null and (int(numeric.notna().sum()) / non_null) >= numeric_threshold:
            out[col] = numeric.astype("float64")
        else:
            out[col] = as_str

    return out


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


def _resolve_container(container_raw: Optional[str]) -> str:
    container_raw = container_raw or os.environ.get("AZURE_FOLDER_EARNINGS")
    if container_raw is None or not str(container_raw).strip():
        raise ValueError("Missing earnings container. Set AZURE_FOLDER_EARNINGS or pass --container.")
    return str(container_raw).strip()


def _build_config(argv: Optional[List[str]]) -> MaterializeConfig:
    parser = argparse.ArgumentParser(
        description="Materialize Silver earnings data into a cross-sectional Delta table (partitioned by date)."
    )
    parser.add_argument("--container", help="Earnings container (default: AZURE_FOLDER_EARNINGS).")
    parser.add_argument("--year-month", required=True, help="Year-month partition to materialize (YYYY-MM).")
    parser.add_argument(
        "--output-path",
        default=DataPaths.get_earnings_by_date_path(),
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
    prefix_root = DataPaths.get_earnings_path("TICKER").split("/")[0]
    tickers_from_container = _try_load_tickers_from_container(container, root_prefix=prefix_root)
    if tickers_from_container is None:
        tickers = _load_ticker_universe()
        ticker_source = "symbol_universe"
    else:
        tickers = tickers_from_container
        ticker_source = "container_listing"

    if max_tickers is not None:
        tickers = tickers[: max_tickers]

    if not tickers:
        write_line(
            f"No per-ticker earnings tables found (source={ticker_source}); no year_months discovered."
        )
        return []

    year_months: set[str] = set()
    for ticker in tickers:
        src_path = DataPaths.get_earnings_path(ticker)
        df = load_delta(container, src_path)
        if df is None or df.empty:
            continue

        date_col = "Date" if "Date" in df.columns else ("date" if "date" in df.columns else None)
        if not date_col:
            continue

        dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
        if dates.empty:
            continue
        for value in dates.dt.strftime("%Y-%m").unique().tolist():
            if value:
                year_months.add(str(value))

    discovered = sorted(year_months)
    write_line(
        f"Discovered {len(discovered)} year_month(s) from silver earnings data in {container}."
    )
    return discovered


def materialize_silver_earnings_by_date(cfg: MaterializeConfig) -> int:
    start, end = _parse_year_month_bounds(cfg.year_month)

    prefix_root = DataPaths.get_earnings_path("TICKER").split("/")[0]
    tickers_from_container = _try_load_tickers_from_container(cfg.container, root_prefix=prefix_root)
    if tickers_from_container is None:
        tickers = _load_ticker_universe()
        ticker_source = "symbol_universe"
    else:
        tickers = tickers_from_container
        ticker_source = "container_listing"

    if cfg.max_tickers is not None:
        tickers = tickers[: cfg.max_tickers]

    write_line(
        f"Materializing earnings-data-by-date for {cfg.year_month}: container={cfg.container} "
        f"tickers={len(tickers)} ticker_source={ticker_source} output_path={cfg.output_path}"
    )

    if not tickers:
        write_line(f"No per-ticker earnings tables found (source={ticker_source}); nothing to materialize.")
        return 0

    frames = []
    for ticker in tickers:
        src_path = DataPaths.get_earnings_path(ticker)
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

        if "Symbol" not in df.columns and "symbol" not in df.columns:
            df["symbol"] = ticker

        df["year_month"] = df[date_col].dt.strftime("%Y-%m")
        df = df[df["year_month"] == cfg.year_month]
        if df.empty:
            continue

        frames.append(df)

    if not frames:
        write_line(f"No Silver earnings rows found for {cfg.year_month}; nothing to materialize.")
        return 0

    out = pd.concat(frames, ignore_index=True)
    date_col = "Date" if "Date" in out.columns else "date"
    predicate = f"year_month = '{cfg.year_month}'"

    # Ensure Arrow-friendly column types before writing.
    if "Symbol" in out.columns:
        out["Symbol"] = out["Symbol"].astype("string")
    if "symbol" in out.columns:
        out["symbol"] = out["symbol"].astype("string")
    out = _normalize_mixed_columns(out, exclude={date_col, "year_month", "Symbol", "symbol"})

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
    return materialize_silver_earnings_by_date(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
