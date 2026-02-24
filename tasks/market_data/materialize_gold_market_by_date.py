from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional, Sequence

import pandas as pd

from core import core as mdc
from core import delta_core
from core.pipeline import DataPaths
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from tasks.common.silver_contracts import normalize_columns_to_snake_case

_YEAR_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


@dataclass(frozen=True)
class MaterializeConfig:
    container: str
    source_prefix: str
    target_path: str
    include_columns: Optional[list[str]]
    year_month: Optional[str]
    max_tables: Optional[int]


@dataclass(frozen=True)
class MaterializeResult:
    tables_scanned: int
    tables_loaded: int
    rows_written: int
    target_path: str
    columns_written: list[str]


def _parse_csv(raw: Optional[str]) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _normalize_columns(raw_columns: Optional[Iterable[str]]) -> Optional[list[str]]:
    if raw_columns is None:
        return None

    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_columns:
        key = str(item or "").strip()
        if not key:
            continue
        snake = re.sub(r"[^0-9a-zA-Z]+", "_", key).strip("_").lower()
        if not snake or snake in seen:
            continue
        seen.add(snake)
        normalized.append(snake)

    return normalized or None


def _parse_year_month(raw: Optional[str]) -> Optional[str]:
    value = str(raw or "").strip()
    if not value:
        return None
    if not _YEAR_MONTH_RE.fullmatch(value):
        raise ValueError("year_month must be in YYYY-MM format.")
    return value


def _build_config(argv: Optional[Sequence[str]]) -> MaterializeConfig:
    parser = argparse.ArgumentParser(description="Materialize a by-date Gold market view from per-symbol Gold tables.")
    parser.add_argument("--container", default=None, help="Gold container name (defaults to AZURE_CONTAINER_GOLD).")
    parser.add_argument("--source-prefix", default=None, help="Per-symbol source prefix (default: market).")
    parser.add_argument("--target-path", default=None, help="By-date table path (default: market_by_date).")
    parser.add_argument(
        "--columns",
        default=None,
        help="Optional comma-separated list of columns to include (symbol/date are always included).",
    )
    parser.add_argument(
        "--year-month",
        default=None,
        help="Optional YYYY-MM filter for partial materialization (overwrites only that partition).",
    )
    parser.add_argument("--max-tables", type=int, default=None, help="Optional limit for source tables (debug).")
    args = parser.parse_args(argv)

    container_raw = args.container or os.environ.get("AZURE_CONTAINER_GOLD")
    if container_raw is None or not str(container_raw).strip():
        raise ValueError("Missing Gold container. Set AZURE_CONTAINER_GOLD or pass --container.")
    container = str(container_raw).strip()

    source_prefix = str(args.source_prefix or os.environ.get("GOLD_MARKET_SOURCE_PREFIX") or "market").strip().strip("/")
    if not source_prefix:
        source_prefix = "market"

    target_path = str(args.target_path or os.environ.get("GOLD_MARKET_BY_DATE_PATH") or DataPaths.get_gold_market_by_date_path()).strip().strip("/")
    if not target_path:
        target_path = DataPaths.get_gold_market_by_date_path()

    columns_raw = args.columns
    if columns_raw is None:
        columns_raw = os.environ.get("GOLD_MARKET_BY_DATE_COLUMNS")
    include_columns = _normalize_columns(_parse_csv(columns_raw))

    year_month_raw = args.year_month if args.year_month is not None else os.environ.get("MATERIALIZE_YEAR_MONTH")
    year_month = _parse_year_month(year_month_raw)

    max_tables = args.max_tables
    if max_tables is None:
        max_tables_raw = str(os.environ.get("GOLD_MARKET_BY_DATE_MAX_TABLES") or "").strip()
        if max_tables_raw:
            max_tables = int(max_tables_raw)
    if max_tables is not None and max_tables <= 0:
        raise ValueError("max_tables must be greater than zero when provided.")

    return MaterializeConfig(
        container=container,
        source_prefix=source_prefix,
        target_path=target_path,
        include_columns=include_columns,
        year_month=year_month,
        max_tables=max_tables,
    )


def _discover_delta_table_paths(
    *,
    container: str,
    source_prefix: str,
    max_tables: Optional[int] = None,
) -> list[str]:
    client = mdc.get_storage_client(container)
    if client is None:
        raise RuntimeError(f"Storage client unavailable for container={container!r}.")

    marker = "/_delta_log/"
    search_prefix = f"{source_prefix.strip('/')}/"
    roots: set[str] = set()
    for name in client.list_files(name_starts_with=search_prefix):
        text = str(name or "")
        if marker not in text:
            continue
        root = text.split(marker, 1)[0].strip("/")
        if not root.startswith(search_prefix.rstrip("/")):
            continue
        roots.add(root)

    discovered = sorted(roots)
    if max_tables is not None:
        return discovered[:max_tables]
    return discovered


def _apply_projection(df: pd.DataFrame, *, include_columns: Optional[list[str]], source_path: str) -> pd.DataFrame:
    required = ["date", "symbol"]

    if include_columns is None:
        selected = [col for col in df.columns if col != "year_month"]
    else:
        selected = required + [col for col in include_columns if col not in {"date", "symbol", "year_month"}]

    missing = [col for col in required if col not in df.columns]
    if missing:
        mdc.write_warning(f"Skipping {source_path}: missing required columns {missing}.")
        return pd.DataFrame()

    if include_columns:
        missing_optional = [col for col in selected if col not in df.columns]
        if missing_optional:
            mdc.write_warning(
                f"Configured by-date columns missing in {source_path}; skipping columns={missing_optional}."
            )

    existing = [col for col in selected if col in df.columns]
    if not existing:
        return pd.DataFrame()

    return df[existing].copy()


def _month_bounds(year_month: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    month_start = pd.Timestamp(f"{year_month}-01")
    month_end = month_start + pd.offsets.MonthBegin(1)
    return month_start, month_end


def _prepare_source_frame(
    df_raw: pd.DataFrame,
    *,
    include_columns: Optional[list[str]],
    year_month: Optional[str],
    backfill_start: Optional[pd.Timestamp],
    source_path: str,
) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    work = normalize_columns_to_snake_case(df_raw)
    work = _apply_projection(work, include_columns=include_columns, source_path=source_path)
    if work.empty:
        return work

    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date"]).copy()
    if work.empty:
        return work

    if hasattr(work["date"].dt, "tz") and work["date"].dt.tz is not None:
        work["date"] = work["date"].dt.tz_convert(None)
    work["date"] = work["date"].dt.normalize()

    work["symbol"] = work["symbol"].astype(str).str.strip().str.upper()
    work = work[work["symbol"] != ""].copy()
    if work.empty:
        return work

    work, _ = apply_backfill_start_cutoff(
        work,
        date_col="date",
        backfill_start=backfill_start,
        context=f"materialize gold market by-date {source_path}",
    )
    if work.empty:
        return work

    if year_month:
        month_start, month_end = _month_bounds(year_month)
        work = work[(work["date"] >= month_start) & (work["date"] < month_end)].copy()
        if work.empty:
            return work

    work["year_month"] = work["date"].dt.strftime("%Y-%m")
    work = work.drop_duplicates(subset=["date", "symbol"], keep="last").reset_index(drop=True)
    return work


def materialize_market_by_date(
    config: MaterializeConfig,
    *,
    source_paths: Optional[Sequence[str]] = None,
) -> MaterializeResult:
    resolved_paths = list(source_paths) if source_paths is not None else _discover_delta_table_paths(
        container=config.container,
        source_prefix=config.source_prefix,
        max_tables=config.max_tables,
    )

    if not resolved_paths:
        raise RuntimeError(f"No source Gold market Delta tables found under '{config.source_prefix}/'.")

    backfill_start, _ = get_backfill_range()
    if backfill_start is not None:
        mdc.write_line(
            "Applying BACKFILL_START_DATE cutoff to gold market by-date view: "
            f"{backfill_start.date().isoformat()}"
        )

    frames: list[pd.DataFrame] = []
    loaded = 0
    for path in resolved_paths:
        df = delta_core.load_delta(config.container, path)
        if df is None or df.empty:
            continue

        loaded += 1
        prepared = _prepare_source_frame(
            df,
            include_columns=config.include_columns,
            year_month=config.year_month,
            backfill_start=backfill_start,
            source_path=path,
        )
        if prepared.empty:
            continue

        frames.append(prepared)

    if not frames:
        raise RuntimeError("No rows available for by-date materialization after applying filters/projection.")

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values(by=["date", "symbol"]).drop_duplicates(subset=["date", "symbol"], keep="last")
    merged = merged.reset_index(drop=True)

    partition_by = ["year_month"]
    if config.year_month:
        predicate = f"year_month = '{config.year_month}'"
        schema_mode = "merge"
    else:
        predicate = None
        schema_mode = "overwrite"

    delta_core.store_delta(
        merged,
        config.container,
        config.target_path,
        mode="overwrite",
        partition_by=partition_by,
        schema_mode=schema_mode,
        predicate=predicate,
    )

    return MaterializeResult(
        tables_scanned=len(resolved_paths),
        tables_loaded=loaded,
        rows_written=int(len(merged)),
        target_path=config.target_path,
        columns_written=[str(col) for col in merged.columns],
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    mdc.log_environment_diagnostics()

    try:
        config = _build_config(argv)
    except Exception as exc:
        mdc.write_error(f"Failed to build by-date materialization config: {exc}")
        return 1

    started_at = datetime.now(timezone.utc)
    mdc.write_line(
        "Materializing Gold market by-date view "
        f"from '{config.source_prefix}/' to '{config.target_path}' "
        f"(year_month={config.year_month or 'ALL'}, columns={config.include_columns or 'ALL'})"
    )

    try:
        result = materialize_market_by_date(config)
    except Exception as exc:
        mdc.write_error(f"Gold market by-date materialization failed: {exc}")
        return 1

    duration = (datetime.now(timezone.utc) - started_at).total_seconds()
    mdc.write_line(
        "Gold market by-date materialization complete: "
        f"rows={result.rows_written} tables_scanned={result.tables_scanned} "
        f"tables_loaded={result.tables_loaded} target={result.target_path} duration_s={duration:.2f}"
    )
    return 0


if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "gold-market-by-date-job"
    ensure_api_awake_from_env(required=True)
    exit_code = main()
    if exit_code == 0:
        write_system_health_marker(layer="gold", domain="market_by_date", job_name=job_name)
    raise SystemExit(exit_code)
