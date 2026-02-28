from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import os
import time
from typing import Any, Optional, Tuple

import pandas as pd
import re
import json

from core import core as mdc
from core import delta_core
from tasks.finance_data import config as cfg
from core.pipeline import DataPaths
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from tasks.common.run_manifests import (
    load_latest_bronze_finance_manifest,
    manifest_blobs,
    silver_finance_ack_exists,
    silver_manifest_consumption_enabled,
    write_silver_finance_ack,
)
from tasks.common.watermarks import (
    check_blob_unchanged,
    load_last_success,
    load_watermarks,
    save_last_success,
    save_watermarks,
    should_process_blob_since_last_success,
)
from tasks.common.silver_contracts import (
    ContractViolation,
    align_to_existing_schema,
    assert_no_unexpected_mixed_empty,
    log_contract_violation,
    normalize_date_column,
    require_non_empty_frame,
    normalize_columns_to_snake_case,
)
from tasks.common.market_reconciliation import (
    collect_bronze_finance_symbols_from_blob_infos,
    collect_delta_silver_finance_symbols,
    enforce_backfill_cutoff_on_tables,
    purge_orphan_tables,
)

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)


@dataclass(frozen=True)
class BlobProcessResult:
    blob_name: str
    silver_path: Optional[str]
    ticker: Optional[str]
    status: str  # ok|skipped|failed
    rows_written: Optional[int] = None
    error: Optional[str] = None
    watermark_signature: Optional[dict[str, Optional[str]]] = None


_KEY_NORMALIZER = re.compile(r"[^a-z0-9]+")
_KNOWN_FINANCE_SUFFIXES: Tuple[str, ...] = (
    "quarterly_balance-sheet",
    "quarterly_valuation_measures",
    "quarterly_cash-flow",
    "quarterly_financials",
)
_FINANCE_RECONCILIATION_TABLES: Tuple[Tuple[str, str], ...] = (
    ("balance_sheet", "quarterly_balance-sheet"),
    ("income_statement", "quarterly_financials"),
    ("cash_flow", "quarterly_cash-flow"),
    ("valuation", "quarterly_valuation_measures"),
)
_DEFAULT_FINANCE_SHARED_LOCK = "finance-pipeline-shared"
_DEFAULT_SILVER_SHARED_LOCK_WAIT_SECONDS = 3600.0
_DEFAULT_CATCHUP_MAX_PASSES = 3


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
        value = int(str(raw).strip())
    except Exception:
        return default
    return value if value > 0 else default


def _get_ingest_max_workers() -> int:
    default_workers = min(8, _get_available_cpus() * 2)
    return _get_positive_int_env("SILVER_FINANCE_INGEST_MAX_WORKERS", default_workers)


def _get_catchup_max_passes() -> int:
    return _get_positive_int_env("SILVER_FINANCE_CATCHUP_MAX_PASSES", _DEFAULT_CATCHUP_MAX_PASSES)


def _parse_wait_timeout_seconds(raw: str | None, *, default: float) -> float | None:
    if raw is None:
        return default
    value = str(raw).strip()
    if not value:
        return default
    if value.lower() in {"none", "inf", "infinite", "forever"}:
        return None
    try:
        parsed = float(value)
    except Exception:
        return default
    return max(0.0, parsed)


def _parse_supported_blob_name(blob_name: str) -> Optional[tuple[str, str, str]]:
    parts = str(blob_name).split("/")
    if len(parts) < 3:
        return None

    folder_name = str(parts[1]).strip()
    filename = str(parts[2]).strip()
    if not filename.endswith(".json"):
        return None

    suffix = next((s for s in _KNOWN_FINANCE_SUFFIXES if filename.endswith(f"{s}.json")), None)
    if not suffix:
        return None

    ticker = filename.replace(f"_{suffix}.json", "").strip()
    if not ticker:
        return None

    return folder_name, ticker, suffix


def _get_blob_dedupe_key(blob_name: str) -> Optional[str]:
    parsed = _parse_supported_blob_name(blob_name)
    if parsed is None:
        return None
    folder_name, ticker, suffix = parsed
    return DataPaths.get_finance_path(folder_name, ticker, suffix)


def _select_preferred_blob_candidates(blobs: list[dict]) -> list[dict]:
    chosen_by_key: dict[str, dict] = {}

    for blob in blobs:
        blob_name = str(blob.get("name", "")).strip()
        dedupe_key = _get_blob_dedupe_key(blob_name)
        if not dedupe_key:
            continue

        existing = chosen_by_key.get(dedupe_key)
        if existing is None:
            chosen_by_key[dedupe_key] = blob
            continue

    selected = list(chosen_by_key.values())
    selected.sort(key=lambda item: str(item.get("name", "")))
    return selected


def _list_bronze_finance_candidates() -> tuple[list[dict], int]:
    listed_blobs = bronze_client.list_blob_infos(name_starts_with="finance-data/")
    blobs = _select_preferred_blob_candidates(listed_blobs)
    supported_count = sum(
        1 for blob in listed_blobs if _get_blob_dedupe_key(str(blob.get("name", "")).strip()) is not None
    )
    unsupported = max(0, len(listed_blobs) - supported_count)
    deduped = max(0, supported_count - len(blobs))
    if unsupported > 0:
        mdc.write_line(
            f"Silver finance strict input filter removed {unsupported} unsupported Bronze blob candidate(s); "
            f"processing {len(blobs)} supported inputs."
        )
    if deduped > 0:
        mdc.write_line(
            f"Silver finance candidate dedupe removed {deduped} duplicate blob candidate(s); "
            f"processing {len(blobs)} preferred inputs."
        )
    return blobs, deduped


def _build_checkpoint_candidates(
    *,
    blobs: list[dict],
    watermarks: dict,
    last_success: Optional[datetime],
) -> tuple[list[dict], int]:
    checkpoint_skipped = 0
    candidates: list[dict] = []
    for blob in blobs:
        blob_name = str(blob.get("name", "")).strip()
        if _get_blob_dedupe_key(blob_name) is None:
            continue
        prior = watermarks.get(blob_name)
        should_process = should_process_blob_since_last_success(
            blob,
            prior_signature=prior,
            last_success_at=last_success,
        )
        if should_process:
            candidates.append(blob)
        else:
            checkpoint_skipped += 1
    return candidates, checkpoint_skipped


def _run_finance_reconciliation(*, bronze_blob_list: list[dict]) -> tuple[int, int]:
    if silver_client is None:
        raise RuntimeError("Silver finance reconciliation requires silver storage client.")

    bronze_symbols = collect_bronze_finance_symbols_from_blob_infos(bronze_blob_list)
    silver_symbols = collect_delta_silver_finance_symbols(client=silver_client)

    def _delete_symbol_tables(symbol: str) -> int:
        deleted = 0
        for folder, suffix in _FINANCE_RECONCILIATION_TABLES:
            table_path = DataPaths.get_finance_path(folder, symbol, suffix)
            deleted += int(silver_client.delete_prefix(table_path) or 0)
        return deleted

    orphan_symbols, deleted_blobs = purge_orphan_tables(
        upstream_symbols=bronze_symbols,
        downstream_symbols=silver_symbols,
        downstream_path_builder=lambda symbol: symbol,
        delete_prefix=_delete_symbol_tables,
    )
    if orphan_symbols:
        mdc.write_line(
            "Silver finance reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs}"
        )
    else:
        mdc.write_line("Silver finance reconciliation: no orphan symbols detected.")

    backfill_start, _ = get_backfill_range()
    cutoff_symbols = silver_symbols.difference(set(orphan_symbols))
    cutoff_stats = enforce_backfill_cutoff_on_tables(
        symbols=cutoff_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_finance_path(folder, symbol, suffix)
            for folder, suffix in _FINANCE_RECONCILIATION_TABLES
        ],
        load_table=lambda path: delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, path),
        store_table=lambda df, path: delta_core.store_delta(df, cfg.AZURE_CONTAINER_SILVER, path, mode="overwrite"),
        delete_prefix=silver_client.delete_prefix,
        date_column_candidates=("date", "Date"),
        backfill_start=backfill_start,
        context="silver finance reconciliation cutoff",
        vacuum_table=lambda path: delta_core.vacuum_delta_table(
            cfg.AZURE_CONTAINER_SILVER,
            path,
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
            full=True,
        ),
    )
    if cutoff_stats.rows_dropped > 0 or cutoff_stats.tables_rewritten > 0 or cutoff_stats.deleted_blobs > 0:
        mdc.write_line(
            "Silver finance reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(
            f"Silver finance reconciliation cutoff sweep encountered errors={cutoff_stats.errors}."
        )
    return len(orphan_symbols), deleted_blobs


def _normalize_key(name: Any) -> str:
    return _KEY_NORMALIZER.sub("", str(name).strip().lower())


def _try_parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None

    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"none", "null", "nan", "n/a", "na", "-"}:
        return None

    text = text.replace(",", "")
    try:
        return float(text)
    except Exception:
        return None


def _get_first_float(payload: dict[str, Any], candidates: list[str]) -> Optional[float]:
    normalized = {_normalize_key(k): v for k, v in payload.items()}
    for candidate in candidates:
        value = normalized.get(_normalize_key(candidate))
        parsed = _try_parse_float(value)
        if parsed is not None:
            return parsed
    return None


def _load_close_prices(ticker: str) -> pd.DataFrame:
    """
    Load close price series for valuation approximation from Silver market Delta only.
    Fallback sources are intentionally disabled.
    """
    market_path = DataPaths.get_market_data_path(ticker.replace(".", "-"))
    df = delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, market_path, columns=["date", "close"])
    if df is None or df.empty or not {"date", "close"}.issubset(df.columns):
        return pd.DataFrame()

    out = df[["date", "close"]].copy()
    out = out.rename(columns={"date": "Date", "close": "Close"})
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce", utc=True).dt.tz_convert(None)
    out["Close"] = pd.to_numeric(out["Close"], errors="coerce")
    out = out.dropna(subset=["Date", "Close"]).sort_values("Date").reset_index(drop=True)
    return out


def _build_valuation_timeseries_from_overview(payload: dict[str, Any], *, ticker: str) -> pd.DataFrame:
    """
    Alpha Vantage does not provide a full historical valuation time series. We approximate it by
    scaling "current" ratios from `OVERVIEW` by historical close prices.

    This yields a best-effort daily valuation series that is consistent on the latest close date
    (i.e., latest date matches the overview ratio).
    """
    df_prices = _load_close_prices(ticker)

    market_cap_now = _get_first_float(payload, ["MarketCapitalization", "Market Cap", "MarketCap"])
    pe_now = _get_first_float(payload, ["PERatio", "P/E", "PE Ratio"])
    forward_pe_now = _get_first_float(payload, ["ForwardPE", "Forward P/E", "Forward PE"])
    ev_ebitda_now = _get_first_float(payload, ["EVToEBITDA", "EV/EBITDA", "EV To EBITDA"])
    ev_revenue_now = _get_first_float(payload, ["EVToRevenue", "EV/Revenue", "EV To Revenue"])
    shares_outstanding_now = _get_first_float(payload, ["SharesOutstanding", "Shares Outstanding"])
    ebitda_now = _get_first_float(payload, ["EBITDA"])

    if df_prices is None or df_prices.empty:
        return pd.DataFrame()

    close_now = float(df_prices["Close"].iloc[-1])
    if close_now <= 0:
        return pd.DataFrame()

    scale = df_prices["Close"] / close_now

    shares_outstanding = shares_outstanding_now
    if shares_outstanding is None and market_cap_now is not None:
        shares_outstanding = market_cap_now / close_now

    out = pd.DataFrame({"Date": df_prices["Date"], "Symbol": ticker})

    if shares_outstanding is not None:
        out["shares_outstanding"] = float(shares_outstanding)

    if market_cap_now is not None:
        if shares_outstanding is not None:
            out["market_cap"] = df_prices["Close"] * float(shares_outstanding)
        else:
            out["market_cap"] = float(market_cap_now) * scale

    if pe_now is not None:
        out["pe_ratio"] = float(pe_now) * scale
    if forward_pe_now is not None:
        out["forward_pe"] = float(forward_pe_now) * scale
    if ev_ebitda_now is not None:
        out["ev_ebitda"] = float(ev_ebitda_now) * scale
    if ev_revenue_now is not None:
        out["ev_revenue"] = float(ev_revenue_now) * scale
    if ebitda_now is not None:
        out["ebitda"] = float(ebitda_now)

    out["Date"] = pd.to_datetime(out["Date"], errors="coerce", utc=True).dt.tz_convert(None)
    out = out.dropna(subset=["Date"]).sort_values(["Date"]).reset_index(drop=True)
    return out


def _read_finance_json(raw_bytes: bytes, *, ticker: str, suffix: str) -> pd.DataFrame:
    payload = json.loads(raw_bytes.decode("utf-8"))

    # Fundamentals endpoints return quarterlyReports/annualReports; OVERVIEW is a flat dict.
    if suffix in {"quarterly_balance-sheet", "quarterly_cash-flow", "quarterly_financials"}:
        reports = payload.get("quarterlyReports") or []
        if not isinstance(reports, list) or not reports:
            return pd.DataFrame()

        rows = []
        for item in reports:
            if not isinstance(item, dict):
                continue
            date_raw = item.get("fiscalDateEnding")
            if not date_raw:
                continue
            row = {"Date": str(date_raw).strip(), "Symbol": ticker}
            for k, v in item.items():
                if k == "fiscalDateEnding":
                    continue
                # Keep values as strings for downstream Delta writes (avoids mixed object types).
                row[str(k)] = "" if v is None else str(v)
            rows.append(row)

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.tz_convert(None)
        df = df.dropna(subset=["Date"]).sort_values(["Date"]).reset_index(drop=True)
        return df

    # "Valuation" bucket: approximate a daily valuation series from OVERVIEW + historical closes.
    if suffix == "quarterly_valuation_measures" and isinstance(payload, dict):
        return _build_valuation_timeseries_from_overview(payload, ticker=ticker)

    return pd.DataFrame()


def _utc_today() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc).date())


def resample_daily_ffill(df: pd.DataFrame, *, extend_to: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Resamples sparse dataframe to daily frequency using forward fill.
    """
    if "Date" not in df.columns:
        return df

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.tz_convert(None)
    df = df.dropna(subset=["Date"])
    if df.empty:
        return df

    df = df.set_index("Date")
    df = df.sort_index()

    # Resample and ffill
    # We must restrict to the known date range
    if df.empty:
        return df

    end = df.index.max()
    if extend_to is not None and extend_to > end:
        end = extend_to

    full_range = pd.date_range(start=df.index.min(), end=end, freq="D", name="Date")
    df_daily = df.reindex(full_range).ffill()

    return df_daily.reset_index()


def _align_to_existing_schema(df: pd.DataFrame, container: str, path: str) -> pd.DataFrame:
    return align_to_existing_schema(df, container=container, path=path)


def _repair_symbol_column_aliases(df: pd.DataFrame, *, ticker: str) -> pd.DataFrame:
    out = df.copy()
    legacy_symbol_cols = [
        col
        for col in out.columns
        if isinstance(col, str) and col.startswith("symbol_") and col[7:].isdigit()
    ]
    if not legacy_symbol_cols:
        return out

    if "symbol" not in out.columns:
        first_legacy = legacy_symbol_cols[0]
        out = out.rename(columns={first_legacy: "symbol"})
        legacy_symbol_cols = legacy_symbol_cols[1:]
        mdc.write_warning(
            f"Silver finance {ticker}: renamed legacy column {first_legacy} -> symbol."
        )

    for col in legacy_symbol_cols:
        if col not in out.columns:
            continue
        primary = out["symbol"].astype("string")
        fallback = out[col].astype("string")
        conflicts = int((primary.notna() & fallback.notna() & (primary != fallback)).sum())
        if conflicts > 0:
            mdc.write_warning(
                f"Silver finance {ticker}: symbol repair conflict in {col}; "
                f"conflicting_rows={conflicts}; keeping existing symbol when both populated."
            )
        out["symbol"] = out["symbol"].combine_first(out[col])
        out = out.drop(columns=[col])
        mdc.write_warning(
            f"Silver finance {ticker}: collapsed legacy column {col} into symbol."
        )

    return out


def process_blob(
    blob,
    *,
    desired_end: pd.Timestamp,
    backfill_start: Optional[pd.Timestamp] = None,
    watermarks: dict,
) -> BlobProcessResult:
    blob_name = blob["name"]
    parsed_blob = _parse_supported_blob_name(blob_name)
    if parsed_blob is None:
        return BlobProcessResult(
            blob_name=blob_name,
            silver_path=None,
            ticker=None,
            status="failed",
            error=f"Unsupported finance blob format: {blob_name}",
        )
    folder_name, ticker, suffix = parsed_blob

    # Silver Path
    # Use DataPaths or manual? DataPaths uses folder name.
    # DataPaths.get_finance_path(folder_name, ticker, suffix)
    silver_path = DataPaths.get_finance_path(folder_name, ticker, suffix)

    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS and ticker not in cfg.DEBUG_SYMBOLS:
        return BlobProcessResult(
            blob_name=blob_name,
            silver_path=silver_path,
            ticker=ticker,
            status="skipped",
        )

    unchanged, signature = check_blob_unchanged(blob, watermarks.get(blob_name))
    if unchanged:
        return BlobProcessResult(
            blob_name=blob_name,
            silver_path=silver_path,
            ticker=ticker,
            status="skipped",
        )

    mdc.write_line(f"Processing {ticker} {folder_name}...")

    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_raw = _read_finance_json(raw_bytes, ticker=ticker, suffix=suffix)

        if df_raw is None or df_raw.empty:
            return BlobProcessResult(
                blob_name=blob_name,
                silver_path=silver_path,
                ticker=ticker,
                status="failed",
                error=f"Empty finance payload: {blob_name}",
            )

        df_clean = df_raw

        try:
            df_clean = require_non_empty_frame(df_clean, context=f"finance preflight {blob_name}")
            df_clean = normalize_date_column(
                df_clean,
                context=f"finance date parse {blob_name}",
                aliases=("Date", "date"),
                canonical="Date",
            )
            df_clean = assert_no_unexpected_mixed_empty(
                df_clean, context=f"finance date filter {blob_name}", alias="Date"
            )
        except ContractViolation as exc:
            log_contract_violation(f"finance preflight failed for {blob_name}", exc, severity="ERROR")
            return BlobProcessResult(
                blob_name=blob_name,
                silver_path=silver_path,
                ticker=ticker,
                status="failed",
                error=str(exc),
            )

        df_clean, _ = apply_backfill_start_cutoff(
            df_clean,
            date_col="Date",
            backfill_start=backfill_start,
            context=f"silver finance {ticker}",
        )
        if backfill_start is not None and (df_clean is None or df_clean.empty):
            if silver_client is not None:
                deleted = silver_client.delete_prefix(silver_path)
                mdc.write_line(
                    f"Silver finance backfill purge for {ticker}: no rows >= {backfill_start.date().isoformat()}, "
                    f"deleted {deleted} blob(s) under {silver_path}."
                )
                watermark_signature = None
                if signature:
                    watermark_signature = dict(signature)
                    watermark_signature["updated_at"] = datetime.now(timezone.utc).isoformat()
                return BlobProcessResult(
                    blob_name=blob_name,
                    silver_path=silver_path,
                    ticker=ticker,
                    status="ok",
                    rows_written=0,
                    watermark_signature=watermark_signature,
                )
            return BlobProcessResult(
                blob_name=blob_name,
                silver_path=silver_path,
                ticker=ticker,
                status="failed",
                error=f"Storage client unavailable for cutoff purge {silver_path}.",
            )

        # Resample to daily frequency (forward fill)
        df_clean = resample_daily_ffill(df_clean, extend_to=desired_end)
        if df_clean is None or df_clean.empty:
            return BlobProcessResult(
                blob_name=blob_name,
                silver_path=silver_path,
                ticker=ticker,
                status="failed",
                error="No valid dated rows after cleaning/resample.",
            )

        # Write to Silver (Overwrite is fine for finance snapshots, or merge?
        # Typically Finance Sheets are full snapshots. Replacing is safer for consistency,
        # but if we want history of OLD financial restatements...
        # Current logic: Overwrite/Upsert. store_delta defaults to append?
        # Let's use overwrite mode for now as Transposed data is simpler.
        # But wait, store_delta default implementation?
        # Checking delta_core usage: usually it appends or overwrites.
        # I'll check store_delta signature in next turn if needed.
        # Assuming overwrite for the partition/table is safest for now to avoid specific duplicates.

        df_clean = _align_to_existing_schema(df_clean, cfg.AZURE_CONTAINER_SILVER, silver_path)
        df_clean = normalize_columns_to_snake_case(df_clean)
        df_clean = _repair_symbol_column_aliases(df_clean, ticker=ticker)
        delta_core.store_delta(
            df_clean,
            cfg.AZURE_CONTAINER_SILVER,
            silver_path,
            mode="overwrite",
        )
        if backfill_start is not None:
            delta_core.vacuum_delta_table(
                cfg.AZURE_CONTAINER_SILVER,
                silver_path,
                retention_hours=0,
                dry_run=False,
                enforce_retention_duration=False,
                full=True,
            )
        mdc.write_line(f"Updated Silver {silver_path}")
        watermark_signature = None
        if signature:
            watermark_signature = dict(signature)
            watermark_signature["updated_at"] = datetime.now(timezone.utc).isoformat()
        return BlobProcessResult(
            blob_name=blob_name,
            silver_path=silver_path,
            ticker=ticker,
            status="ok",
            rows_written=int(len(df_clean)),
            watermark_signature=watermark_signature,
        )

    except Exception as e:
        mdc.write_error(f"Failed to process {blob_name}: {e}")
        return BlobProcessResult(
            blob_name=blob_name,
            silver_path=silver_path,
            ticker=ticker,
            status="failed",
            error=str(e),
        )


def _process_candidate_blobs(
    *,
    candidate_blobs: list[dict],
    desired_end: pd.Timestamp,
    backfill_start: Optional[pd.Timestamp],
    watermarks: dict,
) -> tuple[list[BlobProcessResult], float]:
    max_workers = _get_ingest_max_workers()
    mdc.write_line(f"Silver finance ingest workers={max_workers} candidates={len(candidate_blobs)}.")

    ingest_started = time.perf_counter()
    results: list[BlobProcessResult] = []
    if max_workers <= 1:
        for blob in candidate_blobs:
            results.append(
                process_blob(
                    blob,
                    desired_end=desired_end,
                    backfill_start=backfill_start,
                    watermarks=watermarks,
                )
            )
    else:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="silver-finance") as executor:
            futures = {
                executor.submit(
                    process_blob,
                    blob,
                    desired_end=desired_end,
                    backfill_start=backfill_start,
                    watermarks=watermarks,
                ): blob
                for blob in candidate_blobs
            }
            for future in as_completed(futures):
                blob = futures[future]
                blob_name = str(blob.get("name", "unknown"))
                try:
                    results.append(future.result())
                except Exception as exc:
                    mdc.write_error(f"Unhandled failure while processing {blob_name}: {exc}")
                    results.append(
                        BlobProcessResult(
                            blob_name=blob_name,
                            silver_path=None,
                            ticker=None,
                            status="failed",
                            error=str(exc),
                        )
                    )
    ingest_elapsed = time.perf_counter() - ingest_started
    return results, ingest_elapsed


def main() -> int:
    mdc.log_environment_diagnostics()
    run_started_at = datetime.now(timezone.utc)
    watermarks = load_watermarks("bronze_finance_data")
    last_success = load_last_success("silver_finance_data")
    watermarks_dirty = False

    desired_end = _utc_today()
    backfill_start, _ = get_backfill_range()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to silver finance data: {backfill_start.date().isoformat()}")
    mdc.write_line("Listing Bronze Finance files...")

    manifest_run_id: Optional[str] = None
    manifest_path: Optional[str] = None
    initial_source = "live-listing"
    initial_blobs: list[dict] = []
    deduped_total = 0

    if silver_manifest_consumption_enabled():
        manifest = load_latest_bronze_finance_manifest()
        if isinstance(manifest, dict):
            candidate_run_id = str(manifest.get("runId") or "").strip()
            candidate_manifest_path = str(manifest.get("manifestPath") or "").strip()
            if candidate_run_id and not silver_finance_ack_exists(candidate_run_id):
                manifest_run_id = candidate_run_id
                manifest_path = candidate_manifest_path
                initial_blobs = _select_preferred_blob_candidates(manifest_blobs(manifest))
                initial_source = "bronze-manifest"
                mdc.write_line(
                    "Silver finance will consume bronze manifest runId={run_id} manifestBlobs={blob_count}.".format(
                        run_id=manifest_run_id, blob_count=len(initial_blobs)
                    )
                )
            elif candidate_run_id:
                mdc.write_line(
                    f"Latest bronze finance manifest runId={candidate_run_id} is already acknowledged; "
                    "falling back to live listing."
                )

    if not initial_blobs:
        initial_blobs, deduped = _list_bronze_finance_candidates()
        deduped_total += deduped

    max_passes = _get_catchup_max_passes()
    pass_count = 0
    seen_blob_names: set[str] = set()
    newly_discovered_blob_names: set[str] = set()
    checkpoint_skipped_first_pass = 0
    checkpoint_skipped_total = 0
    all_results: list[BlobProcessResult] = []
    total_ingest_elapsed = 0.0
    reconciliation_source_blobs: list[dict] = list(initial_blobs)

    while pass_count < max_passes:
        pass_count += 1
        if pass_count == 1:
            blobs = list(initial_blobs)
        else:
            blobs, deduped = _list_bronze_finance_candidates()
            deduped_total += deduped

        current_blob_names = {str(item.get("name", "")).strip() for item in blobs if item.get("name")}
        reconciliation_source_blobs = list(blobs)
        if pass_count > 1:
            newly_seen = current_blob_names - seen_blob_names
            if newly_seen:
                newly_discovered_blob_names.update(newly_seen)
                mdc.write_line(
                    f"Silver finance catch-up pass discovered {len(newly_seen)} newly listed Bronze blob(s)."
                )
        seen_blob_names.update(current_blob_names)

        candidate_blobs, checkpoint_skipped = _build_checkpoint_candidates(
            blobs=blobs,
            watermarks=watermarks,
            last_success=last_success,
        )
        checkpoint_skipped_total += checkpoint_skipped
        if pass_count == 1:
            checkpoint_skipped_first_pass = checkpoint_skipped
            if last_success is not None:
                mdc.write_line(
                    "Silver finance checkpoint filter: "
                    f"last_success={last_success.isoformat()} candidates={len(candidate_blobs)} "
                    f"skipped_checkpoint={checkpoint_skipped_first_pass}"
                )

        mdc.write_line(
            "Silver finance ingest pass {pass_no}/{max_passes}: source={source} total={total} "
            "candidates={candidates} skipped_checkpoint={skipped}.".format(
                pass_no=pass_count,
                max_passes=max_passes,
                source=initial_source if pass_count == 1 else "live-listing",
                total=len(blobs),
                candidates=len(candidate_blobs),
                skipped=checkpoint_skipped,
            )
        )
        if not candidate_blobs:
            break

        pass_results, pass_elapsed = _process_candidate_blobs(
            candidate_blobs=candidate_blobs,
            desired_end=desired_end,
            backfill_start=backfill_start,
            watermarks=watermarks,
        )
        total_ingest_elapsed += pass_elapsed
        all_results.extend(pass_results)
        for result in pass_results:
            if result.status != "ok" or not result.watermark_signature:
                continue
            watermarks[result.blob_name] = result.watermark_signature
            watermarks_dirty = True

    lag_candidate_count = 0
    try:
        latest_blobs, deduped = _list_bronze_finance_candidates()
        reconciliation_source_blobs = list(latest_blobs)
        deduped_total += deduped
        lag_candidates, _ = _build_checkpoint_candidates(
            blobs=latest_blobs,
            watermarks=watermarks,
            last_success=last_success,
        )
        lag_candidate_count = len(lag_candidates)
    except Exception as exc:
        mdc.write_warning(f"Silver finance lag probe failed: {exc}")

    processed = sum(1 for r in all_results if r.status == "ok")
    skipped = sum(1 for r in all_results if r.status == "skipped")
    failed = sum(1 for r in all_results if r.status == "failed")
    attempts = len(all_results)
    distinct_tickers = len({str(r.ticker).strip() for r in all_results if r.ticker})
    rows_written = sum(int(r.rows_written or 0) for r in all_results if r.status == "ok")
    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0
    try:
        reconciliation_orphans, reconciliation_deleted_blobs = _run_finance_reconciliation(
            bronze_blob_list=reconciliation_source_blobs
        )
    except Exception as exc:
        reconciliation_failed = 1
        mdc.write_error(f"Silver finance reconciliation failed: {exc}")

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Silver finance ingest complete: "
        f"attempts={attempts}, ok={processed}, skipped={skipped}, failed={total_failed}, "
        f"skippedCheckpoint={checkpoint_skipped_first_pass}, "
        f"distinctSymbols={distinct_tickers}, rowsWritten={rows_written}, elapsedSec={total_ingest_elapsed:.2f}, "
        f"passes={pass_count}, newlyDiscoveredAfterFirstPass={len(newly_discovered_blob_names)}, "
        f"lagCandidates={lag_candidate_count}, reconciled_orphans={reconciliation_orphans}, "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs}"
    )
    if watermarks_dirty:
        save_watermarks("bronze_finance_data", watermarks)

    run_ended_at = datetime.now(timezone.utc)
    manifest_ack_path: Optional[str] = None
    if total_failed == 0:
        checkpoint_metadata = {
            "total_blobs": len(initial_blobs),
            "source": initial_source,
            "candidates": attempts,
            "attempts": attempts,
            "processed": processed,
            "skipped": skipped,
            "skipped_checkpoint": checkpoint_skipped_first_pass,
            "skipped_checkpoint_total": checkpoint_skipped_total,
            "rows_written": rows_written,
            "elapsed_seconds": round(total_ingest_elapsed, 3),
            "catchup_passes": pass_count,
            "new_blobs_discovered_after_first_pass": len(newly_discovered_blob_names),
            "lag_candidate_count": lag_candidate_count,
            "run_started_at": run_started_at.isoformat(),
            "run_ended_at": run_ended_at.isoformat(),
            "deduped_candidates_total": deduped_total,
            "manifest_run_id": manifest_run_id,
            "manifest_path": manifest_path,
            "reconciled_orphans": reconciliation_orphans,
            "reconciliation_deleted_blobs": reconciliation_deleted_blobs,
        }
        save_last_success(
            "silver_finance_data",
            when=run_ended_at,
            metadata=checkpoint_metadata,
        )
        if manifest_run_id:
            manifest_ack_path = write_silver_finance_ack(
                run_id=manifest_run_id,
                manifest_path=manifest_path or "",
                status="succeeded",
                metadata={
                    "processed": processed,
                    "failed": total_failed,
                    "skipped": skipped,
                    "attempts": attempts,
                    "rows_written": rows_written,
                    "run_started_at": run_started_at.isoformat(),
                    "run_ended_at": run_ended_at.isoformat(),
                },
            )
            if manifest_ack_path:
                mdc.write_line(f"Silver finance manifest ack written: runId={manifest_run_id} path={manifest_ack_path}")
        return 0
    if manifest_run_id:
        mdc.write_warning(f"Silver finance run failed; bronze manifest runId={manifest_run_id} was not acknowledged.")
    return 1


if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "silver-finance-job"
    shared_lock_name = (os.environ.get("FINANCE_PIPELINE_SHARED_LOCK_NAME") or _DEFAULT_FINANCE_SHARED_LOCK).strip()
    shared_wait_timeout = _parse_wait_timeout_seconds(
        os.environ.get("SILVER_FINANCE_SHARED_LOCK_WAIT_SECONDS"),
        default=_DEFAULT_SILVER_SHARED_LOCK_WAIT_SECONDS,
    )
    with mdc.JobLock(shared_lock_name, wait_timeout_seconds=shared_wait_timeout):
        ensure_api_awake_from_env(required=True)
        exit_code = main()
        if exit_code == 0:
            write_system_health_marker(layer="silver", domain="finance", job_name=job_name)
            trigger_next_job_from_env()
        raise SystemExit(exit_code)
