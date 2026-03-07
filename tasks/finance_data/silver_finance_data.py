from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
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
from tasks.common import bronze_bucketing
from tasks.common import domain_artifacts
from tasks.common import layer_bucketing
from tasks.common.finance_contracts import (
    PIOTROSKI_ALPHA26_REPORT_LAYOUTS,
    PIOTROSKI_FINANCE_SUBDOMAINS,
    SILVER_FINANCE_PIOTROSKI_COLUMNS_BY_SUBDOMAIN,
    SILVER_FINANCE_PIOTROSKI_SOURCE_ALIASES_BY_SUBDOMAIN,
    SKIPPED_PIOTROSKI_ALPHA26_REPORT_TYPES,
)
from tasks.common import run_manifests
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from tasks.common.watermarks import (
    build_blob_signature,
    load_last_success,
    load_watermarks,
    save_last_success,
    save_watermarks,
    should_process_blob_since_last_success,
)
from tasks.common.delta_write_policy import prepare_delta_write_frame
from tasks.common.silver_contracts import (
    ContractViolation,
    align_to_existing_schema,
    assert_no_unexpected_mixed_empty,
    log_contract_violation,
    normalize_date_column,
    require_non_empty_frame,
    normalize_columns_to_snake_case,
)
from tasks.common.silver_precision import apply_precision_policy
from tasks.common.market_reconciliation import (
    collect_bronze_finance_symbols_from_blob_infos,
    collect_delta_silver_finance_symbols,
    enforce_backfill_cutoff_on_bucket_tables,
    purge_orphan_rows_from_bucket_tables,
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


@dataclass(frozen=True)
class _ManifestSelection:
    source: str
    blobs: list[dict]
    deduped: int
    manifest_run_id: Optional[str] = None
    manifest_path: Optional[str] = None
    manifest_blob_count: int = 0
    manifest_filtered_bucket_blob_count: int = 0


_KEY_NORMALIZER = re.compile(r"[^a-z0-9]+")
_ALPHA26_REPORT_TYPE_TO_TABLE: dict[str, tuple[str, str]] = dict(PIOTROSKI_ALPHA26_REPORT_LAYOUTS)
_DEFAULT_FINANCE_SHARED_LOCK = "finance-pipeline-shared"
_DEFAULT_SILVER_SHARED_LOCK_WAIT_SECONDS = 3600.0
_DEFAULT_CATCHUP_MAX_PASSES = 3
_FINANCE_ALPHA26_SUBDOMAINS: Tuple[str, ...] = PIOTROSKI_FINANCE_SUBDOMAINS


def _get_positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(str(raw).strip())
    except Exception:
        return default
    return value if value > 0 else default


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


def _list_alpha26_finance_bucket_candidates() -> tuple[list[dict], int]:
    listed_blobs = bronze_client.list_blob_infos(name_starts_with="finance-data/buckets/")
    blobs = [
        blob
        for blob in listed_blobs
        if str(blob.get("name", "")).startswith("finance-data/buckets/")
        and str(blob.get("name", "")).endswith(".parquet")
    ]
    blobs.sort(key=lambda item: str(item.get("name", "")))
    unsupported = max(0, len(listed_blobs) - len(blobs))
    if unsupported > 0:
        mdc.write_line(
            f"Silver finance alpha26 input filter removed {unsupported} unsupported Bronze blob candidate(s); "
            f"processing {len(blobs)} bucket inputs."
        )
    return blobs, 0


def _filter_alpha26_manifest_bucket_blobs(manifest: dict[str, Any]) -> list[dict]:
    filtered: list[dict] = []
    for blob in run_manifests.manifest_blobs(manifest):
        name = str(blob.get("name", "")).strip()
        if not name.startswith("finance-data/buckets/"):
            continue
        if not name.endswith(".parquet"):
            continue
        filtered.append(blob)
    filtered.sort(key=lambda item: str(item.get("name", "")))
    return filtered


def _select_initial_alpha26_source() -> _ManifestSelection:
    if run_manifests.silver_manifest_consumption_enabled():
        manifest = run_manifests.load_latest_bronze_finance_manifest()
        if isinstance(manifest, dict):
            run_id = str(manifest.get("runId", "")).strip()
            manifest_path = str(manifest.get("manifestPath", "")).strip()
            if run_id and run_manifests.silver_finance_ack_exists(run_id):
                mdc.write_line(f"Silver finance manifest run already acknowledged; falling back to listing runId={run_id}.")
            elif run_id and manifest_path:
                filtered = _filter_alpha26_manifest_bucket_blobs(manifest)
                manifest_blob_count = len(run_manifests.manifest_blobs(manifest))
                mdc.write_line(
                    "Silver finance selected manifest source: "
                    f"runId={run_id} manifestPath={manifest_path} "
                    f"manifestBlobs={manifest_blob_count} bucketBlobs={len(filtered)}"
                )
                return _ManifestSelection(
                    source="bronze-manifest",
                    blobs=filtered,
                    deduped=0,
                    manifest_run_id=run_id,
                    manifest_path=manifest_path,
                    manifest_blob_count=manifest_blob_count,
                    manifest_filtered_bucket_blob_count=len(filtered),
                )
            else:
                mdc.write_warning("Silver finance manifest pointer missing runId/path; falling back to listing.")

    listed, deduped = _list_alpha26_finance_bucket_candidates()
    return _ManifestSelection(source="alpha26-bucket-listing", blobs=listed, deduped=deduped)


def _build_alpha26_checkpoint_candidates(
    *,
    blobs: list[dict],
    watermarks: dict,
    last_success: Optional[datetime],
    force_reprocess: bool = False,
) -> tuple[list[dict], int]:
    checkpoint_skipped = 0
    candidates: list[dict] = []
    for blob in blobs:
        prior = watermarks.get(str(blob.get("name", "")))
        should_process = should_process_blob_since_last_success(
            blob,
            prior_signature=prior,
            last_success_at=last_success,
            force_reprocess=force_reprocess,
        )
        if should_process:
            candidates.append(blob)
        else:
            checkpoint_skipped += 1
    return candidates, checkpoint_skipped


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


def _get_first_value(payload: dict[str, Any], candidates: tuple[str, ...]) -> Any:
    normalized = {_normalize_key(k): v for k, v in payload.items()}
    for candidate in candidates:
        value = normalized.get(_normalize_key(candidate))
        if value is not None:
            return value
    return None


def _read_finance_json(raw_bytes: bytes, *, ticker: str, suffix: str) -> pd.DataFrame:
    payload = json.loads(raw_bytes.decode("utf-8"))

    suffix_to_sub_domain = {
        report_suffix: sub_domain
        for sub_domain, (_folder_name, report_suffix) in PIOTROSKI_ALPHA26_REPORT_LAYOUTS.items()
    }
    sub_domain = suffix_to_sub_domain.get(suffix)
    if sub_domain is None:
        return pd.DataFrame()

    reports = payload.get("quarterlyReports") or []
    if not isinstance(reports, list) or not reports:
        return pd.DataFrame()

    alias_map = SILVER_FINANCE_PIOTROSKI_SOURCE_ALIASES_BY_SUBDOMAIN[sub_domain]
    expected_columns = SILVER_FINANCE_PIOTROSKI_COLUMNS_BY_SUBDOMAIN[sub_domain]
    rows = []
    for item in reports:
        if not isinstance(item, dict):
            continue
        date_raw = item.get("fiscalDateEnding")
        if not date_raw:
            continue
        row: dict[str, Any] = {"Date": str(date_raw).strip(), "Symbol": ticker}
        for column in expected_columns[2:]:
            row[column] = _try_parse_float(_get_first_value(item, alias_map[column]))
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    for column in expected_columns[2:]:
        if column not in df.columns:
            df[column] = pd.Series(dtype="float64")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.tz_convert(None)
    df = df.dropna(subset=["Date"]).sort_values(["Date"]).reset_index(drop=True)
    return df[["Date", "Symbol", *expected_columns[2:]]]


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


def _finance_sub_domain(folder_name: str) -> str:
    key = str(folder_name or "").strip().lower().replace("-", " ").replace("_", " ")
    key = " ".join(key.split())
    if key == "balance sheet":
        return "balance_sheet"
    if key == "income statement":
        return "income_statement"
    if key == "cash flow":
        return "cash_flow"
    if key == "valuation":
        return "valuation"
    return key.replace(" ", "_")


def _split_finance_bucket_rows(df_bucket: Optional[pd.DataFrame], *, ticker: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df_bucket is None or df_bucket.empty:
        empty = pd.DataFrame()
        return empty, empty

    out = df_bucket.copy()
    if "Date" in out.columns and "date" not in out.columns:
        out = out.rename(columns={"Date": "date"})
    if "symbol" not in out.columns and "Symbol" in out.columns:
        out = out.rename(columns={"Symbol": "symbol"})
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
    if "symbol" not in out.columns:
        out["symbol"] = pd.NA
    out["symbol"] = out["symbol"].astype("string").str.upper()
    symbol = str(ticker or "").strip().upper()
    symbol_mask = out["symbol"] == symbol
    return out.loc[symbol_mask].copy(), out.loc[~symbol_mask].copy()


def _write_alpha26_finance_silver_buckets(
    bucket_frames: dict[tuple[str, str], list[pd.DataFrame]],
) -> tuple[int, Optional[str], Optional[int]]:
    symbol_to_bucket: dict[str, str] = {}
    symbols_by_sub_domain: dict[str, dict[str, str]] = {key: {} for key in _FINANCE_ALPHA26_SUBDOMAINS}
    for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS:
        for bucket in layer_bucketing.ALPHABET_BUCKETS:
            silver_bucket_path = DataPaths.get_silver_finance_bucket_path(sub_domain, bucket)
            parts = bucket_frames.get((sub_domain, bucket), [])
            if parts:
                df_bucket = pd.concat(parts, ignore_index=True)
                if "symbol" in df_bucket.columns and "date" in df_bucket.columns:
                    df_bucket["symbol"] = df_bucket["symbol"].astype(str).str.upper()
                    df_bucket["date"] = pd.to_datetime(df_bucket["date"], errors="coerce")
                    df_bucket = df_bucket.dropna(subset=["symbol", "date"]).copy()
                    df_bucket = df_bucket.sort_values(["symbol", "date"]).drop_duplicates(
                        subset=["symbol", "date"], keep="last"
                    )
                    for symbol in df_bucket["symbol"].dropna().astype(str).tolist():
                        if symbol:
                            symbol_to_bucket[symbol] = bucket
                            symbols_by_sub_domain[sub_domain][symbol] = bucket
                else:
                    df_bucket = pd.DataFrame(columns=["date", "symbol"])
            else:
                df_bucket = pd.DataFrame(columns=["date", "symbol"])

            write_decision = prepare_delta_write_frame(
                df_bucket.reset_index(drop=True),
                container=cfg.AZURE_CONTAINER_SILVER,
                path=silver_bucket_path,
            )
            mdc.write_line(
                "delta_write_decision layer=silver domain=finance "
                f"bucket={bucket} action={'skip' if write_decision.action == 'skip_empty_no_schema' else 'write'} "
                f"reason={write_decision.reason} path={silver_bucket_path}"
            )
            if write_decision.action == "skip_empty_no_schema":
                mdc.write_line(
                    f"Skipping Silver finance empty bucket write for {silver_bucket_path}: no existing Delta schema."
                )
                continue

            delta_core.store_delta(
                write_decision.frame,
                cfg.AZURE_CONTAINER_SILVER,
                silver_bucket_path,
                mode="overwrite",
            )
            try:
                domain_artifacts.write_bucket_artifact(
                    layer="silver",
                    domain="finance",
                    sub_domain=sub_domain,
                    bucket=bucket,
                    df=write_decision.frame,
                    date_column="date",
                    client=silver_client,
                    job_name="silver-finance-job",
                )
            except Exception as exc:
                mdc.write_warning(
                    f"Silver finance metadata bucket artifact write failed sub_domain={sub_domain} bucket={bucket}: {exc}"
                )
    root_index_path = layer_bucketing.write_layer_symbol_index(
        layer="silver",
        domain="finance",
        symbol_to_bucket=symbol_to_bucket,
    )
    index_path = root_index_path
    column_count: Optional[int] = None
    finance_subdomain_artifacts: dict[str, dict[str, Any]] = {}
    for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS:
        sub_index_path = layer_bucketing.write_layer_symbol_index(
            layer="silver",
            domain="finance",
            symbol_to_bucket=symbols_by_sub_domain.get(sub_domain, {}),
            sub_domain=sub_domain,
        )
        if sub_index_path:
            try:
                payload = domain_artifacts.write_domain_artifact(
                    layer="silver",
                    domain="finance",
                    sub_domain=sub_domain,
                    date_column="date",
                    client=silver_client,
                    symbol_count_override=len(symbols_by_sub_domain.get(sub_domain, {})),
                    symbol_index_path=sub_index_path,
                    job_name="silver-finance-job",
                )
                if payload is not None:
                    finance_subdomain_artifacts[sub_domain] = payload
            except Exception as exc:
                mdc.write_warning(
                    f"Silver finance metadata artifact write failed for sub_domain={sub_domain}: {exc}"
                )
            index_path = sub_index_path
    if root_index_path:
        try:
            payload = domain_artifacts.write_domain_artifact(
                layer="silver",
                domain="finance",
                date_column="date",
                client=silver_client,
                symbol_count_override=len(symbol_to_bucket),
                symbol_index_path=root_index_path,
                job_name="silver-finance-job",
                finance_subdomains=finance_subdomain_artifacts,
            )
            column_count = domain_artifacts.extract_column_count(payload)
        except Exception as exc:
            mdc.write_warning(f"Silver finance metadata artifact write failed: {exc}")
    return len(symbol_to_bucket), index_path, column_count


def _run_finance_reconciliation(*, bronze_blob_list: list[dict]) -> tuple[int, int]:
    if silver_client is None:
        raise RuntimeError("Silver finance reconciliation requires silver storage client.")

    bronze_symbols = collect_bronze_finance_symbols_from_blob_infos(bronze_blob_list)
    silver_symbols = collect_delta_silver_finance_symbols(client=silver_client)
    orphan_symbols, purge_stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols=bronze_symbols,
        downstream_symbols=silver_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_silver_finance_bucket_path(sub_domain, layer_bucketing.bucket_letter(symbol))
            for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS
        ],
        load_table=lambda path: delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, path),
        store_table=lambda df, path: delta_core.store_delta(df, cfg.AZURE_CONTAINER_SILVER, path, mode="overwrite"),
        delete_prefix=silver_client.delete_prefix,
        vacuum_table=lambda path: delta_core.vacuum_delta_table(
            cfg.AZURE_CONTAINER_SILVER,
            path,
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
            full=True,
        ),
    )
    deleted_blobs = purge_stats.deleted_blobs
    if orphan_symbols:
        mdc.write_line(
            "Silver finance reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
            f"tables_rewritten={purge_stats.tables_rewritten} rows_deleted={purge_stats.rows_deleted}"
        )
    else:
        mdc.write_line("Silver finance reconciliation: no orphan symbols detected.")
    if purge_stats.errors > 0:
        mdc.write_warning(f"Silver finance orphan purge encountered errors={purge_stats.errors}.")

    backfill_start, _ = get_backfill_range()
    cutoff_stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=[
            DataPaths.get_silver_finance_bucket_path(sub_domain, bucket)
            for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS
            for bucket in layer_bucketing.ALPHABET_BUCKETS
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
    status = "failed" if cutoff_stats.errors > 0 else "ok"
    mdc.write_line(
        "reconciliation_result layer=silver domain=finance "
        f"status={status} orphan_count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
        f"cutoff_rows_dropped={cutoff_stats.rows_dropped} cutoff_tables_rewritten={cutoff_stats.tables_rewritten} "
        f"cutoff_errors={cutoff_stats.errors}"
    )
    return len(orphan_symbols), deleted_blobs


def _process_finance_frame(
    *,
    blob_name: str,
    ticker: str,
    folder_name: str,
    suffix: str,
    silver_path: str,
    df_raw: pd.DataFrame,
    desired_end: pd.Timestamp,
    backfill_start: Optional[pd.Timestamp],
    signature: Optional[dict[str, Optional[str]]],
    persist: bool = True,
    alpha26_bucket_frames: Optional[dict[tuple[str, str], list[pd.DataFrame]]] = None,
) -> BlobProcessResult:
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
        df_clean = assert_no_unexpected_mixed_empty(df_clean, context=f"finance date filter {blob_name}", alias="Date")
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
    existing_bucket = delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, silver_path) if persist else None
    df_history, df_other_symbols = _split_finance_bucket_rows(existing_bucket, ticker=ticker)

    if backfill_start is not None and (df_clean is None or df_clean.empty):
        if not persist:
            return BlobProcessResult(
                blob_name=blob_name,
                silver_path=silver_path,
                ticker=ticker,
                status="ok",
                rows_written=0,
            )
        if silver_client is not None:
            df_remaining = df_other_symbols.copy()
            if not df_remaining.empty:
                df_remaining = normalize_columns_to_snake_case(df_remaining)
                df_remaining = _repair_symbol_column_aliases(df_remaining, ticker=ticker)
                if "symbol" in df_remaining.columns:
                    df_remaining["symbol"] = df_remaining["symbol"].astype("string").str.upper()
            if df_remaining.empty:
                deleted = silver_client.delete_prefix(silver_path)
                mdc.write_line(
                    f"Silver finance backfill purge for {ticker}: no rows >= {backfill_start.date().isoformat()}, "
                    f"deleted {deleted} blob(s) under {silver_path}."
                )
            else:
                delta_core.store_delta(df_remaining.reset_index(drop=True), cfg.AZURE_CONTAINER_SILVER, silver_path, mode="overwrite")
                delta_core.vacuum_delta_table(
                    cfg.AZURE_CONTAINER_SILVER,
                    silver_path,
                    retention_hours=0,
                    dry_run=False,
                    enforce_retention_duration=False,
                    full=True,
                )
                mdc.write_line(f"Silver finance backfill purge for {ticker}: removed symbol rows from {silver_path}.")
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

    df_clean = resample_daily_ffill(df_clean, extend_to=desired_end)
    if df_clean is None or df_clean.empty:
        return BlobProcessResult(
            blob_name=blob_name,
            silver_path=silver_path,
            ticker=ticker,
            status="failed",
            error="No valid dated rows after cleaning/resample.",
        )

    df_clean = _align_to_existing_schema(df_clean, cfg.AZURE_CONTAINER_SILVER, silver_path)
    if not df_history.empty:
        df_history = _align_to_existing_schema(df_history, cfg.AZURE_CONTAINER_SILVER, silver_path)
        df_clean = pd.concat([df_history, df_clean], ignore_index=True)
    df_clean = normalize_columns_to_snake_case(df_clean)
    df_clean = _repair_symbol_column_aliases(df_clean, ticker=ticker)
    if "date" in df_clean.columns:
        df_clean["date"] = pd.to_datetime(df_clean["date"], errors="coerce")
    if "symbol" in df_clean.columns:
        df_clean["symbol"] = df_clean["symbol"].astype("string").str.upper()
        df_clean = df_clean.sort_values(["symbol", "date"]).drop_duplicates(subset=["symbol", "date"], keep="last")
        df_clean = df_clean.reset_index(drop=True)
    df_clean = apply_precision_policy(
        df_clean,
        price_columns=set(),
        calculated_columns=set(),
        price_scale=2,
        calculated_scale=4,
    )
    if not persist:
        if alpha26_bucket_frames is None:
            raise ValueError("alpha26_bucket_frames must be provided when persist=False.")
        sub_domain = _finance_sub_domain(folder_name)
        bucket = layer_bucketing.bucket_letter(ticker)
        alpha26_bucket_frames.setdefault((sub_domain, bucket), []).append(df_clean.copy())
    else:
        df_other_symbols = normalize_columns_to_snake_case(df_other_symbols)
        df_other_symbols = _repair_symbol_column_aliases(df_other_symbols, ticker=ticker)
        if "date" in df_other_symbols.columns:
            df_other_symbols["date"] = pd.to_datetime(df_other_symbols["date"], errors="coerce")
        if "symbol" in df_other_symbols.columns:
            df_other_symbols["symbol"] = df_other_symbols["symbol"].astype("string").str.upper()
        df_bucket_to_store = pd.concat([df_other_symbols, df_clean], ignore_index=True)
        if "symbol" in df_bucket_to_store.columns and "date" in df_bucket_to_store.columns:
            df_bucket_to_store = df_bucket_to_store.sort_values(["symbol", "date"]).drop_duplicates(
                subset=["symbol", "date"],
                keep="last",
            )
        df_bucket_to_store = df_bucket_to_store.reset_index(drop=True)
        delta_core.store_delta(
            df_bucket_to_store,
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
    mdc.write_line(
        "precision_policy_applied domain=finance "
        f"ticker={ticker} report_suffix={suffix} price_cols=none calc_cols=none rows={len(df_clean)}"
    )
    if persist:
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


def process_alpha26_bucket_blob(
    blob: dict,
    *,
    desired_end: pd.Timestamp,
    backfill_start: Optional[pd.Timestamp],
    watermarks: dict,
    persist: bool = True,
    alpha26_bucket_frames: Optional[dict[tuple[str, str], list[pd.DataFrame]]] = None,
) -> list[BlobProcessResult]:
    blob_name = str(blob.get("name", ""))
    signature = build_blob_signature(blob)

    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_bucket = pd.read_parquet(BytesIO(raw_bytes))
    except Exception as exc:
        return [
            BlobProcessResult(
                blob_name=blob_name,
                silver_path=None,
                ticker=None,
                status="failed",
                error=f"Failed to read alpha26 bucket {blob_name}: {exc}",
            )
        ]

    if df_bucket is None or df_bucket.empty:
        if signature:
            signature["updated_at"] = datetime.now(timezone.utc).isoformat()
            watermarks[blob_name] = signature
        return [BlobProcessResult(blob_name=blob_name, silver_path=None, ticker=None, status="skipped")]

    debug_symbols = set(getattr(cfg, "DEBUG_SYMBOLS", []) or [])
    results: list[BlobProcessResult] = []
    for _, row in df_bucket.iterrows():
        ticker = str(row.get("symbol") or "").strip().upper()
        report_type = str(row.get("report_type") or "").strip().lower()
        if not ticker or not report_type:
            continue
        if debug_symbols and ticker not in debug_symbols:
            continue
        mapped = _ALPHA26_REPORT_TYPE_TO_TABLE.get(report_type)
        if not mapped:
            if report_type in SKIPPED_PIOTROSKI_ALPHA26_REPORT_TYPES:
                continue
            results.append(
                BlobProcessResult(
                    blob_name=blob_name,
                    silver_path=None,
                    ticker=ticker,
                    status="failed",
                    error=f"Unsupported alpha26 report_type={report_type}",
                )
            )
            continue
        folder_name, suffix = mapped
        sub_domain = _finance_sub_domain(folder_name)
        bucket = layer_bucketing.bucket_letter(ticker)
        silver_path = DataPaths.get_silver_finance_bucket_path(sub_domain, bucket)
        payload_raw = row.get("payload_json")
        try:
            payload = json.loads(str(payload_raw))
        except Exception as exc:
            results.append(
                BlobProcessResult(
                    blob_name=blob_name,
                    silver_path=silver_path,
                    ticker=ticker,
                    status="failed",
                    error=f"Invalid payload_json for {ticker}/{report_type}: {exc}",
                )
            )
            continue
        try:
            raw_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            df_raw = _read_finance_json(raw_json, ticker=ticker, suffix=suffix)
            result = _process_finance_frame(
                blob_name=blob_name,
                ticker=ticker,
                folder_name=folder_name,
                suffix=suffix,
                silver_path=silver_path,
                df_raw=df_raw,
                desired_end=desired_end,
                backfill_start=backfill_start,
                signature=None,
                persist=persist,
                alpha26_bucket_frames=alpha26_bucket_frames,
            )
            results.append(result)
        except Exception as exc:
            results.append(
                BlobProcessResult(
                    blob_name=blob_name,
                    silver_path=silver_path,
                    ticker=ticker,
                    status="failed",
                    error=f"Failed alpha26 process for {ticker}/{report_type}: {exc}",
                )
            )

    if all(result.status != "failed" for result in results) and signature:
        signature["updated_at"] = datetime.now(timezone.utc).isoformat()
        watermarks[blob_name] = signature
    return results or [BlobProcessResult(blob_name=blob_name, silver_path=None, ticker=None, status="skipped")]


def _process_alpha26_candidate_blobs(
    *,
    candidate_blobs: list[dict],
    desired_end: pd.Timestamp,
    backfill_start: Optional[pd.Timestamp],
    watermarks: dict,
    persist: bool = True,
    alpha26_bucket_frames: Optional[dict[tuple[str, str], list[pd.DataFrame]]] = None,
) -> tuple[list[BlobProcessResult], float]:
    ingest_started = time.perf_counter()
    results: list[BlobProcessResult] = []
    call_kwargs = {
        "desired_end": desired_end,
        "backfill_start": backfill_start,
        "watermarks": watermarks,
    }
    if (not persist) or (alpha26_bucket_frames is not None):
        call_kwargs["persist"] = persist
        call_kwargs["alpha26_bucket_frames"] = alpha26_bucket_frames
    for blob in candidate_blobs:
        blob_results = process_alpha26_bucket_blob(
            blob,
            **call_kwargs,
        )
        results.extend(blob_results)
        # Watermarks are updated per-bucket internally on all-success.
        for result in blob_results:
            if result.status == "ok" and result.watermark_signature:
                watermarks[result.blob_name] = result.watermark_signature
    ingest_elapsed = time.perf_counter() - ingest_started
    return results, ingest_elapsed


def main() -> int:
    mdc.log_environment_diagnostics()
    run_started_at = datetime.now(timezone.utc)
    watermarks = load_watermarks("bronze_finance_data")
    last_success = load_last_success("silver_finance_data")
    watermarks_dirty = False
    bronze_bucketing.bronze_layout_mode()
    layer_bucketing.silver_layout_mode()
    force_rebuild = layer_bucketing.silver_alpha26_force_rebuild()

    desired_end = _utc_today()
    backfill_start, _ = get_backfill_range()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to silver finance data: {backfill_start.date().isoformat()}")
    mdc.write_line("Listing Bronze Finance files...")

    selection = _select_initial_alpha26_source()
    initial_source = selection.source
    initial_blobs = list(selection.blobs)
    deduped_total = int(selection.deduped)
    manifest_run_id = selection.manifest_run_id
    manifest_path = selection.manifest_path
    manifest_blob_count = int(selection.manifest_blob_count)
    manifest_filtered_bucket_blob_count = int(selection.manifest_filtered_bucket_blob_count)

    max_passes = _get_catchup_max_passes()
    pass_count = 0
    seen_blob_names: set[str] = set()
    newly_discovered_blob_names: set[str] = set()
    checkpoint_skipped_first_pass = 0
    checkpoint_skipped_total = 0
    all_results: list[BlobProcessResult] = []
    total_ingest_elapsed = 0.0
    alpha26_bucket_frames: dict[tuple[str, str], list[pd.DataFrame]] = {}

    while pass_count < max_passes:
        pass_count += 1
        if pass_count == 1:
            blobs = list(initial_blobs)
        else:
            blobs, deduped = _list_alpha26_finance_bucket_candidates()
            deduped_total += deduped

        current_blob_names = {str(item.get("name", "")).strip() for item in blobs if item.get("name")}
        if pass_count > 1:
            newly_seen = current_blob_names - seen_blob_names
            if newly_seen:
                newly_discovered_blob_names.update(newly_seen)
                mdc.write_line(
                    f"Silver finance catch-up pass discovered {len(newly_seen)} newly listed Bronze blob(s)."
                )
        seen_blob_names.update(current_blob_names)

        candidate_blobs, checkpoint_skipped = _build_alpha26_checkpoint_candidates(
            blobs=blobs,
            watermarks=watermarks,
            last_success=last_success,
            force_reprocess=force_rebuild,
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
                source=initial_source if pass_count == 1 else "alpha26-bucket-listing",
                total=len(blobs),
                candidates=len(candidate_blobs),
                skipped=checkpoint_skipped,
            )
        )
        if not candidate_blobs:
            break

        pass_results, pass_elapsed = _process_alpha26_candidate_blobs(
            candidate_blobs=candidate_blobs,
            desired_end=desired_end,
            backfill_start=backfill_start,
            watermarks=watermarks,
            persist=False,
            alpha26_bucket_frames=alpha26_bucket_frames,
        )
        watermarks_dirty = True if candidate_blobs else watermarks_dirty
        total_ingest_elapsed += pass_elapsed
        all_results.extend(pass_results)

    lag_candidate_count = 0
    try:
        latest_blobs, deduped = _list_alpha26_finance_bucket_candidates()
        deduped_total += deduped
        lag_candidates, _ = _build_alpha26_checkpoint_candidates(
            blobs=latest_blobs,
            watermarks=watermarks,
            last_success=last_success,
            force_reprocess=force_rebuild,
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
    alpha26_written_symbols = 0
    alpha26_index_path: Optional[str] = None
    alpha26_column_count: Optional[int] = None
    if failed == 0:
        try:
            alpha26_written_symbols, alpha26_index_path, alpha26_column_count = (
                _write_alpha26_finance_silver_buckets(alpha26_bucket_frames)
            )
            mdc.write_line(
                "Silver finance alpha26 buckets written: "
                f"symbols={alpha26_written_symbols} index_path={alpha26_index_path or 'unavailable'}"
            )
        except Exception as exc:
            failed += 1
            mdc.write_error(f"Silver finance alpha26 bucket write failed: {exc}")
    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Silver finance ingest complete: "
        f"attempts={attempts}, ok={processed}, skipped={skipped}, failed={total_failed}, "
        f"skippedCheckpoint={checkpoint_skipped_first_pass}, "
        f"distinctSymbols={distinct_tickers}, rowsWritten={rows_written}, alpha26Symbols={alpha26_written_symbols}, "
        f"elapsedSec={total_ingest_elapsed:.2f}, "
        f"passes={pass_count}, newlyDiscoveredAfterFirstPass={len(newly_discovered_blob_names)}, "
        f"lagCandidates={lag_candidate_count}, source={initial_source}, "
        f"manifestRunId={manifest_run_id or 'n/a'}, "
        f"reconciled_orphans={reconciliation_orphans}, "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs}"
    )
    if watermarks_dirty:
        save_watermarks("bronze_finance_data", watermarks)

    run_ended_at = datetime.now(timezone.utc)
    if total_failed == 0:
        manifest_ack_path: Optional[str] = None
        if initial_source == "bronze-manifest" and manifest_run_id and manifest_path:
            manifest_ack_path = run_manifests.write_silver_finance_ack(
                run_id=manifest_run_id,
                manifest_path=manifest_path,
                status="succeeded",
                metadata={
                    "processed": processed,
                    "skipped": skipped,
                    "failed": total_failed,
                    "attempts": attempts,
                    "rows_written": rows_written,
                    "column_count": alpha26_column_count,
                    "source": initial_source,
                },
            )
            if manifest_ack_path:
                mdc.write_line(
                    "Silver finance manifest acknowledged: "
                    f"runId={manifest_run_id} ackPath={manifest_ack_path}"
                )
            else:
                mdc.write_warning(
                    "Silver finance manifest ack not written: "
                    f"runId={manifest_run_id} manifestPath={manifest_path}"
                )

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
            "alpha26_symbols": alpha26_written_symbols,
            "alpha26_index_path": alpha26_index_path,
            "column_count": alpha26_column_count,
            "elapsed_seconds": round(total_ingest_elapsed, 3),
            "catchup_passes": pass_count,
            "new_blobs_discovered_after_first_pass": len(newly_discovered_blob_names),
            "lag_candidate_count": lag_candidate_count,
            "run_started_at": run_started_at.isoformat(),
            "run_ended_at": run_ended_at.isoformat(),
            "deduped_candidates_total": deduped_total,
            "manifest_run_id": manifest_run_id,
            "manifest_path": manifest_path,
            "manifest_blob_count": manifest_blob_count,
            "manifest_filtered_bucket_blob_count": manifest_filtered_bucket_blob_count,
            "manifest_ack_path": manifest_ack_path,
            "reconciled_orphans": reconciliation_orphans,
            "reconciliation_deleted_blobs": reconciliation_deleted_blobs,
        }
        save_last_success(
            "silver_finance_data",
            when=run_ended_at,
            metadata=checkpoint_metadata,
        )
        return 0
    if initial_source == "bronze-manifest" and manifest_run_id:
        mdc.write_warning(
            "Silver finance manifest remains unacknowledged due to run failures: "
            f"runId={manifest_run_id} failed={total_failed}"
        )
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
