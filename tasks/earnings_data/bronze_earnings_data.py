from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from typing import Any, Optional

import pandas as pd

from core.alpha_vantage_gateway_client import (
    AlphaVantageGatewayClient,
    AlphaVantageGatewayError,
    AlphaVantageGatewayInvalidSymbolError,
    AlphaVantageGatewayThrottleError,
)
from core import config as cfg
from core import core as mdc
from core.pipeline import ListManager
from tasks.common.bronze_backfill_coverage import (
    extract_min_date_from_dataframe,
    extract_min_date_from_rows,
    load_coverage_marker,
    normalize_date,
    resolve_backfill_start_date,
    should_force_backfill,
    write_coverage_marker,
)
from tasks.common.backfill import filter_by_date


bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
common_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_COMMON)
list_manager = ListManager(bronze_client, cfg.EARNINGS_DATA_PREFIX, auto_flush=False)


EARNINGS_STALE_DAYS = 7
_COVERAGE_DOMAIN = "earnings"
_COVERAGE_PROVIDER = "alpha-vantage"


def _empty_coverage_summary() -> dict[str, int]:
    return {
        "coverage_checked": 0,
        "coverage_forced_refetch": 0,
        "coverage_marked_covered": 0,
        "coverage_marked_limited": 0,
        "coverage_skipped_limited_marker": 0,
    }


def _mark_coverage(
    *,
    symbol: str,
    backfill_start: date,
    status: str,
    earliest_available: Optional[date],
    coverage_summary: dict[str, int],
) -> None:
    try:
        write_coverage_marker(
            common_client=common_client,
            domain=_COVERAGE_DOMAIN,
            symbol=symbol,
            backfill_start=backfill_start,
            coverage_status=status,
            earliest_available=earliest_available,
            provider=_COVERAGE_PROVIDER,
        )
        if status == "covered":
            coverage_summary["coverage_marked_covered"] += 1
        elif status == "limited":
            coverage_summary["coverage_marked_limited"] += 1
    except Exception as exc:
        mdc.write_warning(f"Failed to write earnings coverage marker for {symbol}: {exc}")


def _is_truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _validate_environment() -> None:
    if not cfg.AZURE_CONTAINER_BRONZE:
        raise ValueError("Environment variable 'AZURE_CONTAINER_BRONZE' is strictly required.")
    if not os.environ.get("ASSET_ALLOCATION_API_BASE_URL"):
        raise ValueError("Environment variable 'ASSET_ALLOCATION_API_BASE_URL' is strictly required.")
    if not (
        os.environ.get("ASSET_ALLOCATION_API_KEY")
        or os.environ.get("API_KEY")
        or _is_truthy(os.environ.get("ASSET_ALLOCATION_API_ALLOW_NO_AUTH"))
    ):
        raise ValueError("Environment variable 'ASSET_ALLOCATION_API_KEY' (or API_KEY) is strictly required.")


def _is_fresh(blob_last_modified: Optional[datetime], *, fresh_days: int) -> bool:
    if blob_last_modified is None:
        return False
    try:
        age = datetime.now(timezone.utc) - blob_last_modified
    except Exception:
        return False
    return age <= timedelta(days=max(0, fresh_days))


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "n/a", "na", "-"}:
        return None
    # Alpha Vantage sometimes returns numeric strings.
    try:
        return float(text)
    except Exception:
        return None


def _coerce_surprise_fraction(payload: dict[str, Any]) -> Optional[float]:
    """
    Return Surprise as a fraction (e.g. 0.05 for +5%).

    The prior ingestion stored surprise percentage as a fraction; maintain that
    convention for compatibility with downstream features.
    """
    percent = _coerce_float(payload.get("surprisePercentage"))
    if percent is not None:
        return percent / 100.0
    # Fall back to Alpha Vantage 'surprise' if present (absolute). Do not convert.
    return _coerce_float(payload.get("surprise"))


def _parse_earnings_records(
    symbol: str,
    payload: dict[str, Any],
    *,
    backfill_start: Optional[date] = None,
) -> pd.DataFrame:
    rows = []
    for item in payload.get("quarterlyEarnings") or []:
        if not isinstance(item, dict):
            continue
        date_raw = item.get("fiscalDateEnding") or item.get("reportedDate")
        if not date_raw:
            continue
        rows.append(
            {
                "Date": str(date_raw).strip(),
                "Symbol": symbol,
                "Reported EPS": _coerce_float(item.get("reportedEPS")),
                "EPS Estimate": _coerce_float(item.get("estimatedEPS")),
                "Surprise": _coerce_surprise_fraction(item),
            }
        )

    df = pd.DataFrame(rows, columns=["Date", "Symbol", "Reported EPS", "EPS Estimate", "Surprise"])
    if df.empty:
        return df

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.tz_localize(None)
    df = df.dropna(subset=["Date"]).copy()
    backfill_start_ts = pd.Timestamp(backfill_start) if backfill_start is not None else None
    df = filter_by_date(df, "Date", backfill_start_ts, None)
    df = df.sort_values(["Date"]).drop_duplicates(subset=["Date", "Symbol"], keep="last").reset_index(drop=True)
    return df


def _extract_source_earliest_earnings_date(payload: dict[str, Any]) -> Optional[date]:
    rows = payload.get("quarterlyEarnings")
    if not isinstance(rows, list):
        return None
    return extract_min_date_from_rows(rows, date_keys=("fiscalDateEnding", "reportedDate", "date"))


def _load_existing_earnings_df(blob_path: str) -> pd.DataFrame:
    try:
        raw = mdc.read_raw_bytes(blob_path, client=bronze_client)
    except Exception:
        return pd.DataFrame()
    if not raw:
        return pd.DataFrame()
    try:
        df = pd.read_json(BytesIO(raw), orient="records")
    except Exception:
        return pd.DataFrame()
    if df.empty or "Date" not in df.columns:
        return pd.DataFrame()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.tz_localize(None)
    df = df.dropna(subset=["Date"]).copy()
    return df


def _extract_latest_earnings_date(df: pd.DataFrame) -> Optional[pd.Timestamp]:
    if df.empty or "Date" not in df.columns:
        return None
    parsed = pd.to_datetime(df["Date"], errors="coerce", utc=True).dropna()
    if parsed.empty:
        return None
    return parsed.max()


def fetch_and_save_raw(
    symbol: str,
    av: AlphaVantageGatewayClient,
    *,
    backfill_start: Optional[date] = None,
    coverage_summary: Optional[dict[str, int]] = None,
) -> bool:
    """
    Fetch earnings for a single symbol via the API-hosted Alpha Vantage gateway and store as Bronze JSON records.

    Returns True when a Bronze write occurred, False when skipped/no-op.
    """
    coverage_summary = coverage_summary if coverage_summary is not None else _empty_coverage_summary()
    if list_manager.is_blacklisted(symbol):
        return False

    blob_path = f"{cfg.EARNINGS_DATA_PREFIX}/{symbol}.json"
    blob_exists: Optional[bool] = None
    resolved_backfill_start = normalize_date(backfill_start)
    existing_df = pd.DataFrame()
    existing_min_date: Optional[date] = None
    force_backfill = False

    # Freshness gate (quarterly data); avoid re-fetching too frequently.
    try:
        blob = bronze_client.get_blob_client(blob_path)
        blob_exists = bool(blob.exists())
        if blob_exists:
            props = blob.get_blob_properties()
            if resolved_backfill_start is not None:
                coverage_summary["coverage_checked"] += 1
                existing_df = _load_existing_earnings_df(blob_path)
                existing_min_date = extract_min_date_from_dataframe(existing_df, date_col="Date")
                marker = load_coverage_marker(
                    common_client=common_client,
                    domain=_COVERAGE_DOMAIN,
                    symbol=symbol,
                )
                force_backfill, skipped_limited_marker = should_force_backfill(
                    existing_min_date=existing_min_date,
                    backfill_start=resolved_backfill_start,
                    marker=marker,
                )
                if skipped_limited_marker:
                    coverage_summary["coverage_skipped_limited_marker"] += 1
                if force_backfill:
                    coverage_summary["coverage_forced_refetch"] += 1
                elif existing_min_date is not None and existing_min_date <= resolved_backfill_start:
                    _mark_coverage(
                        symbol=symbol,
                        backfill_start=resolved_backfill_start,
                        status="covered",
                        earliest_available=existing_min_date,
                        coverage_summary=coverage_summary,
                    )
            if _is_fresh(props.last_modified, fresh_days=EARNINGS_STALE_DAYS) and not force_backfill:
                list_manager.add_to_whitelist(symbol)
                return False
    except Exception:
        pass

    payload = av.get_earnings(symbol=symbol)
    if not isinstance(payload, dict):
        raise AlphaVantageGatewayError("Unexpected Alpha Vantage earnings response type.", payload={"symbol": symbol})

    source_records = payload.get("quarterlyEarnings") or []
    has_source_records = any(
        isinstance(item, dict) and (item.get("fiscalDateEnding") or item.get("reportedDate"))
        for item in source_records
    )
    source_earliest = _extract_source_earliest_earnings_date(payload)
    df = _parse_earnings_records(symbol, payload, backfill_start=resolved_backfill_start)
    if df is None or df.empty:
        if not has_source_records:
            raise AlphaVantageGatewayInvalidSymbolError("No quarterly earnings records found.")
        if resolved_backfill_start is not None:
            if force_backfill:
                _mark_coverage(
                    symbol=symbol,
                    backfill_start=resolved_backfill_start,
                    status="limited",
                    earliest_available=source_earliest,
                    coverage_summary=coverage_summary,
                )
            if blob_exists is not False:
                cutoff_iso = pd.Timestamp(resolved_backfill_start).date().isoformat()
                bronze_client.delete_file(blob_path)
                mdc.write_line(
                    f"No earnings rows on/after {cutoff_iso} for {symbol}; "
                    f"deleted bronze {blob_path}."
                )
                list_manager.add_to_whitelist(symbol)
                return True
            list_manager.add_to_whitelist(symbol)
            return False
        raw_json = b"[]"
    else:
        if blob_exists and resolved_backfill_start is None:
            existing_df = _load_existing_earnings_df(blob_path)
            incoming_latest = _extract_latest_earnings_date(df)
            existing_latest = _extract_latest_earnings_date(existing_df)
            if (
                incoming_latest is not None
                and existing_latest is not None
                and incoming_latest <= existing_latest
            ):
                list_manager.add_to_whitelist(symbol)
                return False
        raw_json = df.to_json(orient="records").encode("utf-8")

    if resolved_backfill_start is not None and force_backfill:
        marker_status = (
            "covered"
            if source_earliest is not None and source_earliest <= resolved_backfill_start
            else "limited"
        )
        _mark_coverage(
            symbol=symbol,
            backfill_start=resolved_backfill_start,
            status=marker_status,
            earliest_available=source_earliest,
            coverage_summary=coverage_summary,
        )

    if blob_exists:
        try:
            existing_raw = mdc.read_raw_bytes(blob_path, client=bronze_client)
        except Exception:
            existing_raw = None
        if existing_raw == raw_json:
            list_manager.add_to_whitelist(symbol)
            return False

    mdc.store_raw_bytes(raw_json, blob_path, client=bronze_client)
    list_manager.add_to_whitelist(symbol)
    return True


def _format_failure_reason(exc: BaseException) -> str:
    reason_parts = [f"type={type(exc).__name__}"]
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        reason_parts.append(f"status={status_code}")
    detail = getattr(exc, "detail", None)
    if detail:
        reason_parts.append(f"detail={str(detail)[:220]}")
    else:
        message = str(exc).strip()
        if message:
            reason_parts.append(f"message={message[:220]}")
    payload = getattr(exc, "payload", None)
    if isinstance(payload, dict):
        path = payload.get("path")
        if path:
            reason_parts.append(f"path={path}")
    return " ".join(reason_parts)


async def main_async() -> int:
    mdc.log_environment_diagnostics()
    _validate_environment()

    list_manager.load()

    df_symbols = mdc.get_symbols().dropna(subset=["Symbol"]).copy()
    symbols = []
    for sym in df_symbols["Symbol"].astype(str).tolist():
        if "." in sym:
            continue
        if list_manager.is_blacklisted(sym):
            continue
        symbols.append(sym)
    symbols = list(dict.fromkeys(symbols))

    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
        symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]

    mdc.write_line(f"Starting Alpha Vantage Bronze Earnings Ingestion for {len(symbols)} symbols...")

    av = AlphaVantageGatewayClient.from_env()
    backfill_start = resolve_backfill_start_date()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to bronze earnings data: {backfill_start.isoformat()}")

    max_workers = max(1, int(cfg.ALPHA_VANTAGE_MAX_WORKERS))
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="alpha-vantage-earnings")
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)

    progress = {"processed": 0, "written": 0, "skipped": 0, "failed": 0, "blacklisted": 0}
    coverage_progress = _empty_coverage_summary()
    failure_counts: dict[str, int] = {}
    failure_examples: dict[str, str] = {}
    progress_lock = asyncio.Lock()

    def worker(symbol: str) -> tuple[bool, dict[str, int]]:
        coverage_summary = _empty_coverage_summary()
        wrote = fetch_and_save_raw(
            symbol,
            av,
            backfill_start=backfill_start,
            coverage_summary=coverage_summary,
        )
        return wrote, coverage_summary

    async def record_failure(symbol: str, exc: BaseException) -> None:
        failure_type = type(exc).__name__
        failure_reason = _format_failure_reason(exc)
        async with progress_lock:
            progress["failed"] += 1
            failure_counts[failure_type] = failure_counts.get(failure_type, 0) + 1
            failure_examples.setdefault(failure_type, f"symbol={symbol} {failure_reason}")
            failed_total = progress["failed"]
            type_total = failure_counts[failure_type]

        # Sample detailed failures to avoid log flooding while still exposing root causes.
        if type_total <= 3 or failed_total % 250 == 0:
            mdc.write_warning(
                "Bronze AV earnings failure: symbol={symbol} {reason} total_failed={failed_total} "
                "type_failed={type_total}".format(
                    symbol=symbol,
                    reason=failure_reason,
                    failed_total=failed_total,
                    type_total=type_total,
                )
            )

    async def run_symbol(symbol: str) -> None:
        async with semaphore:
            try:
                wrote, coverage_summary = await loop.run_in_executor(executor, worker, symbol)
                async with progress_lock:
                    if wrote:
                        progress["written"] += 1
                    else:
                        progress["skipped"] += 1
                    for key in coverage_progress:
                        coverage_progress[key] += int(coverage_summary.get(key, 0) or 0)
            except AlphaVantageGatewayInvalidSymbolError:
                list_manager.add_to_blacklist(symbol)
                async with progress_lock:
                    progress["blacklisted"] += 1
            except AlphaVantageGatewayThrottleError as exc:
                await record_failure(symbol, exc)
            except AlphaVantageGatewayError as exc:
                await record_failure(symbol, exc)
            except Exception as exc:
                await record_failure(symbol, exc)
            finally:
                async with progress_lock:
                    progress["processed"] += 1
                    if progress["processed"] % 500 == 0:
                        mdc.write_line(
                            "Bronze AV earnings progress: processed={processed} written={written} skipped={skipped} "
                            "blacklisted={blacklisted} failed={failed}".format(**progress)
                        )

    try:
        await asyncio.gather(*(run_symbol(s) for s in symbols), return_exceptions=True)
    finally:
        try:
            executor.shutdown(wait=True, cancel_futures=False)
        except Exception:
            pass
        try:
            av.close()
        except Exception:
            pass
        try:
            list_manager.flush()
        except Exception as exc:
            mdc.write_warning(f"Failed to flush whitelist/blacklist updates: {exc}")

    if failure_counts:
        ordered = sorted(failure_counts.items(), key=lambda item: item[1], reverse=True)
        summary = ", ".join(f"{name}={count}" for name, count in ordered[:8])
        mdc.write_warning(f"Bronze AV earnings failure summary: {summary}")
        for name, _ in ordered[:3]:
            example = failure_examples.get(name)
            if example:
                mdc.write_warning(f"Bronze AV earnings failure example ({name}): {example}")

    mdc.write_line(
        "Bronze AV earnings ingest complete: processed={processed} written={written} skipped={skipped} "
        "blacklisted={blacklisted} failed={failed} coverage_checked={coverage_checked} "
        "coverage_forced_refetch={coverage_forced_refetch} coverage_marked_covered={coverage_marked_covered} "
        "coverage_marked_limited={coverage_marked_limited} coverage_skipped_limited_marker={coverage_skipped_limited_marker}".format(
            **progress,
            **coverage_progress,
        )
    )
    return 0 if progress["failed"] == 0 else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "bronze-earnings-job"
    with mdc.JobLock(job_name):
        ensure_api_awake_from_env(required=True)
        exit_code = main()
        if exit_code == 0:
            write_system_health_marker(layer="bronze", domain="earnings", job_name=job_name)
            trigger_next_job_from_env()
        raise SystemExit(exit_code)
