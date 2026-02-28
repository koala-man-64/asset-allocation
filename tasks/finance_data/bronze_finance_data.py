from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

from core.alpha_vantage_gateway_client import (
    AlphaVantageGatewayClient,
    AlphaVantageGatewayError,
    AlphaVantageGatewayInvalidSymbolError,
    AlphaVantageGatewayThrottleError,
)
from core import core as mdc
from core.pipeline import ListManager
from tasks.common.bronze_backfill_coverage import (
    extract_min_date_from_payload_sections,
    load_coverage_marker,
    normalize_date,
    resolve_backfill_start_date,
    should_force_backfill,
    write_coverage_marker,
)
from tasks.common.run_manifests import create_bronze_finance_manifest
from tasks.finance_data import config as cfg


bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
common_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_COMMON)
list_manager = ListManager(bronze_client, "finance-data", auto_flush=False)


REPORTS = [
    {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    },
    {
        "folder": "Cash Flow",
        "file_suffix": "quarterly_cash-flow",
        "report": "cash_flow",
    },
    {
        "folder": "Income Statement",
        "file_suffix": "quarterly_financials",
        "report": "income_statement",
    },
    # Legacy "valuation measures" is not a 1:1 match in Alpha Vantage.
    # We store the OVERVIEW snapshot here for continuity of downstream paths.
    {
        "folder": "Valuation",
        "file_suffix": "quarterly_valuation_measures",
        "report": "overview",
    },
]


FINANCE_REPORT_STALE_DAYS = 7
_RECOVERY_MAX_ATTEMPTS = 3
_RECOVERY_SLEEP_SECONDS = 5.0
_DEFAULT_SHARED_FINANCE_LOCK = "finance-pipeline-shared"
_COVERAGE_DOMAIN = "finance"
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
        mdc.write_warning(f"Failed to write finance coverage marker for {symbol}: {exc}")


def _is_truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


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


def _serialize_json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _parse_iso_date(raw: Any) -> Optional[date]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date()
    except Exception:
        return None


def _apply_backfill_start_to_finance_payload(payload: dict[str, Any], *, backfill_start: Optional[date]) -> dict[str, Any]:
    if backfill_start is None:
        return payload

    filtered_payload = dict(payload)
    for reports_key in ("quarterlyReports", "annualReports"):
        reports = filtered_payload.get(reports_key)
        if not isinstance(reports, list):
            continue

        filtered_rows = []
        for row in reports:
            if not isinstance(row, dict):
                filtered_rows.append(row)
                continue
            row_date = _parse_iso_date(row.get("fiscalDateEnding") or row.get("reportedDate") or row.get("date"))
            if row_date is not None and row_date < backfill_start:
                continue
            filtered_rows.append(row)
        filtered_payload[reports_key] = filtered_rows

    return filtered_payload


def _extract_latest_finance_report_date(payload: dict[str, Any]) -> Optional[date]:
    latest: Optional[date] = None
    for reports_key in ("quarterlyReports", "annualReports"):
        reports = payload.get(reports_key)
        if not isinstance(reports, list):
            continue
        for row in reports:
            if not isinstance(row, dict):
                continue
            row_date = _parse_iso_date(row.get("fiscalDateEnding") or row.get("reportedDate") or row.get("date"))
            if row_date is None:
                continue
            if latest is None or row_date > latest:
                latest = row_date
    return latest


def _extract_source_earliest_finance_date(payload: dict[str, Any]) -> Optional[date]:
    return extract_min_date_from_payload_sections(
        payload,
        section_keys=("quarterlyReports", "annualReports"),
        date_keys=("fiscalDateEnding", "reportedDate", "date"),
    )


def _load_existing_finance_payload(blob_path: str) -> Optional[dict[str, Any]]:
    try:
        raw = mdc.read_raw_bytes(blob_path, client=bronze_client)
    except Exception:
        return None
    if not raw:
        return None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except Exception:
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _has_non_empty_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float, bool)):
        return True
    text = str(value).strip()
    if not text:
        return False
    return text.lower() not in {"none", "null", "nan", "n/a", "na", "-", "not available"}


def _is_empty_finance_payload(payload: dict[str, Any], *, report_name: str) -> bool:
    """
    Detect Alpha Vantage finance payloads that are structurally present but contain no usable records.
    """
    if not payload:
        return True

    if report_name in {"balance_sheet", "cash_flow", "income_statement"}:
        reports = payload.get("quarterlyReports")
        if not isinstance(reports, list) or not reports:
            return True
        for row in reports:
            if not isinstance(row, dict):
                continue
            if _has_non_empty_value(row.get("fiscalDateEnding")):
                return False
        return True

    if report_name == "overview":
        for key, value in payload.items():
            if str(key).strip().lower() == "symbol":
                continue
            if _has_non_empty_value(value):
                return False
        return True

    return False


def fetch_and_save_raw(
    symbol: str,
    report: dict[str, str],
    av_client: AlphaVantageGatewayClient,
    *,
    backfill_start: Optional[date] = None,
    coverage_summary: Optional[dict[str, int]] = None,
) -> bool:
    """
    Fetch a finance report via the API-hosted Alpha Vantage gateway and store raw JSON bytes in Bronze.

    Returns True when a write occurred, False when skipped (fresh/no-op).
    """
    coverage_summary = coverage_summary if coverage_summary is not None else _empty_coverage_summary()
    if list_manager.is_blacklisted(symbol):
        return False

    folder = report["folder"]
    suffix = report["file_suffix"]
    report_name = report["report"]

    blob_path = f"finance-data/{folder}/{symbol}_{suffix}.json"
    blob_exists: Optional[bool] = None
    resolved_backfill_start = normalize_date(backfill_start)
    existing_payload: Optional[dict[str, Any]] = None
    existing_min_date: Optional[date] = None
    force_backfill = False

    try:
        blob = bronze_client.get_blob_client(blob_path)
        blob_exists = bool(blob.exists())
        if blob_exists:
            existing_payload = _load_existing_finance_payload(blob_path)
            if resolved_backfill_start is not None:
                coverage_summary["coverage_checked"] += 1
                if isinstance(existing_payload, dict):
                    existing_min_date = _extract_source_earliest_finance_date(existing_payload)
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
            props = blob.get_blob_properties()
            if _is_fresh(
                props.last_modified,
                fresh_days=FINANCE_REPORT_STALE_DAYS,
            ) and not force_backfill:
                list_manager.add_to_whitelist(symbol)
                return False
    except Exception:
        pass

    payload = av_client.get_finance_report(symbol=symbol, report=report_name)
    if not isinstance(payload, dict):
        raise AlphaVantageGatewayError(
            "Unexpected Alpha Vantage finance response type.",
            payload={"symbol": symbol, "report": report_name},
        )
    if _is_empty_finance_payload(payload, report_name=report_name):
        list_manager.add_to_blacklist(symbol)
        raise AlphaVantageGatewayInvalidSymbolError(
            f"Alpha Vantage returned empty finance payload for {symbol} report={report_name}; blacklisting."
        )
    source_earliest = _extract_source_earliest_finance_date(payload)
    payload = _apply_backfill_start_to_finance_payload(payload, backfill_start=resolved_backfill_start)
    if resolved_backfill_start is not None and _is_empty_finance_payload(payload, report_name=report_name):
        if force_backfill:
            _mark_coverage(
                symbol=symbol,
                backfill_start=resolved_backfill_start,
                status="limited",
                earliest_available=source_earliest,
                coverage_summary=coverage_summary,
            )
        if blob_exists is not False:
            bronze_client.delete_file(blob_path)
            mdc.write_line(
                f"No finance rows on/after {resolved_backfill_start.isoformat()} for {symbol} report={report_name}; "
                f"deleted bronze {blob_path}."
            )
            list_manager.add_to_whitelist(symbol)
            return True
        list_manager.add_to_whitelist(symbol)
        return False

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

    if existing_payload is None and blob_exists:
        existing_payload = _load_existing_finance_payload(blob_path)
    if existing_payload is not None:
        if existing_payload == payload:
            list_manager.add_to_whitelist(symbol)
            return False
        if resolved_backfill_start is None:
            incoming_latest = _extract_latest_finance_report_date(payload)
            existing_latest = _extract_latest_finance_report_date(existing_payload)
            if (
                incoming_latest is not None
                and existing_latest is not None
                and incoming_latest <= existing_latest
            ):
                list_manager.add_to_whitelist(symbol)
                return False

    raw = _serialize_json_bytes(payload)
    mdc.store_raw_bytes(raw, blob_path, client=bronze_client)
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


def _safe_close_alpha_vantage_client(client: AlphaVantageGatewayClient | None) -> None:
    if client is None:
        return
    try:
        client.close()
    except Exception:
        pass


class _ThreadLocalAlphaVantageClientManager:
    def __init__(self, factory: Callable[[], AlphaVantageGatewayClient] | None = None) -> None:
        self._factory = factory or AlphaVantageGatewayClient.from_env
        self._lock = threading.Lock()
        self._generation = 0
        self._clients: dict[int, tuple[int, AlphaVantageGatewayClient]] = {}

    def get_client(self) -> AlphaVantageGatewayClient:
        thread_id = threading.get_ident()
        with self._lock:
            current = self._clients.get(thread_id)
            if current and current[0] == self._generation:
                return current[1]
            if current:
                _safe_close_alpha_vantage_client(current[1])
            fresh_client = self._factory()
            self._clients[thread_id] = (self._generation, fresh_client)
            return fresh_client

    def reset_current(self) -> None:
        thread_id = threading.get_ident()
        with self._lock:
            current = self._clients.pop(thread_id, None)
        if current:
            _safe_close_alpha_vantage_client(current[1])

    def close_all(self) -> None:
        with self._lock:
            for _, client in list(self._clients.values()):
                _safe_close_alpha_vantage_client(client)
            self._clients.clear()


def _is_recoverable_alpha_vantage_error(exc: BaseException) -> bool:
    if isinstance(exc, AlphaVantageGatewayInvalidSymbolError):
        return False

    if isinstance(exc, AlphaVantageGatewayThrottleError):
        return True

    if isinstance(exc, AlphaVantageGatewayError):
        status_code = getattr(exc, "status_code", None)
        if status_code in {408, 429, 500, 502, 503, 504}:
            return True

        message = str(exc).strip().lower()
        transient_markers = (
            "timeout",
            "timed out",
            "connection",
            "server disconnected",
            "remoteprotocolerror",
            "readerror",
            "connecterror",
            "gateway unavailable",
        )
        return any(marker in message for marker in transient_markers)

    return False


def _process_symbol_with_recovery(
    symbol: str,
    client_manager: _ThreadLocalAlphaVantageClientManager,
    *,
    backfill_start: Optional[date] = None,
    max_attempts: int = _RECOVERY_MAX_ATTEMPTS,
    sleep_seconds: float = _RECOVERY_SLEEP_SECONDS,
) -> tuple[int, bool, list[tuple[str, BaseException]], dict[str, int]]:
    attempts = max(1, int(max_attempts))
    sleep_seconds = max(0.0, float(sleep_seconds))
    pending_reports = list(REPORTS)
    wrote = 0
    final_failures: list[tuple[str, BaseException]] = []
    coverage_summary = _empty_coverage_summary()

    for attempt in range(1, attempts + 1):
        next_pending: list[dict[str, str]] = []
        transient_failures: list[tuple[str, BaseException]] = []

        for report in pending_reports:
            report_name = str(report.get("report") or "unknown")
            try:
                av_client = client_manager.get_client()
                if fetch_and_save_raw(
                    symbol,
                    report,
                    av_client,
                    backfill_start=backfill_start,
                    coverage_summary=coverage_summary,
                ):
                    wrote += 1
            except AlphaVantageGatewayInvalidSymbolError as exc:
                return wrote, True, [(report_name, exc)], coverage_summary
            except BaseException as exc:
                if _is_recoverable_alpha_vantage_error(exc) and attempt < attempts:
                    next_pending.append(report)
                    transient_failures.append((report_name, exc))
                else:
                    final_failures.append((report_name, exc))

        if not next_pending:
            return wrote, False, final_failures, coverage_summary

        report_labels = ",".join(sorted({name for name, _ in transient_failures})) or "unknown"
        mdc.write_warning(
            f"Transient Alpha Vantage error for {symbol}; attempt {attempt}/{attempts} failed for report(s) "
            f"[{report_labels}]. Sleeping {sleep_seconds:.1f}s and retrying remaining reports."
        )
        client_manager.reset_current()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        pending_reports = next_pending

    return wrote, False, final_failures, coverage_summary


async def main_async() -> int:
    mdc.log_environment_diagnostics()
    _validate_environment()

    list_manager.load()

    mdc.write_line("Fetching symbols...")
    df_symbols = mdc.get_symbols().dropna(subset=["Symbol"]).copy()

    symbols: list[str] = []
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

    mdc.write_line(f"Starting Alpha Vantage Bronze Finance Ingestion for {len(symbols)} symbols...")

    client_manager = _ThreadLocalAlphaVantageClientManager()
    backfill_start = resolve_backfill_start_date()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to bronze finance data: {backfill_start.isoformat()}")

    max_workers = max(
        1,
        int(
            getattr(
                cfg,
                "ALPHA_VANTAGE_MAX_WORKERS",
                32,
            )
        ),
    )
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="alpha-vantage-finance")
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)

    progress = {"processed": 0, "written": 0, "skipped": 0, "failed": 0, "blacklisted": 0}
    coverage_progress = _empty_coverage_summary()
    retry_next_run: set[str] = set()
    failure_counts: dict[str, int] = {}
    failure_examples: dict[str, str] = {}
    progress_lock = asyncio.Lock()

    def worker(symbol: str) -> tuple[int, bool, list[tuple[str, BaseException]], dict[str, int]]:
        return _process_symbol_with_recovery(
            symbol,
            client_manager,
            backfill_start=backfill_start,
        )

    async def record_failures(symbol: str, failures: list[tuple[str, BaseException]]) -> None:
        failure_reasons: list[str] = []
        async with progress_lock:
            progress["failed"] += 1
            retry_next_run.add(symbol)
            for report_name, exc in failures:
                failure_type = type(exc).__name__
                failure_reason = _format_failure_reason(exc)
                failure_reasons.append(f"report={report_name} {failure_reason}")
                failure_counts[failure_type] = failure_counts.get(failure_type, 0) + 1
                failure_examples.setdefault(
                    failure_type,
                    f"symbol={symbol} report={report_name} {failure_reason}",
                )
            failed_total = progress["failed"]

        # Sample detailed failures to avoid log flooding while still exposing root causes.
        if failed_total <= 20 or failed_total % 250 == 0:
            summary = " | ".join(failure_reasons[:4])
            mdc.write_warning(
                "Bronze AV finance symbol failure: symbol={symbol} total_failed={failed_total} details={summary}".format(
                    symbol=symbol,
                    failed_total=failed_total,
                    summary=summary,
                )
            )

    async def run_symbol(symbol: str) -> None:
        async with semaphore:
            try:
                wrote, blacklisted, failures, coverage_summary = await loop.run_in_executor(executor, worker, symbol)
                if blacklisted:
                    list_manager.add_to_blacklist(symbol)
                    async with progress_lock:
                        progress["blacklisted"] += 1
                        for key in coverage_progress:
                            coverage_progress[key] += int(coverage_summary.get(key, 0) or 0)
                elif failures:
                    async with progress_lock:
                        for key in coverage_progress:
                            coverage_progress[key] += int(coverage_summary.get(key, 0) or 0)
                    await record_failures(symbol, failures)
                else:
                    async with progress_lock:
                        if wrote:
                            progress["written"] += 1
                        else:
                            progress["skipped"] += 1
                        for key in coverage_progress:
                            coverage_progress[key] += int(coverage_summary.get(key, 0) or 0)
            except Exception as exc:
                await record_failures(symbol, [("unknown", exc)])
            finally:
                async with progress_lock:
                    progress["processed"] += 1
                    if progress["processed"] % 250 == 0:
                        mdc.write_line(
                            "Bronze AV finance progress: processed={processed} written={written} skipped={skipped} "
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
            client_manager.close_all()
        except Exception:
            pass
        try:
            list_manager.flush()
        except Exception as exc:
            mdc.write_warning(f"Failed to flush whitelist/blacklist updates: {exc}")

    if failure_counts:
        ordered = sorted(failure_counts.items(), key=lambda item: item[1], reverse=True)
        summary = ", ".join(f"{name}={count}" for name, count in ordered[:8])
        mdc.write_warning(f"Bronze AV finance failure summary: {summary}")
        for name, _ in ordered[:3]:
            example = failure_examples.get(name)
            if example:
                mdc.write_warning(f"Bronze AV finance failure example ({name}): {example}")
    if retry_next_run:
        preview = ", ".join(sorted(retry_next_run)[:50])
        suffix = " ..." if len(retry_next_run) > 50 else ""
        mdc.write_line(
            f"Retry-on-next-run candidates (not blacklisted): count={len(retry_next_run)} symbols={preview}{suffix}"
        )

    mdc.write_line(
        "Bronze AV finance ingest complete: processed={processed} written={written} skipped={skipped} "
        "blacklisted={blacklisted} failed={failed} coverage_checked={coverage_checked} "
        "coverage_forced_refetch={coverage_forced_refetch} coverage_marked_covered={coverage_marked_covered} "
        "coverage_marked_limited={coverage_marked_limited} coverage_skipped_limited_marker={coverage_skipped_limited_marker}".format(
            **progress,
            **coverage_progress,
        )
    )
    try:
        listed_blobs = bronze_client.list_blob_infos(name_starts_with="finance-data/")
        manifest = create_bronze_finance_manifest(
            producer_job_name="bronze-finance-job",
            listed_blobs=listed_blobs,
            metadata={
                "jobStatus": "succeeded" if progress["failed"] == 0 else "failed",
                "processed": int(progress["processed"]),
                "written": int(progress["written"]),
                "skipped": int(progress["skipped"]),
                "blacklisted": int(progress["blacklisted"]),
                "failed": int(progress["failed"]),
            },
        )
        if manifest:
            mdc.write_line(
                "Bronze finance manifest published: runId={run_id} blobCount={blob_count} path={path}".format(
                    run_id=manifest.get("runId"),
                    blob_count=manifest.get("blobCount"),
                    path=manifest.get("manifestPath"),
                )
            )
    except Exception as exc:
        mdc.write_warning(f"Failed to publish bronze finance manifest: {exc}")
    return 0 if progress["failed"] == 0 else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "bronze-finance-job"
    shared_lock_name = (os.environ.get("FINANCE_PIPELINE_SHARED_LOCK_NAME") or _DEFAULT_SHARED_FINANCE_LOCK).strip()
    shared_wait_timeout = _parse_wait_timeout_seconds(
        os.environ.get("BRONZE_FINANCE_SHARED_LOCK_WAIT_SECONDS"),
        default=0.0,
    )
    with mdc.JobLock(shared_lock_name, wait_timeout_seconds=shared_wait_timeout):
        with mdc.JobLock(job_name):
            ensure_api_awake_from_env(required=True)
            exit_code = main()
            if exit_code == 0:
                write_system_health_marker(layer="bronze", domain="finance", job_name=job_name)
                trigger_next_job_from_env()
            raise SystemExit(exit_code)
