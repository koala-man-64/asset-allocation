from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
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


bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
list_manager = ListManager(bronze_client, cfg.EARNINGS_DATA_PREFIX, auto_flush=False)


def _is_truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _validate_environment() -> None:
    if not cfg.AZURE_CONTAINER_BRONZE:
        raise ValueError("Environment variable 'AZURE_CONTAINER_BRONZE' is strictly required.")
    if not (os.environ.get("ASSET_ALLOCATION_API_BASE_URL") or os.environ.get("ASSET_ALLOCATION_API_URL")):
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


def _parse_earnings_records(symbol: str, payload: dict[str, Any]) -> pd.DataFrame:
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
    df = df.sort_values(["Date"]).drop_duplicates(subset=["Date", "Symbol"], keep="last").reset_index(drop=True)
    return df


def fetch_and_save_raw(symbol: str, av: AlphaVantageGatewayClient) -> bool:
    """
    Fetch earnings for a single symbol via the API-hosted Alpha Vantage gateway and store as Bronze JSON records.

    Returns True when a Bronze write occurred, False when skipped/no-op.
    """
    if list_manager.is_blacklisted(symbol):
        return False

    blob_path = f"{cfg.EARNINGS_DATA_PREFIX}/{symbol}.json"

    # Freshness gate (quarterly data); avoid re-fetching too frequently.
    try:
        blob = bronze_client.get_blob_client(blob_path)
        if blob.exists():
            props = blob.get_blob_properties()
            if _is_fresh(props.last_modified, fresh_days=int(cfg.ALPHA_VANTAGE_EARNINGS_FRESH_DAYS)):
                list_manager.add_to_whitelist(symbol)
                return False
    except Exception:
        pass

    payload = av.get_earnings(symbol=symbol)
    if not isinstance(payload, dict):
        raise AlphaVantageGatewayError("Unexpected Alpha Vantage earnings response type.", payload={"symbol": symbol})

    df = _parse_earnings_records(symbol, payload)
    if df is None or df.empty:
        raise AlphaVantageGatewayInvalidSymbolError("No quarterly earnings records found.")

    raw_json = df.to_json(orient="records").encode("utf-8")
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

    max_workers = max(1, int(cfg.ALPHA_VANTAGE_MAX_WORKERS))
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="alpha-vantage-earnings")
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)

    progress = {"processed": 0, "written": 0, "skipped": 0, "failed": 0, "blacklisted": 0}
    failure_counts: dict[str, int] = {}
    failure_examples: dict[str, str] = {}
    progress_lock = asyncio.Lock()

    def worker(symbol: str) -> bool:
        return fetch_and_save_raw(symbol, av)

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
                wrote = await loop.run_in_executor(executor, worker, symbol)
                async with progress_lock:
                    if wrote:
                        progress["written"] += 1
                    else:
                        progress["skipped"] += 1
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
        "blacklisted={blacklisted} failed={failed}".format(**progress)
    )
    return 0 if progress["failed"] == 0 else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    from tasks.common.job_trigger import trigger_next_job_from_env

    job_name = "bronze-earnings-job"
    with mdc.JobLock(job_name):
        exit_code = main()
        if exit_code == 0:
            trigger_next_job_from_env()
        raise SystemExit(exit_code)
