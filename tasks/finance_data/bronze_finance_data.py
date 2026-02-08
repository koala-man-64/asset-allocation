from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import json

from core.massive_gateway_client import (
    MassiveGatewayClient,
    MassiveGatewayError,
    MassiveGatewayNotFoundError,
    MassiveGatewayRateLimitError,
)
from core import core as mdc
from core.pipeline import ListManager
from tasks.finance_data import config as cfg


bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
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
    # Legacy "valuation measures" is not a 1:1 match in Massive.
    # We store a ratios snapshot here for continuity of downstream paths.
    {
        "folder": "Valuation",
        "file_suffix": "quarterly_valuation_measures",
        "report": "overview",
    },
]


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


def _serialize_json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def fetch_and_save_raw(symbol: str, report: dict[str, str], massive_client: MassiveGatewayClient) -> bool:
    """
    Fetch a finance report via the API-hosted Massive gateway and store raw JSON bytes in Bronze.

    Returns True when a write occurred, False when skipped (fresh/no-op).
    """
    if list_manager.is_blacklisted(symbol):
        return False

    folder = report["folder"]
    suffix = report["file_suffix"]
    report_name = report["report"]

    blob_path = f"finance-data/{folder}/{symbol}_{suffix}.json"

    try:
        blob = bronze_client.get_blob_client(blob_path)
        if blob.exists():
            props = blob.get_blob_properties()
            if _is_fresh(
                props.last_modified,
                fresh_days=int(
                    getattr(
                        cfg,
                        "MASSIVE_FINANCE_FRESH_DAYS",
                        getattr(cfg, "ALPHA_VANTAGE_FINANCE_FRESH_DAYS", 28),
                    )
                ),
            ):
                list_manager.add_to_whitelist(symbol)
                return False
    except Exception:
        pass

    payload = massive_client.get_finance_report(symbol=symbol, report=report_name)
    if not isinstance(payload, dict):
        raise MassiveGatewayError(
            "Unexpected Massive finance response type.",
            payload={"symbol": symbol, "report": report_name},
        )

    raw = _serialize_json_bytes(payload)
    mdc.store_raw_bytes(raw, blob_path, client=bronze_client)
    list_manager.add_to_whitelist(symbol)
    return True


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

    mdc.write_line(f"Starting Massive Bronze Finance Ingestion for {len(symbols)} symbols...")

    massive_client = MassiveGatewayClient.from_env()

    max_workers = max(
        1,
        int(
            getattr(
                cfg,
                "MASSIVE_MAX_WORKERS",
                getattr(cfg, "ALPHA_VANTAGE_MAX_WORKERS", 32),
            )
        ),
    )
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="massive-finance")
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)

    progress = {"processed": 0, "written": 0, "skipped": 0, "failed": 0, "blacklisted": 0}
    progress_lock = asyncio.Lock()

    def worker(symbol: str) -> int:
        wrote = 0
        for report in REPORTS:
            if fetch_and_save_raw(symbol, report, massive_client):
                wrote += 1
        return wrote

    async def run_symbol(symbol: str) -> None:
        async with semaphore:
            try:
                wrote = await loop.run_in_executor(executor, worker, symbol)
                async with progress_lock:
                    if wrote:
                        progress["written"] += 1
                    else:
                        progress["skipped"] += 1
            except MassiveGatewayNotFoundError:
                list_manager.add_to_blacklist(symbol)
                async with progress_lock:
                    progress["blacklisted"] += 1
            except MassiveGatewayRateLimitError:
                async with progress_lock:
                    progress["failed"] += 1
            except MassiveGatewayError:
                async with progress_lock:
                    progress["failed"] += 1
            except Exception:
                async with progress_lock:
                    progress["failed"] += 1
            finally:
                async with progress_lock:
                    progress["processed"] += 1
                    if progress["processed"] % 250 == 0:
                        mdc.write_line(
                            "Bronze Massive finance progress: processed={processed} written={written} skipped={skipped} "
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
            massive_client.close()
        except Exception:
            pass
        try:
            list_manager.flush()
        except Exception as exc:
            mdc.write_warning(f"Failed to flush whitelist/blacklist updates: {exc}")

    mdc.write_line(
        "Bronze Massive finance ingest complete: processed={processed} written={written} skipped={skipped} "
        "blacklisted={blacklisted} failed={failed}".format(**progress)
    )
    return 0 if progress["failed"] == 0 else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    from tasks.common.job_trigger import trigger_next_job_from_env

    job_name = "bronze-finance-job"
    with mdc.JobLock(job_name):
        exit_code = main()
        if exit_code == 0:
            trigger_next_job_from_env()
        raise SystemExit(exit_code)
