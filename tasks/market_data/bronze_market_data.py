from __future__ import annotations

import asyncio
import os
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from io import StringIO

import pandas as pd

from core.massive_gateway_client import (
    MassiveGatewayClient,
    MassiveGatewayError,
    MassiveGatewayNotFoundError,
    MassiveGatewayRateLimitError,
)
from core import core as mdc
from core.pipeline import ListManager
from tasks.common.backfill import get_backfill_range
from tasks.market_data import config as cfg


bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
list_manager = ListManager(bronze_client, "market-data", auto_flush=False)


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


def _utc_today() -> datetime.date:
    return datetime.now(timezone.utc).date()


def _normalize_provider_daily_csv(csv_text: str) -> bytes:
    """
    Normalize provider OHLCV CSV to the canonical Bronze market schema.

    Output columns:
      Date,Open,High,Low,Close,Volume
    """
    df = pd.read_csv(StringIO(csv_text))

    rename_map = {
        "timestamp": "Date",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "Date" not in df.columns:
        raise ValueError("Provider CSV missing required timestamp/Date column.")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).copy()
    if df.empty:
        raise ValueError("Provider CSV contained no valid dated rows.")

    required = ["Open", "High", "Low", "Close"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Provider CSV missing required columns: {missing}")

    if "Volume" not in df.columns:
        df["Volume"] = 0.0

    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]
    df = df.sort_values("Date").reset_index(drop=True)

    # Ensure stable output types for downstream parsers.
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    return df.to_csv(index=False).encode("utf-8")


def _should_skip_symbol(symbol: str, existing_last_modified: dict[str, datetime], *, utc_today: datetime.date) -> bool:
    last_mod = existing_last_modified.get(symbol)
    if last_mod is None:
        return False
    try:
        return last_mod.astimezone(timezone.utc).date() >= utc_today
    except Exception:
        return False


def _prefetch_existing_last_modified() -> dict[str, datetime]:
    existing_last_modified: dict[str, datetime] = {}
    try:
        for blob in bronze_client.list_blob_infos(name_starts_with="market-data/"):
            name = str(blob.get("name") or "")
            if not name.endswith(".csv"):
                continue
            ticker = name.split("/")[-1].replace(".csv", "")
            lm = blob.get("last_modified")
            if isinstance(lm, datetime):
                existing_last_modified[ticker] = lm
    except Exception as exc:
        mdc.write_warning(f"Unable to prefetch existing market-data blobs; proceeding without skip logic. ({exc})")
    return existing_last_modified


def download_and_save_raw(symbol: str, massive_client: MassiveGatewayClient) -> None:
    """
    Backwards-compatible helper (used by tests/local tooling) that fetches a single ticker
    from the API-hosted Massive gateway and stores it in Bronze.
    """
    if list_manager.is_blacklisted(symbol):
        return

    today = _utc_today().isoformat()
    backfill_start, backfill_end = get_backfill_range()
    if backfill_start or backfill_end:
        from_date = backfill_start or "2000-01-01"
        to_date = backfill_end or today
    else:
        from_date = (datetime.now(timezone.utc).date() - timedelta(days=180)).isoformat()
        to_date = today

    raw_text = massive_client.get_daily_time_series_csv(
        symbol=symbol,
        from_date=from_date,
        to_date=to_date,
        adjusted=True,
    )

    try:
        raw_bytes = _normalize_provider_daily_csv(raw_text)
    except Exception as exc:
        snippet = raw_text.strip().replace("\n", " ")
        if len(snippet) > 200:
            snippet = snippet[:200] + "..."
        raise MassiveGatewayError(
            f"Failed to normalize Massive daily CSV for {symbol}: {type(exc).__name__}: {exc}",
            payload={"snippet": snippet},
        ) from exc

    try:
        mdc.store_raw_bytes(raw_bytes, f"market-data/{symbol}.csv", client=bronze_client)
    except Exception as exc:
        raise RuntimeError(f"Failed to store bronze market-data/{symbol}.csv: {type(exc).__name__}: {exc}") from exc
    list_manager.add_to_whitelist(symbol)


def _get_max_workers() -> int:
    return max(
        1,
        int(
            getattr(
                cfg,
                "MASSIVE_MAX_WORKERS",
                getattr(cfg, "ALPHA_VANTAGE_MAX_WORKERS", 32),
            )
        ),
    )


# Backwards-compatible alias used by some tests/local tooling.
_normalize_alpha_vantage_daily_csv = _normalize_provider_daily_csv


async def main_async() -> int:
    mdc.log_environment_diagnostics()
    _validate_environment()

    list_manager.load()

    mdc.write_line("Fetching symbol universe...")
    df_symbols = mdc.get_symbols()

    symbols: list[str] = []
    for raw in df_symbols["Symbol"].dropna().astype(str).tolist():
        if "." in raw:
            continue
        if list_manager.is_blacklisted(raw):
            continue
        symbols.append(raw)
    # Preserve original ordering while de-duping.
    symbols = list(dict.fromkeys(symbols))

    debug_mode = bool(hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS)
    if debug_mode:
        mdc.write_line(f"DEBUG MODE: Restricting to {cfg.DEBUG_SYMBOLS}")
        symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]

    mdc.write_line(f"Starting Massive Bronze Market Ingestion for {len(symbols)} symbols...")

    existing_last_modified = _prefetch_existing_last_modified()
    utc_today = _utc_today()

    massive_client = MassiveGatewayClient.from_env()

    progress = {"processed": 0, "skipped": 0, "downloaded": 0, "failed": 0, "blacklisted": 0}
    progress_lock = asyncio.Lock()

    def worker(symbol: str) -> None:
        if list_manager.is_blacklisted(symbol):
            raise MassiveGatewayNotFoundError("Symbol is blacklisted.")

        if _should_skip_symbol(symbol, existing_last_modified, utc_today=utc_today):
            raise RuntimeError("SKIP_FRESH")

        download_and_save_raw(symbol, massive_client)

    max_workers = _get_max_workers()
    semaphore = asyncio.Semaphore(max_workers)
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="massive-market")

    async def run_symbol(symbol: str) -> None:
        async with semaphore:
            try:
                if debug_mode:
                    mdc.write_line(f"Downloading OHLCV for {symbol}...")
                await loop.run_in_executor(executor, worker, symbol)
                async with progress_lock:
                    progress["downloaded"] += 1
            except RuntimeError as exc:
                if str(exc) == "SKIP_FRESH":
                    list_manager.add_to_whitelist(symbol)
                    async with progress_lock:
                        progress["skipped"] += 1
                else:
                    should_log = debug_mode
                    async with progress_lock:
                        progress["failed"] += 1
                        should_log = should_log or progress["failed"] <= 20
                    if should_log:
                        mdc.write_error(f"Failed to ingest {symbol}: {exc}")
            except MassiveGatewayNotFoundError as exc:
                list_manager.add_to_blacklist(symbol)
                should_log = debug_mode
                async with progress_lock:
                    progress["blacklisted"] += 1
                    should_log = should_log or progress["blacklisted"] <= 20
                if should_log:
                    mdc.write_warning(f"Invalid symbol {symbol}; blacklisting. ({exc})")
            except MassiveGatewayRateLimitError as exc:
                should_log = debug_mode
                async with progress_lock:
                    progress["failed"] += 1
                    should_log = should_log or progress["failed"] <= 20
                if should_log:
                    note = str(exc)
                    if len(note) > 200:
                        note = note[:200] + "..."
                    mdc.write_warning(f"Massive rate-limited while processing {symbol}. ({note})")
            except MassiveGatewayError as exc:
                should_log = debug_mode
                async with progress_lock:
                    progress["failed"] += 1
                    should_log = should_log or progress["failed"] <= 20
                if should_log:
                    details = f"status={getattr(exc, 'status_code', 'unknown')} message={exc}"
                    payload = getattr(exc, "payload", None)
                    if payload:
                        details = f"{details} payload={payload}"
                    mdc.write_error(f"Massive gateway error while processing {symbol}. ({details})")
            except Exception as exc:
                should_log = debug_mode
                async with progress_lock:
                    progress["failed"] += 1
                    should_log = should_log or progress["failed"] <= 20
                if should_log:
                    mdc.write_error(
                        f"Unexpected error processing {symbol}: {type(exc).__name__}: {exc}\n{traceback.format_exc()}"
                    )
            finally:
                async with progress_lock:
                    progress["processed"] += 1
                    if progress["processed"] % 250 == 0:
                        mdc.write_line(
                            "Bronze Massive market progress: processed={processed} downloaded={downloaded} skipped={skipped} "
                            "blacklisted={blacklisted} failed={failed}".format(**progress)
                        )

    try:
        await asyncio.gather(*(run_symbol(s) for s in symbols))
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
        "Bronze Massive market ingest complete: processed={processed} downloaded={downloaded} skipped={skipped} "
        "blacklisted={blacklisted} failed={failed}".format(**progress)
    )
    return 0 if progress["failed"] == 0 else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    from tasks.common.job_trigger import trigger_next_job_from_env

    job_name = "bronze-market-job"
    with mdc.JobLock(job_name):
        exit_code = main()
        if exit_code == 0:
            trigger_next_job_from_env()
        raise SystemExit(exit_code)
