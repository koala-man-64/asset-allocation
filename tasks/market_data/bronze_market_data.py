from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from io import StringIO

import pandas as pd

from alpha_vantage import (
    AlphaVantageClient,
    AlphaVantageConfig,
    AlphaVantageError,
    AlphaVantageInvalidSymbolError,
    AlphaVantageThrottleError,
)
from core import core as mdc
from core.pipeline import ListManager
from tasks.common.backfill import get_backfill_range
from tasks.market_data import config as cfg


bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
list_manager = ListManager(bronze_client, "market-data", auto_flush=False)


def _validate_environment() -> None:
    if not cfg.AZURE_CONTAINER_BRONZE:
        raise ValueError("Environment variable 'AZURE_CONTAINER_BRONZE' is strictly required.")
    if not getattr(cfg, "ALPHA_VANTAGE_API_KEY", None):
        raise ValueError("Environment variable 'ALPHA_VANTAGE_API_KEY' is strictly required.")


def _utc_today() -> datetime.date:
    return datetime.now(timezone.utc).date()


def _normalize_alpha_vantage_daily_csv(csv_text: str) -> bytes:
    """
    Normalize Alpha Vantage TIME_SERIES_DAILY CSV to the canonical Bronze market schema.

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
        raise ValueError("Alpha Vantage CSV missing required timestamp/Date column.")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).copy()
    if df.empty:
        raise ValueError("Alpha Vantage CSV contained no valid dated rows.")

    required = ["Open", "High", "Low", "Close"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Alpha Vantage CSV missing required columns: {missing}")

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


def download_and_save_raw(symbol: str, av: AlphaVantageClient) -> None:
    """
    Backwards-compatible helper (used by tests/local tooling) that fetches a single ticker
    from Alpha Vantage and stores it in Bronze.
    """
    if list_manager.is_blacklisted(symbol):
        return

    outputsize = "compact"
    backfill_start, backfill_end = get_backfill_range()
    if backfill_start or backfill_end:
        outputsize = "full"

    csv_text = av.get_daily_time_series(symbol, outputsize=outputsize, datatype="csv")  # type: ignore[assignment]
    raw_bytes = _normalize_alpha_vantage_daily_csv(str(csv_text))
    mdc.store_raw_bytes(raw_bytes, f"market-data/{symbol}.csv", client=bronze_client)
    list_manager.add_to_whitelist(symbol)


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

    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG MODE: Restricting to {cfg.DEBUG_SYMBOLS}")
        symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]

    mdc.write_line(f"Starting Alpha Vantage Bronze Ingestion for {len(symbols)} symbols...")

    existing_last_modified = _prefetch_existing_last_modified()
    utc_today = _utc_today()

    av_cfg = AlphaVantageConfig(
        api_key=str(cfg.ALPHA_VANTAGE_API_KEY),
        rate_limit_per_min=int(getattr(cfg, "ALPHA_VANTAGE_RATE_LIMIT_PER_MIN", 300)),
        timeout=float(getattr(cfg, "ALPHA_VANTAGE_TIMEOUT_SECONDS", 15.0)),
        max_retries=5,
        backoff_base_seconds=0.5,
    )
    av = AlphaVantageClient(av_cfg)

    progress = {"processed": 0, "skipped": 0, "downloaded": 0, "failed": 0, "blacklisted": 0}
    progress_lock = asyncio.Lock()

    def worker(symbol: str) -> None:
        if list_manager.is_blacklisted(symbol):
            raise AlphaVantageInvalidSymbolError("Symbol is blacklisted.")

        if _should_skip_symbol(symbol, existing_last_modified, utc_today=utc_today):
            raise RuntimeError("SKIP_FRESH")

        download_and_save_raw(symbol, av)

    max_workers = max(1, int(getattr(cfg, "ALPHA_VANTAGE_MAX_WORKERS", 32)))
    semaphore = asyncio.Semaphore(max_workers)
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="alpha-vantage-market")

    async def run_symbol(symbol: str) -> None:
        async with semaphore:
            try:
                await loop.run_in_executor(executor, worker, symbol)
                async with progress_lock:
                    progress["downloaded"] += 1
            except RuntimeError as exc:
                if str(exc) == "SKIP_FRESH":
                    list_manager.add_to_whitelist(symbol)
                    async with progress_lock:
                        progress["skipped"] += 1
                else:
                    async with progress_lock:
                        progress["failed"] += 1
            except AlphaVantageInvalidSymbolError:
                list_manager.add_to_blacklist(symbol)
                async with progress_lock:
                    progress["blacklisted"] += 1
            except AlphaVantageThrottleError:
                async with progress_lock:
                    progress["failed"] += 1
            except AlphaVantageError:
                async with progress_lock:
                    progress["failed"] += 1
            finally:
                async with progress_lock:
                    progress["processed"] += 1
                    if progress["processed"] % 250 == 0:
                        mdc.write_line(
                            "Bronze AV market progress: processed={processed} downloaded={downloaded} skipped={skipped} "
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

    mdc.write_line(
        "Bronze AV market ingest complete: processed={processed} downloaded={downloaded} skipped={skipped} "
        "blacklisted={blacklisted} failed={failed}".format(**progress)
    )
    return 0 if progress["failed"] == 0 else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    from tasks.common.job_trigger import trigger_next_job_from_env

    job_name = "bronze-market-job"
    with mdc.JobLock("alpha_vantage", wait_timeout_seconds=None):
        with mdc.JobLock(job_name):
            exit_code = main()
            if exit_code == 0:
                trigger_next_job_from_env()
            raise SystemExit(exit_code)
