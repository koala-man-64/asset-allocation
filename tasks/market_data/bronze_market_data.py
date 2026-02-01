
import os
import asyncio
import warnings
import pandas as pd
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

from core import playwright_lib as pl
from core import core as mdc
from tasks.market_data import config as cfg
from core.pipeline import ListManager



# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
list_manager = ListManager(bronze_client, "market-data", auto_flush=False)

def _validate_environment():
    if not cfg.AZURE_CONTAINER_BRONZE:
        raise ValueError("Environment variable 'AZURE_CONTAINER_BRONZE' is strictly required.")

async def _download_ticker_csv_bytes(ticker: str, page) -> bytes | None:
    # Default to 10 years or max.
    period1 = int((datetime.now(timezone.utc) - relativedelta(years=10)).timestamp())
    url = (
        "https://query1.finance.yahoo.com/v7/finance/download/"
        f"{ticker.replace('.', '-')}"
        f"?period1={period1}&period2={cfg.YAHOO_MAX_PERIOD}&interval=1d&events=history"
    )

    try:
        download_path = await pl.download_yahoo_price_data_async(page, url)
    except Exception as exc:
        message = str(exc)
        # Only blacklist when we're reasonably sure the ticker has no data.
        if "No data available" in message:
            list_manager.add_to_blacklist(ticker)
        mdc.write_error(f"Error downloading {ticker}: {message}")
        return None

    if not download_path or not os.path.exists(download_path):
        return None

    with open(download_path, "rb") as f:
        raw_bytes = f.read()

    try:
        os.remove(download_path)
    except OSError:
        pass

    return raw_bytes


async def download_and_save_raw(ticker: str, page) -> None:
    """
    Backwards-compatible helper (used by tests) that downloads a single ticker and stores it in Bronze.

    NOTE: The main job orchestration applies additional skip logic and batches list writes via ListManager.flush().
    """
    if list_manager.is_blacklisted(ticker):
        return

    raw_bytes = await _download_ticker_csv_bytes(ticker, page)
    if not raw_bytes:
        return

    await asyncio.to_thread(
        mdc.store_raw_bytes,
        raw_bytes,
        f"market-data/{ticker}.csv",
        client=bronze_client,
    )
    list_manager.add_to_whitelist(ticker)

async def main_async():
    mdc.log_environment_diagnostics()
    _validate_environment()

    async def run_once(_playwright, _browser, context, page, guard: pl.YahooInteractionGuard):
        await pl.authenticate_yahoo_async(page, context)

        mdc.write_line("Fetching symbol universe...")
        df_symbols = mdc.get_symbols()

        # Debug Filter
        if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
            mdc.write_line(f"DEBUG MODE: Restricting to {cfg.DEBUG_SYMBOLS}")
            df_symbols = df_symbols[df_symbols['Symbol'].isin(cfg.DEBUG_SYMBOLS)]

        # Load Lists
        list_manager.load()

        # Filter out NaNs/floats and ensure strings
        # Filter out tickers containing '.' or non-string values
        symbols = []
        for _, row in df_symbols.iterrows():
            sym = row['Symbol']
            if pd.isna(sym) or not isinstance(sym, str):
                continue
            if '.' in sym:
                continue
            symbols.append(sym)

        mdc.write_line(f"Starting Bronze Ingestion for {len(symbols)} symbols...")

        utc_today = datetime.now(timezone.utc).date()
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

        semaphore = asyncio.Semaphore(2)
        progress_lock = asyncio.Lock()
        progress = {"skipped": 0, "downloaded": 0, "failed": 0, "blacklisted": 0, "processed": 0}

        async def process(symbol):
            async with semaphore:
                if guard.triggered:
                    return

                if list_manager.is_blacklisted(symbol):
                    async with progress_lock:
                        progress["blacklisted"] += 1
                        progress["processed"] += 1
                    return

                last_mod = existing_last_modified.get(symbol)
                if last_mod is not None:
                    try:
                        if last_mod.astimezone(timezone.utc).date() >= utc_today:
                            # Already refreshed today; avoid re-downloading (the endpoint is daily bars).
                            list_manager.add_to_whitelist(symbol)
                            async with progress_lock:
                                progress["skipped"] += 1
                                progress["processed"] += 1
                            return
                    except Exception:
                        pass

                try:
                    page = await context.new_page()
                except Exception as exc:
                    mdc.write_error(f"Failed to create page for {symbol}: {exc}")
                    async with progress_lock:
                        progress["failed"] += 1
                        progress["processed"] += 1
                    return
                try:
                    raw_bytes = await _download_ticker_csv_bytes(symbol, page)
                    if raw_bytes:
                        await asyncio.to_thread(
                            mdc.store_raw_bytes,
                            raw_bytes,
                            f"market-data/{symbol}.csv",
                            client=bronze_client,
                        )
                        list_manager.add_to_whitelist(symbol)
                        existing_last_modified[symbol] = datetime.now(timezone.utc)
                        async with progress_lock:
                            progress["downloaded"] += 1
                    else:
                        async with progress_lock:
                            progress["failed"] += 1
                finally:
                    await page.close()
                    async with progress_lock:
                        progress["processed"] += 1
                        if progress["processed"] % 250 == 0:
                            mdc.write_line(
                                "Bronze market progress: processed={processed} downloaded={downloaded} skipped={skipped} "
                                "blacklisted={blacklisted} failed={failed}".format(**progress)
                            )

        tasks = [process(sym) for sym in symbols]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            try:
                list_manager.flush()
            except Exception as exc:
                mdc.write_warning(f"Failed to flush whitelist/blacklist updates: {exc}")

        mdc.write_line(
            "Bronze market ingest complete: processed={processed} downloaded={downloaded} skipped={skipped} "
            "blacklisted={blacklisted} failed={failed}".format(**progress)
        )
        mdc.write_line("Bronze Ingestion Complete.")

    await pl.run_with_yahoo_backoff(run_once)

if __name__ == "__main__":
    from tasks.common.job_trigger import trigger_next_job_from_env

    job_name = 'bronze-market-job'
    with mdc.JobLock("yahoo", wait_timeout_seconds=None):
        with mdc.JobLock(job_name):
            asyncio.run(main_async())
            trigger_next_job_from_env()
