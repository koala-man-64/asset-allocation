
import os
import asyncio
import pandas as pd
import nasdaqdatalink
from datetime import datetime, date, timedelta, timezone
from io import BytesIO
from typing import Dict, List, Optional

from core import core as mdc
from tasks.price_target_data import config as cfg
from core.pipeline import ListManager
from tasks.common.backfill import get_backfill_range

# Initialize Client
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
list_manager = ListManager(bronze_client, "price-target-data", auto_flush=False)

BATCH_SIZE = 50
PRICE_TARGET_FULL_HISTORY_START_DATE = date(2020, 1, 1)


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _is_test_environment() -> bool:
    return "PYTEST_CURRENT_TEST" in os.environ or _is_truthy(os.environ.get("TEST_MODE"))

def _validate_environment() -> None:
    required = ["AZURE_CONTAINER_BRONZE", "NASDAQ_API_KEY"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Missing env vars: " + " ".join(missing))
    
    nasdaqdatalink.ApiConfig.api_key = os.environ.get('NASDAQ_API_KEY')

def _resolve_price_target_backfill_start() -> Optional[date]:
    backfill_start, _ = get_backfill_range()
    if backfill_start is None:
        return None
    try:
        return backfill_start.to_pydatetime().date()
    except Exception:
        return None


def _load_existing_price_target_df(symbol: str) -> pd.DataFrame:
    blob_path = f"price-target-data/{symbol}.parquet"
    try:
        raw = mdc.read_raw_bytes(blob_path, client=bronze_client)
    except Exception:
        return pd.DataFrame()
    if not raw:
        return pd.DataFrame()
    try:
        return pd.read_parquet(BytesIO(raw))
    except Exception:
        return pd.DataFrame()


def _extract_max_obs_date(df: pd.DataFrame) -> Optional[date]:
    if df.empty or "obs_date" not in df.columns:
        return None
    parsed = pd.to_datetime(df["obs_date"], errors="coerce", utc=True).dropna()
    if parsed.empty:
        return None
    try:
        return parsed.max().date()
    except Exception:
        return None


def _delete_price_target_blob_for_cutoff(
    symbol: str,
    *,
    min_date: date,
    summary: dict,
) -> None:
    blob_path = f"price-target-data/{symbol}.parquet"
    try:
        bronze_client.delete_file(blob_path)
        list_manager.add_to_whitelist(symbol)
        summary["deleted"] += 1
    except Exception as exc:
        mdc.write_error(f"Failed to delete cutoff Bronze {symbol}: {exc}")
        summary["save_failed"] += 1
    finally:
        summary["filtered_missing"] += 1
        mdc.write_line(
            f"No data for {symbol} on/after {min_date.strftime('%Y-%m-%d')}; deleted bronze {blob_path}."
        )


async def process_batch_bronze(
    symbols: List[str],
    semaphore: asyncio.Semaphore,
    *,
    backfill_start: Optional[date] = None,
) -> dict:
    batch_summary = {
        "requested": len(symbols),
        "stale": 0,
        "api_rows": 0,
        "saved": 0,
        "deleted": 0,
        "save_failed": 0,
        "blacklisted": 0,
        "filtered_missing": 0,
        "api_error": False,
    }
    async with semaphore:
        # Determine stale symbols and per-symbol incremental start windows.
        stale_symbols: List[str] = []
        symbol_start_dates: Dict[str, date] = {}
        symbol_has_existing_blob: Dict[str, bool] = {}
        existing_frames: Dict[str, pd.DataFrame] = {}
        default_start_date = backfill_start or PRICE_TARGET_FULL_HISTORY_START_DATE

        for sym in symbols:
            blob_path = f"price-target-data/{sym}.parquet"
            try:
                blob = bronze_client.get_blob_client(blob_path)
                exists = bool(blob.exists())
                symbol_has_existing_blob[sym] = exists
                if exists:
                    props = blob.get_blob_properties()
                    age = datetime.now(timezone.utc) - props.last_modified
                    if age.total_seconds() < 24 * 3600:
                        continue
                    if backfill_start is None:
                        existing_df = _load_existing_price_target_df(sym)
                        existing_frames[sym] = existing_df
                        existing_max = _extract_max_obs_date(existing_df)
                        if existing_max is not None:
                            symbol_start_dates[sym] = existing_max + timedelta(days=1)
            except Exception:
                pass

            if sym not in symbol_start_dates:
                symbol_start_dates[sym] = default_start_date
            stale_symbols.append(sym)

        batch_summary["stale"] = len(stale_symbols)
        if not stale_symbols:
            return batch_summary

        min_date = min(symbol_start_dates.get(sym, default_start_date) for sym in stale_symbols)

        loop = asyncio.get_event_loop()
        api_error_message = ""

        def fetch_api():
            nonlocal api_error_message
            try:
                tickers_str = ",".join(stale_symbols)
                return nasdaqdatalink.get_table(
                    "ZACKS/TP",
                    ticker=tickers_str,
                    obs_date={"gte": min_date.strftime("%Y-%m-%d")},
                )
            except Exception as e:
                api_error_message = str(e)
                mdc.write_error(f"API Batch Error: {e}")
                return pd.DataFrame()

        mdc.write_line(f"Fetching {len(stale_symbols)} symbols from Nasdaq...")
        if _is_test_environment():
            # Avoid threadpool usage in test/sandbox environments.
            batch_df = fetch_api()
        else:
            batch_df = await loop.run_in_executor(None, fetch_api)

        if not batch_df.empty and "obs_date" in batch_df.columns:
            min_ts = pd.Timestamp(min_date)
            parsed_obs_date = pd.to_datetime(batch_df["obs_date"], errors="coerce", utc=True)
            batch_df = batch_df.copy()
            batch_df["obs_date"] = parsed_obs_date.dt.tz_localize(None)
            batch_df = batch_df.loc[batch_df["obs_date"].notna() & (batch_df["obs_date"] >= min_ts)].copy()

        if api_error_message:
            batch_summary["api_error"] = True

        grouped: Dict[str, pd.DataFrame] = {}
        if not batch_df.empty:
            batch_summary["api_rows"] = int(len(batch_df))
            for symbol, group_df in batch_df.groupby("ticker"):
                grouped[str(symbol)] = group_df.copy()
        elif stale_symbols and not api_error_message:
            if backfill_start is None:
                mdc.write_warning(
                    f"Nasdaq batch returned no rows for stale symbols (count={len(stale_symbols)})."
                )

        for sym in stale_symbols:
            symbol_min = symbol_start_dates.get(sym, default_start_date)
            symbol_df = grouped.get(sym, pd.DataFrame()).copy()
            if not symbol_df.empty and "obs_date" in symbol_df.columns:
                symbol_df = symbol_df.loc[symbol_df["obs_date"] >= pd.Timestamp(symbol_min)].copy()

            if symbol_df.empty:
                if backfill_start is not None:
                    _delete_price_target_blob_for_cutoff(sym, min_date=symbol_min, summary=batch_summary)
                    continue
                batch_summary["filtered_missing"] += 1
                # Incremental no-op: no rows newer than per-symbol watermark.
                if bool(symbol_has_existing_blob.get(sym)) or symbol_min > PRICE_TARGET_FULL_HISTORY_START_DATE:
                    list_manager.add_to_whitelist(sym)
                    continue
                mdc.write_line(f"No data for {sym}, blacklisting.")
                list_manager.add_to_blacklist(sym)
                batch_summary["blacklisted"] += 1
                continue

            try:
                if backfill_start is None:
                    existing_df = existing_frames.get(sym)
                    if existing_df is None:
                        existing_df = _load_existing_price_target_df(sym)
                    if existing_df is not None and not existing_df.empty:
                        symbol_df = pd.concat([existing_df, symbol_df], ignore_index=True, sort=False)
                        symbol_df = symbol_df.drop_duplicates().reset_index(drop=True)
                if "obs_date" in symbol_df.columns:
                    symbol_df = symbol_df.sort_values("obs_date").reset_index(drop=True)

                raw_parquet = symbol_df.to_parquet(index=False)
                mdc.store_raw_bytes(raw_parquet, f"price-target-data/{sym}.parquet", client=bronze_client)
                mdc.write_line(f"Saved Bronze {sym}")
                list_manager.add_to_whitelist(sym)
                batch_summary["saved"] += 1
            except Exception as e:
                mdc.write_error(f"Failed to save {sym}: {e}")
                batch_summary["save_failed"] += 1

        mdc.write_line(
            "Bronze price target batch summary: requested={requested} stale={stale} api_rows={api_rows} "
            "saved={saved} deleted={deleted} save_failed={save_failed} blacklisted={blacklisted} filtered_missing={filtered_missing} "
            "api_error={api_error}".format(**batch_summary)
        )
        return batch_summary

async def main_async() -> int:
    mdc.log_environment_diagnostics()
    _validate_environment()
    
    list_manager.load()
    backfill_start = _resolve_price_target_backfill_start()
    if backfill_start is not None:
        mdc.write_line(f"Applying BACKFILL_START_DATE cutoff to bronze price-target data: {backfill_start.isoformat()}")
    
    df_symbols = mdc.get_symbols()
    # Filter NaNs and ensure string
    df_symbols = df_symbols.dropna(subset=['Symbol'])
    # Filter out tickers containing '.' or non-string values
    symbols = []
    for _, row in df_symbols.iterrows():
        sym = row['Symbol']
        if pd.isna(sym) or not isinstance(sym, str):
            continue
        if '.' in sym:
            continue
        if list_manager.is_blacklisted(sym):
            continue
        symbols.append(sym)

    if cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
        symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]
        
    chunked_symbols = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    semaphore = asyncio.Semaphore(3)
    
    mdc.write_line(f"Starting Bronze Price Target Ingestion for {len(symbols)} symbols...")
    tasks = [process_batch_bronze(chunk, semaphore, backfill_start=backfill_start) for chunk in chunked_symbols]
    batch_exception_count = 0
    aggregate = {
        "requested": 0,
        "stale": 0,
        "api_rows": 0,
        "saved": 0,
        "deleted": 0,
        "save_failed": 0,
        "blacklisted": 0,
        "filtered_missing": 0,
        "api_error_batches": 0,
    }
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                batch_exception_count += 1
                mdc.write_error(
                    f"Bronze price target batch exception idx={idx}: {type(result).__name__}: {result}"
                )
                continue
            if not isinstance(result, dict):
                continue
            aggregate["requested"] += int(result.get("requested", 0) or 0)
            aggregate["stale"] += int(result.get("stale", 0) or 0)
            aggregate["api_rows"] += int(result.get("api_rows", 0) or 0)
            aggregate["saved"] += int(result.get("saved", 0) or 0)
            aggregate["deleted"] += int(result.get("deleted", 0) or 0)
            aggregate["save_failed"] += int(result.get("save_failed", 0) or 0)
            aggregate["blacklisted"] += int(result.get("blacklisted", 0) or 0)
            aggregate["filtered_missing"] += int(result.get("filtered_missing", 0) or 0)
            if bool(result.get("api_error", False)):
                aggregate["api_error_batches"] += 1
    finally:
        try:
            list_manager.flush()
        except Exception as exc:
            mdc.write_warning(f"Failed to flush whitelist/blacklist updates: {exc}")
        mdc.write_line(
            "Bronze price target overall summary: requested={requested} stale={stale} api_rows={api_rows} "
            "saved={saved} deleted={deleted} save_failed={save_failed} blacklisted={blacklisted} "
            "filtered_missing={filtered_missing} "
            "api_error_batches={api_error_batches} "
            "batch_exceptions={batch_exception_count}".format(
                batch_exception_count=batch_exception_count,
                **aggregate,
            )
        )
        mdc.write_line("Bronze Ingestion Complete.")
    has_failures = (
        batch_exception_count > 0
        or int(aggregate.get("save_failed", 0) or 0) > 0
        or int(aggregate.get("api_error_batches", 0) or 0) > 0
    )
    return 1 if has_failures else 0

if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = 'bronze-price-target-job'
    with mdc.JobLock("nasdaq", wait_timeout_seconds=None):
        with mdc.JobLock(job_name):
            ensure_api_awake_from_env(required=True)
            exit_code = asyncio.run(main_async())
            if exit_code == 0:
                write_system_health_marker(layer="bronze", domain="price-target", job_name=job_name)
                trigger_next_job_from_env()
            raise SystemExit(exit_code)
