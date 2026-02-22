from __future__ import annotations

import asyncio
import os
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from io import BytesIO, StringIO
from typing import Any, Callable, Optional

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

_SUPPLEMENTAL_MARKET_COLUMNS = ("ShortInterest", "ShortVolume", "FloatShares")
_RECOVERY_MAX_ATTEMPTS = 3
_RECOVERY_SLEEP_SECONDS = 5.0
_FULL_HISTORY_START_DATE = "1900-01-01"


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


def _normalize_key(name: Any) -> str:
    return "".join(ch for ch in str(name).strip().lower() if ch.isalnum())


def _extract_payload_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        results = payload.get("results")
        if isinstance(results, list):
            return [row for row in results if isinstance(row, dict)]
        if isinstance(results, dict):
            return [results]
        return [payload]
    return []


def _extract_row_date(payload: dict[str, Any]) -> str | None:
    normalized = {_normalize_key(k): v for k, v in payload.items()}
    date_candidates = (
        "date",
        "filingdate",
        "filing_date",
        "settlement_date",
        "settlementdate",
        "effective_date",
        "effectivedate",
        "as_of",
        "asof",
        "session",
        "day",
        "start",
        "start_date",
        "startdate",
        "timestamp",
        "t",
        "time",
        "window_start",
        "windowstart",
        "report_date",
        "reportdate",
        "calendar_date",
        "calendardate",
    )
    for key in date_candidates:
        out = _extract_iso_date(normalized.get(key))
        if out:
            return out
    return None


def _is_within_window(
    date_str: str | None,
    *,
    min_date: str | None = None,
    max_date: str | None = None,
) -> bool:
    parsed = _extract_iso_date(date_str)
    if parsed is None:
        return False
    if min_date:
        window_min = _extract_iso_date(min_date)
        if window_min and parsed < window_min:
            return False
    if max_date:
        window_max = _extract_iso_date(max_date)
        if window_max and parsed > window_max:
            return False
    return True


def _normalize_window_bound(value: str | None) -> str | None:
    if not value:
        return None
    normalized = _extract_iso_date(value)
    return normalized


def _extract_first_numeric(payload: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    normalized = {_normalize_key(k): v for k, v in payload.items()}
    for key in keys:
        raw = normalized.get(_normalize_key(key))
        if raw is None:
            continue
        try:
            return float(raw)
        except Exception:
            continue
    return None


def _extract_iso_date(raw: Any) -> str | None:
    if raw is None:
        return None

    if isinstance(raw, (int, float)):
        try:
            ivalue = int(raw)
            unit = "ms" if abs(ivalue) > 10_000_000_000 else "s"
            parsed = pd.to_datetime(ivalue, unit=unit, errors="coerce", utc=True)
            if pd.isna(parsed):
                return None
            return parsed.date().isoformat()
        except Exception:
            return None

    text = str(raw).strip()
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce", utc=True)
    if pd.isna(parsed):
        parsed = pd.to_datetime(text[:10], errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _build_metric_series(
    payload: Any,
    *,
    metric_column: str,
    value_keys: tuple[str, ...],
    fallback_date: str,
    min_date: str | None = None,
    max_date: str | None = None,
) -> pd.DataFrame:
    rows = _extract_payload_rows(payload)
    out_rows: list[dict[str, Any]] = []

    for row in rows:
        value = _extract_first_numeric(row, value_keys)
        if value is None:
            continue
        date_str = _extract_row_date(row)
        if date_str is None or not _is_within_window(date_str, min_date=min_date, max_date=max_date):
            continue
        out_rows.append({"Date": date_str, metric_column: value})

    if not out_rows and isinstance(payload, dict):
        top_level_value = _extract_first_numeric(payload, value_keys)
        if top_level_value is not None:
            out_rows.append({"Date": fallback_date, metric_column: top_level_value})

    df_metric = pd.DataFrame(out_rows, columns=["Date", metric_column])
    if df_metric.empty:
        return df_metric
    df_metric["Date"] = pd.to_datetime(df_metric["Date"], errors="coerce")
    df_metric = df_metric.dropna(subset=["Date"]).copy()
    if df_metric.empty:
        return pd.DataFrame(columns=["Date", metric_column])
    df_metric["Date"] = df_metric["Date"].dt.strftime("%Y-%m-%d")
    df_metric = df_metric.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
    return df_metric.reset_index(drop=True)


def _normalize_provider_daily_df(csv_text: str) -> pd.DataFrame:
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

    return df


def _merge_market_fundamentals(
    df_daily: pd.DataFrame,
    *,
    short_interest_payload: Any,
    short_volume_payload: Any,
    float_payload: Any,
) -> pd.DataFrame:
    out = df_daily.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    metric_min_date = out["Date"].dropna().min()
    metric_max_date = out["Date"].dropna().max()

    fallback_date = str(metric_max_date) if not out.empty else _utc_today().isoformat()
    normalized_min = _normalize_window_bound(metric_min_date)
    normalized_max = _normalize_window_bound(metric_max_date)
    metric_specs = (
        (
            "ShortInterest",
            short_interest_payload,
            (
                "short_interest",
                "shortinterest",
                "shortinterestshares",
                "short_interest_shares",
                "sharesshort",
                "value",
            ),
        ),
        (
            "ShortVolume",
            short_volume_payload,
            (
                "short_volume",
                "shortvolume",
                "shortvolumeshares",
                "short_volume_shares",
                "volumeshort",
                "value",
            ),
        ),
        (
            "FloatShares",
            float_payload,
            (
                "float_shares",
                "shares_float",
                "sharesfloat",
                "floatshares",
                "floatshares",
                "float",
                "free_float",
                "value",
            ),
        ),
    )

    for column_name, payload, value_keys in metric_specs:
        df_metric = _build_metric_series(
            payload,
            metric_column=column_name,
            value_keys=value_keys,
            fallback_date=fallback_date,
            min_date=normalized_min,
            max_date=normalized_max,
        )
        if df_metric.empty:
            out[column_name] = pd.NA
        else:
            out = out.merge(df_metric, on="Date", how="left")
            out[column_name] = pd.to_numeric(out[column_name], errors="coerce")
        out[column_name] = out[column_name].ffill().bfill()

    for column_name in _SUPPLEMENTAL_MARKET_COLUMNS:
        if column_name not in out.columns:
            out[column_name] = pd.NA
        out[column_name] = pd.to_numeric(out[column_name], errors="coerce")

    return out


def _serialize_market_csv(df_daily: pd.DataFrame) -> bytes:
    out = df_daily.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out = out.dropna(subset=["Date"]).copy()
    out = out.sort_values("Date").reset_index(drop=True)
    out["Date"] = out["Date"].dt.strftime("%Y-%m-%d")
    ordered_columns = ["Date", "Open", "High", "Low", "Close", "Volume", *_SUPPLEMENTAL_MARKET_COLUMNS]
    return out[[c for c in ordered_columns if c in out.columns]].to_csv(index=False).encode("utf-8")


def _normalize_provider_daily_csv(csv_text: str) -> bytes:
    return _serialize_market_csv(_normalize_provider_daily_df(csv_text))


def _is_header_only_provider_daily_csv(csv_text: str) -> bool:
    """
    Detect CSV payloads that contain only the header row and no usable data rows.

    Example payload:
      Date,Open,High,Low,Close,Volume
    """
    raw = str(csv_text or "")
    if not raw.strip():
        return False

    lines = [line.strip() for line in raw.replace("\r\n", "\n").split("\n")]
    while lines and not lines[-1]:
        lines.pop()
    if not lines:
        return False

    header = lines[0].strip().strip("'").strip('"').lower()
    valid_headers = {
        "date,open,high,low,close,volume",
        "timestamp,open,high,low,close,volume",
    }
    if header not in valid_headers:
        return False

    # Any non-empty/meaningful line after the header counts as data.
    for line in lines[1:]:
        cleaned = line.strip().strip("'").strip('"').strip(",").strip()
        if cleaned:
            return False

    return True


def _load_existing_market_df(symbol: str) -> pd.DataFrame:
    path = f"market-data/{symbol}.csv"
    try:
        raw_bytes = mdc.read_raw_bytes(path, client=bronze_client)
    except Exception as exc:
        mdc.write_warning(
            f"Unable to read existing bronze market blob for {symbol}; continuing with provider-only payload. ({exc})"
        )
        return pd.DataFrame()

    if not raw_bytes:
        return pd.DataFrame()

    try:
        existing_df = pd.read_csv(BytesIO(raw_bytes))
    except Exception as exc:
        mdc.write_warning(f"Existing bronze market CSV for {symbol} is unreadable; rebuilding from provider data. ({exc})")
        return pd.DataFrame()

    if existing_df.empty or "Date" not in existing_df.columns:
        return pd.DataFrame()

    existing_df["Date"] = pd.to_datetime(existing_df["Date"], errors="coerce")
    existing_df = existing_df.dropna(subset=["Date"]).copy()
    if existing_df.empty:
        return pd.DataFrame()

    existing_df["Date"] = existing_df["Date"].dt.strftime("%Y-%m-%d")
    return existing_df


def _extract_latest_market_date(existing_df: pd.DataFrame) -> date | None:
    if existing_df.empty or "Date" not in existing_df.columns:
        return None
    parsed = pd.to_datetime(existing_df["Date"], errors="coerce").dropna()
    if parsed.empty:
        return None
    try:
        return parsed.max().date()
    except Exception:
        return None


def _resolve_backfill_range_dates() -> tuple[Optional[date], Optional[date]]:
    """Resolve shared backfill date window from BACKFILL_START_DATE / BACKFILL_END_DATE."""
    backfill_start, backfill_end = get_backfill_range()
    return (
        backfill_start.to_pydatetime().date() if backfill_start is not None else None,
        backfill_end.to_pydatetime().date() if backfill_end is not None else None,
    )


def _resolve_fetch_window(*, existing_latest_date: date | None) -> tuple[str, str]:
    today = _utc_today()
    backfill_start, backfill_end = _resolve_backfill_range_dates()
    if backfill_start or backfill_end:
        from_date = backfill_start.isoformat() if isinstance(backfill_start, date) else _FULL_HISTORY_START_DATE
        to_date = backfill_end.isoformat() if isinstance(backfill_end, date) else today.isoformat()
        return from_date, to_date

    if existing_latest_date is None:
        from_date = _FULL_HISTORY_START_DATE
    else:
        from_date = min(existing_latest_date, today).isoformat()
    return from_date, today.isoformat()


def _merge_existing_and_new_market_data(existing_df: pd.DataFrame, incoming_df: pd.DataFrame) -> pd.DataFrame:
    if existing_df.empty:
        return incoming_df

    merged = pd.concat([existing_df, incoming_df], ignore_index=True, sort=False)
    merged["Date"] = pd.to_datetime(merged["Date"], errors="coerce")
    merged = merged.dropna(subset=["Date"]).copy()
    if merged.empty:
        return incoming_df

    numeric_columns = ("Open", "High", "Low", "Close", "Volume", *_SUPPLEMENTAL_MARKET_COLUMNS)
    for column_name in numeric_columns:
        if column_name not in merged.columns:
            merged[column_name] = pd.NA
        merged[column_name] = pd.to_numeric(merged[column_name], errors="coerce")

    merged = merged.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)
    merged["Date"] = merged["Date"].dt.strftime("%Y-%m-%d")
    return merged


def _drop_rows_before_backfill_start(df: pd.DataFrame, *, backfill_start: date | None) -> pd.DataFrame:
    if backfill_start is None or df.empty or "Date" not in df.columns:
        return df

    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out = out.loc[out["Date"].notna() & (out["Date"] >= pd.Timestamp(backfill_start))].copy()
    if out.empty:
        return pd.DataFrame(columns=df.columns)
    out["Date"] = out["Date"].dt.strftime("%Y-%m-%d")
    return out.reset_index(drop=True)


def _safe_close_massive_client(client: MassiveGatewayClient | None) -> None:
    if client is None:
        return
    try:
        client.close()
    except Exception:
        pass


class _ThreadLocalMassiveClientManager:
    def __init__(self, factory: Callable[[], MassiveGatewayClient] | None = None) -> None:
        self._factory = factory or MassiveGatewayClient.from_env
        self._lock = threading.Lock()
        self._generation = 0
        self._clients: dict[int, tuple[int, MassiveGatewayClient]] = {}

    def get_client(self) -> MassiveGatewayClient:
        thread_id = threading.get_ident()
        with self._lock:
            current = self._clients.get(thread_id)
            if current and current[0] == self._generation:
                return current[1]
            if current:
                _safe_close_massive_client(current[1])
            fresh_client = self._factory()
            self._clients[thread_id] = (self._generation, fresh_client)
            return fresh_client

    def reset_all(self) -> None:
        with self._lock:
            for _, client in list(self._clients.values()):
                _safe_close_massive_client(client)
            self._clients.clear()
            self._generation += 1

    def close_all(self) -> None:
        with self._lock:
            for _, client in list(self._clients.values()):
                _safe_close_massive_client(client)
            self._clients.clear()


def _is_recoverable_massive_error(exc: Exception) -> bool:
    if isinstance(exc, MassiveGatewayNotFoundError):
        return False

    if isinstance(exc, MassiveGatewayRateLimitError):
        return True

    if isinstance(exc, MassiveGatewayError):
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


def _download_and_save_raw_with_recovery(
    symbol: str,
    client_manager: _ThreadLocalMassiveClientManager,
    *,
    max_attempts: int = _RECOVERY_MAX_ATTEMPTS,
    sleep_seconds: float = _RECOVERY_SLEEP_SECONDS,
) -> None:
    attempts = max(1, int(max_attempts))
    sleep_seconds = max(0.0, float(sleep_seconds))

    for attempt in range(1, attempts + 1):
        client = client_manager.get_client()
        try:
            download_and_save_raw(symbol, client)
            return
        except MassiveGatewayNotFoundError:
            raise
        except Exception as exc:
            should_retry = _is_recoverable_massive_error(exc) and attempt < attempts
            if not should_retry:
                raise

            mdc.write_warning(
                f"Transient Massive error for {symbol}; attempt {attempt}/{attempts} failed ({exc}). "
                f"Sleeping {sleep_seconds:.1f}s, resetting clients, and retrying."
            )
            time.sleep(sleep_seconds)
            client_manager.reset_all()


def download_and_save_raw(symbol: str, massive_client: MassiveGatewayClient) -> None:
    """
    Backwards-compatible helper (used by tests/local tooling) that fetches a single ticker
    from the API-hosted Massive gateway and stores it in Bronze.
    """
    if list_manager.is_blacklisted(symbol):
        return

    existing_df = _load_existing_market_df(symbol)
    existing_latest_date = _extract_latest_market_date(existing_df)
    from_date, to_date = _resolve_fetch_window(existing_latest_date=existing_latest_date)
    backfill_start, _ = _resolve_backfill_range_dates()
    raw_text = massive_client.get_daily_time_series_csv(
        symbol=symbol,
        from_date=from_date,
        to_date=to_date,
        adjusted=True,
    )

    if _is_header_only_provider_daily_csv(raw_text):
        if not existing_df.empty:
            mdc.write_warning(
                f"Massive returned header-only daily CSV for {symbol} in range {from_date}..{to_date}; "
                "keeping existing bronze data."
            )
            list_manager.add_to_whitelist(symbol)
            return
        list_manager.add_to_blacklist(symbol)
        raise MassiveGatewayNotFoundError(f"Massive returned header-only daily CSV for {symbol}; blacklisting.")

    blob_path = f"market-data/{symbol}.csv"

    try:
        df_daily = _normalize_provider_daily_df(raw_text)

        try:
            short_interest_payload = massive_client.get_short_interest(
                symbol=symbol,
                settlement_date_gte=from_date,
                settlement_date_lte=to_date,
            )
        except MassiveGatewayNotFoundError:
            short_interest_payload = {}

        try:
            short_volume_payload = massive_client.get_short_volume(
                symbol=symbol,
                date_gte=from_date,
                date_lte=to_date,
            )
        except MassiveGatewayNotFoundError:
            short_volume_payload = {}

        try:
            float_payload = massive_client.get_float(symbol=symbol)
        except MassiveGatewayNotFoundError:
            float_payload = {}

        df_daily = _merge_market_fundamentals(
            df_daily,
            short_interest_payload=short_interest_payload,
            short_volume_payload=short_volume_payload,
            float_payload=float_payload,
        )
        df_daily = _merge_existing_and_new_market_data(existing_df, df_daily)
        df_daily = _drop_rows_before_backfill_start(df_daily, backfill_start=backfill_start)
        if backfill_start is not None and df_daily.empty:
            bronze_client.delete_file(blob_path)
            mdc.write_line(
                f"No market rows on/after {backfill_start.isoformat()} for {symbol}; deleted bronze {blob_path}."
            )
            list_manager.add_to_whitelist(symbol)
            return
        raw_bytes = _serialize_market_csv(df_daily)
    except (MassiveGatewayRateLimitError, MassiveGatewayError):
        raise
    except Exception as exc:
        snippet = raw_text.strip().replace("\n", " ")
        if len(snippet) > 200:
            snippet = snippet[:200] + "..."
        raise MassiveGatewayError(
            f"Failed to build Massive daily+fundamentals CSV for {symbol}: {type(exc).__name__}: {exc}",
            payload={"snippet": snippet},
        ) from exc

    try:
        mdc.store_raw_bytes(raw_bytes, blob_path, client=bronze_client)
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
    backfill_start, _ = _resolve_backfill_range_dates()
    if backfill_start is not None:
        mdc.write_line(f"Applying BACKFILL_START_DATE cutoff to bronze market data: {backfill_start.isoformat()}")

    list_manager.load()
    mdc.write_line(
        f"Bronze market blacklist loaded with {len(list_manager.blacklist)} symbols (excluded from scheduling)."
    )

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

    client_manager = _ThreadLocalMassiveClientManager()

    progress = {"processed": 0, "downloaded": 0, "failed": 0, "blacklisted": 0}
    retry_next_run: set[str] = set()
    progress_lock = asyncio.Lock()

    def worker(symbol: str) -> None:
        if list_manager.is_blacklisted(symbol):
            raise MassiveGatewayNotFoundError("Symbol is blacklisted.")

        _download_and_save_raw_with_recovery(symbol, client_manager)

    max_workers = _get_max_workers()
    semaphore = asyncio.Semaphore(max_workers)
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="massive-market")

    async def run_symbol(symbol: str) -> None:
        async with semaphore:
            try:
                if debug_mode:
                    mdc.write_line(f"Downloading OHLCV+fundamentals for {symbol}...")
                await loop.run_in_executor(executor, worker, symbol)
                async with progress_lock:
                    progress["downloaded"] += 1
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
                    retry_next_run.add(symbol)
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
                    retry_next_run.add(symbol)
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
                    retry_next_run.add(symbol)
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
                            "Bronze Massive market progress: processed={processed} downloaded={downloaded} "
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
            client_manager.close_all()
        except Exception:
            pass
        try:
            list_manager.flush()
        except Exception as exc:
            mdc.write_warning(f"Failed to flush whitelist/blacklist updates: {exc}")

    mdc.write_line(
        "Bronze Massive market ingest complete: processed={processed} downloaded={downloaded} "
        "blacklisted={blacklisted} failed={failed}".format(**progress)
    )
    if retry_next_run:
        preview = ", ".join(sorted(retry_next_run)[:50])
        suffix = " ..." if len(retry_next_run) > 50 else ""
        mdc.write_line(
            f"Retry-on-next-run candidates (not blacklisted): count={len(retry_next_run)} symbols={preview}{suffix}"
        )
    return 0 if progress["failed"] == 0 else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    from tasks.common.job_trigger import trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "bronze-market-job"
    with mdc.JobLock(job_name):
        exit_code = main()
        if exit_code == 0:
            write_system_health_marker(layer="bronze", domain="market", job_name=job_name)
            trigger_next_job_from_env()
        raise SystemExit(exit_code)
