from __future__ import annotations

import asyncio
import hashlib
import os
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from io import StringIO
from typing import Any, Callable, Dict, Optional

import pandas as pd

from core.massive_gateway_client import (
    MassiveGatewayClient,
    MassiveGatewayError,
    MassiveGatewayNotFoundError,
    MassiveGatewayRateLimitError,
)
from core import symbol_availability
from core import core as mdc
from core.pipeline import ListManager
from tasks.common import bronze_bucketing
from tasks.common import domain_artifacts
from tasks.common.bronze_symbol_policy import (
    BronzeCoverageUnavailableError,
    build_bronze_run_id,
    clear_invalid_candidate_marker,
    is_explicit_invalid_candidate,
    record_invalid_symbol_candidate,
)
from tasks.common.job_status import resolve_job_run_status
from tasks.market_data import config as cfg


bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
common_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_COMMON)
list_manager = ListManager(bronze_client, "market-data", auto_flush=False, allow_blacklist_updates=False)

_SUPPLEMENTAL_MARKET_COLUMNS = ("ShortInterest", "ShortVolume")
_RECOVERY_MAX_ATTEMPTS = 3
_RECOVERY_SLEEP_SECONDS = 5.0
_FULL_HISTORY_START_DATE = "1970-01-01"
_SNAPSHOT_BATCH_SIZE = 250
_SNAPSHOT_ASSET_TYPE = "stocks"
_REGIME_REQUIRED_MARKET_SYMBOLS = frozenset({"SPY", "^VIX", "^VIX3M"})
_BUCKET_COLUMNS = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "short_interest",
    "short_volume",
    "ingested_at",
    "source_hash",
]
_MARKET_OUTCOME_LOG_SAMPLE_LIMIT = 20
_MARKET_OUTCOME_LOG_INTERVAL = 250
_DOMAIN = "market"
_PROVIDER = "massive"


def _is_truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _is_regime_required_market_symbol(symbol: object) -> bool:
    normalized = str(symbol or "").strip().upper()
    return normalized in _REGIME_REQUIRED_MARKET_SYMBOLS


def _should_skip_blacklisted_market_symbol(symbol: object) -> bool:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return False
    if _is_regime_required_market_symbol(normalized):
        return False
    return bool(list_manager.is_blacklisted(normalized))


def _should_log_market_outcome(count: int) -> bool:
    return count <= _MARKET_OUTCOME_LOG_SAMPLE_LIMIT or count % _MARKET_OUTCOME_LOG_INTERVAL == 0


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


def _extract_snapshot_symbol(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("ticker", "symbol"):
        symbol = str(payload.get(key) or "").strip().upper()
        if symbol:
            return symbol

    details = payload.get("details")
    if isinstance(details, dict):
        for key in ("ticker", "symbol"):
            symbol = str(details.get(key) or "").strip().upper()
            if symbol:
                return symbol
    return None


def _extract_snapshot_daily_row(payload: dict[str, Any]) -> dict[str, float | str] | None:
    candidate_blocks: list[dict[str, Any]] = []
    for key in ("session", "day", "daily_bar", "bar"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            candidate_blocks.append(nested)
    candidate_blocks.append(payload)

    for block in candidate_blocks:
        open_ = _extract_first_numeric(block, ("open", "o", "Open"))
        high = _extract_first_numeric(block, ("high", "h", "High"))
        low = _extract_first_numeric(block, ("low", "l", "Low"))
        close = _extract_first_numeric(block, ("close", "c", "Close"))
        if open_ is None or high is None or low is None or close is None:
            continue

        date_raw = _extract_row_date(block) or _extract_row_date(payload) or _utc_today().isoformat()
        as_of = _extract_iso_date(date_raw)
        if not as_of:
            continue

        volume = _extract_first_numeric(block, ("volume", "v", "Volume"))
        return {
            "Date": as_of,
            "Open": float(open_),
            "High": float(high),
            "Low": float(low),
            "Close": float(close),
            "Volume": float(volume or 0.0),
        }
    return None


def _snapshot_row_to_daily_df(snapshot_row: dict[str, float | str] | None) -> pd.DataFrame | None:
    if not isinstance(snapshot_row, dict):
        return None

    required = ("Date", "Open", "High", "Low", "Close")
    if any(snapshot_row.get(key) is None for key in required):
        return None

    frame = pd.DataFrame([snapshot_row], columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
    frame = frame.dropna(subset=["Date"]).copy()
    if frame.empty:
        return None

    for col in ("Open", "High", "Low", "Close", "Volume"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    if frame[["Open", "High", "Low", "Close"]].isna().any().any():
        return None
    frame["Volume"] = frame["Volume"].fillna(0.0)
    return frame


def _chunk_symbols(symbols: list[str], chunk_size: int) -> list[list[str]]:
    out: list[list[str]] = []
    size = max(1, int(chunk_size))
    for idx in range(0, len(symbols), size):
        out.append(symbols[idx : idx + size])
    return out


def _fetch_snapshot_daily_rows(symbols: list[str]) -> dict[str, dict[str, float | str]]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    if not normalized:
        return {}

    rows_by_symbol: dict[str, dict[str, float | str]] = {}
    client = MassiveGatewayClient.from_env()
    try:
        for chunk in _chunk_symbols(normalized, _SNAPSHOT_BATCH_SIZE):
            payload = client.get_unified_snapshot(symbols=chunk, asset_type=_SNAPSHOT_ASSET_TYPE)
            for row in _extract_payload_rows(payload):
                symbol = _extract_snapshot_symbol(row)
                if not symbol:
                    continue
                snapshot_row = _extract_snapshot_daily_row(row)
                if snapshot_row is None:
                    continue
                current = rows_by_symbol.get(symbol)
                if current is None or str(snapshot_row["Date"]) >= str(current["Date"]):
                    rows_by_symbol[symbol] = snapshot_row
    finally:
        _safe_close_massive_client(client)

    return rows_by_symbol


def _can_use_snapshot_for_incremental(
    *,
    existing_latest_date: date | None,
) -> bool:
    if existing_latest_date is None:
        return False

    today = _utc_today()
    previous_business_day = (pd.Timestamp(today) - pd.tseries.offsets.BDay(1)).date()
    return existing_latest_date >= previous_business_day


def _extract_snapshot_date(snapshot_row: dict[str, float | str] | None) -> date | None:
    if not isinstance(snapshot_row, dict):
        return None
    iso_date = _extract_iso_date(snapshot_row.get("Date"))
    if not iso_date:
        return None
    try:
        return date.fromisoformat(iso_date)
    except Exception:
        return None


def _incoming_has_new_market_dates(
    *,
    existing_latest_date: date | None,
    incoming_df: pd.DataFrame | None,
) -> bool:
    if incoming_df is None or incoming_df.empty or "Date" not in incoming_df.columns:
        return False
    if existing_latest_date is None:
        return True
    parsed = pd.to_datetime(incoming_df["Date"], errors="coerce").dropna()
    if parsed.empty:
        return False
    return parsed.max().date() > existing_latest_date


def _existing_has_complete_supplementals(existing_df: pd.DataFrame, *, as_of_date: date | None) -> bool:
    if as_of_date is None or existing_df.empty or "Date" not in existing_df.columns:
        return False

    out = existing_df.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out = out.dropna(subset=["Date"]).copy()
    if out.empty:
        return False
    target = out.loc[out["Date"].dt.date == as_of_date]
    if target.empty:
        return False
    row = target.sort_values("Date").iloc[-1]
    for col in _SUPPLEMENTAL_MARKET_COLUMNS:
        if col not in target.columns:
            return False
        value = pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0]
        if pd.isna(value):
            return False
    return True


def _canonical_market_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ordered = ["Date", "Open", "High", "Low", "Close", "Volume", *_SUPPLEMENTAL_MARKET_COLUMNS]
    if "Date" not in out.columns:
        out["Date"] = pd.NaT
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out = out.dropna(subset=["Date"]).copy()
    for col in ordered:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[ordered].sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)
    out["Date"] = out["Date"].dt.strftime("%Y-%m-%d")
    numeric_columns = ("Open", "High", "Low", "Close", "Volume", *_SUPPLEMENTAL_MARKET_COLUMNS)
    for col in numeric_columns:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _market_frames_equal(existing_df: pd.DataFrame, merged_df: pd.DataFrame) -> bool:
    left = _canonical_market_df(existing_df)
    right = _canonical_market_df(merged_df)
    return left.equals(right)


def _merge_market_fundamentals(
    df_daily: pd.DataFrame,
    *,
    short_interest_payload: Any,
    short_volume_payload: Any,
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
        # Preserve temporal direction: allow carrying known values forward only.
        out = out.sort_values("Date").reset_index(drop=True)
        out[column_name] = out[column_name].ffill()

    for column_name in _SUPPLEMENTAL_MARKET_COLUMNS:
        if column_name not in out.columns:
            out[column_name] = pd.NA
        out[column_name] = pd.to_numeric(out[column_name], errors="coerce")

    return out
def _normalize_market_bucket_df(symbol: str, df_daily: pd.DataFrame) -> pd.DataFrame:
    out = df_daily.copy()
    out["symbol"] = str(symbol).upper()
    out["date"] = pd.to_datetime(out.get("Date"), errors="coerce", utc=True).dt.tz_localize(None)
    out["open"] = pd.to_numeric(out.get("Open"), errors="coerce")
    out["high"] = pd.to_numeric(out.get("High"), errors="coerce")
    out["low"] = pd.to_numeric(out.get("Low"), errors="coerce")
    out["close"] = pd.to_numeric(out.get("Close"), errors="coerce")
    out["volume"] = pd.to_numeric(out.get("Volume"), errors="coerce")
    out["short_interest"] = pd.to_numeric(out.get("ShortInterest"), errors="coerce")
    out["short_volume"] = pd.to_numeric(out.get("ShortVolume"), errors="coerce")
    out = out.dropna(subset=["date"]).copy()
    payload = out[
        [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "short_interest",
            "short_volume",
        ]
    ].to_json(orient="records", date_format="iso")
    out["source_hash"] = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    out["ingested_at"] = datetime.now(timezone.utc).isoformat()
    out = out[_BUCKET_COLUMNS]
    return out.reset_index(drop=True)


def _write_alpha26_market_buckets(symbol_frames: dict[str, pd.DataFrame]) -> tuple[int, str | None]:
    bucket_frames = bronze_bucketing.empty_bucket_frames(_BUCKET_COLUMNS)
    symbol_to_bucket: dict[str, str] = {}
    for symbol, frame in symbol_frames.items():
        if frame is None or frame.empty:
            continue
        normalized = _normalize_market_bucket_df(symbol, frame)
        if normalized.empty:
            continue
        bucket = bronze_bucketing.bucket_letter(symbol)
        symbol_to_bucket[str(symbol).upper()] = bucket
        if bucket_frames[bucket].empty:
            bucket_frames[bucket] = normalized
        else:
            bucket_frames[bucket] = pd.concat([bucket_frames[bucket], normalized], ignore_index=True)

    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        frame = bucket_frames[bucket]
        if frame.empty:
            frame = pd.DataFrame(columns=_BUCKET_COLUMNS)
        bronze_bucketing.write_bucket_parquet(
            client=bronze_client,
            prefix="market-data",
            bucket=bucket,
            df=frame,
            codec=bronze_bucketing.alpha26_codec(),
        )
        try:
            domain_artifacts.write_bucket_artifact(
                layer="bronze",
                domain="market",
                bucket=bucket,
                df=frame,
                date_column="date",
                client=bronze_client,
                job_name="bronze-market-job",
            )
        except Exception as exc:
            mdc.write_warning(f"Bronze market metadata bucket artifact write failed bucket={bucket}: {exc}")

    index_path = bronze_bucketing.write_symbol_index(
        domain="market",
        symbol_to_bucket=symbol_to_bucket,
    )
    if index_path:
        try:
            domain_artifacts.write_domain_artifact(
                layer="bronze",
                domain="market",
                date_column="date",
                client=bronze_client,
                symbol_count_override=len(symbol_to_bucket),
                symbol_index_path=index_path,
                job_name="bronze-market-job",
            )
        except Exception as exc:
            mdc.write_warning(f"Bronze market metadata artifact write failed: {exc}")
    return len(symbol_to_bucket), index_path


def _delete_flat_symbol_blobs() -> int:
    deleted = 0
    for blob in bronze_client.list_blob_infos(name_starts_with="market-data/"):
        name = str(blob.get("name") or "")
        if not name.endswith(".csv"):
            continue
        if name.endswith("whitelist.csv") or name.endswith("blacklist.csv"):
            continue
        if "/buckets/" in name:
            continue
        try:
            bronze_client.delete_file(name)
            deleted += 1
        except Exception as exc:
            mdc.write_warning(f"Failed deleting flat market blob {name}: {exc}")
    return deleted


def _load_alpha26_existing_market_frames(*, symbols: set[str]) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    failed_buckets: list[str] = []
    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        try:
            bucket_df = bronze_bucketing.read_bucket_parquet(
                client=bronze_client,
                prefix="market-data",
                bucket=bucket,
            )
        except Exception as exc:
            failed_buckets.append(bucket)
            mdc.write_error(f"Bronze market alpha26 preload failed bucket={bucket}: {exc}")
            continue
        if bucket_df is None or bucket_df.empty:
            continue

        out = bucket_df.copy()
        rename_map = {
            "symbol": "Symbol",
            "date": "Date",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
            "short_interest": "ShortInterest",
            "short_volume": "ShortVolume",
        }
        out = out.rename(columns={k: v for k, v in rename_map.items() if k in out.columns})
        if "Symbol" not in out.columns or "Date" not in out.columns:
            continue

        out["Symbol"] = out["Symbol"].astype(str).str.strip().str.upper()
        out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
        out = out.dropna(subset=["Symbol", "Date"]).copy()
        if out.empty:
            continue
        out["Date"] = out["Date"].dt.strftime("%Y-%m-%d")
        keep_cols = ["Symbol", "Date", "Open", "High", "Low", "Close", "Volume", *_SUPPLEMENTAL_MARKET_COLUMNS]
        out = out[[c for c in keep_cols if c in out.columns]]
        for symbol, group in out.groupby("Symbol", sort=False):
            clean_symbol = str(symbol).strip().upper()
            if not clean_symbol or (symbols and clean_symbol not in symbols):
                continue
            group = group.drop(columns=["Symbol"], errors="ignore")
            group = _canonical_market_df(group)
            if not group.empty:
                frames[clean_symbol] = group
    if failed_buckets:
        bucket_list = ",".join(sorted(failed_buckets))
        raise RuntimeError(f"Bronze market alpha26 preload failed for bucket(s): {bucket_list}")
    return frames


def _set_collected_market_frame(
    *,
    symbol: str,
    frame: pd.DataFrame,
    collected_symbol_frames: Optional[Dict[str, pd.DataFrame]],
    collected_lock: Optional[threading.Lock],
) -> None:
    if collected_symbol_frames is None:
        return
    normalized_symbol = str(symbol).strip().upper()
    if not normalized_symbol:
        return
    normalized_frame = _canonical_market_df(frame)
    if collected_lock is not None:
        with collected_lock:
            collected_symbol_frames[normalized_symbol] = normalized_frame
    else:
        collected_symbol_frames[normalized_symbol] = normalized_frame


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


def _resolve_fetch_window(
    *,
    existing_latest_date: date | None,
) -> tuple[str, str]:
    today = _utc_today()
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

    def reset_current(self) -> None:
        thread_id = threading.get_ident()
        with self._lock:
            current = self._clients.pop(thread_id, None)
        if current:
            _safe_close_massive_client(current[1])

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
    snapshot_row: dict[str, float | str] | None = None,
    collected_symbol_frames: Dict[str, pd.DataFrame],
    collected_lock: Optional[threading.Lock] = None,
    existing_symbol_df: Optional[pd.DataFrame] = None,
    max_attempts: int = _RECOVERY_MAX_ATTEMPTS,
    sleep_seconds: float = _RECOVERY_SLEEP_SECONDS,
) -> None:
    attempts = max(1, int(max_attempts))
    sleep_seconds = max(0.0, float(sleep_seconds))

    for attempt in range(1, attempts + 1):
        client = client_manager.get_client()
        try:
            download_and_save_raw(
                symbol,
                client,
                snapshot_row=snapshot_row,
                collected_symbol_frames=collected_symbol_frames,
                collected_lock=collected_lock,
                existing_symbol_df=existing_symbol_df,
            )
            return
        except (BronzeCoverageUnavailableError, MassiveGatewayNotFoundError):
            raise
        except Exception as exc:
            should_retry = _is_recoverable_massive_error(exc) and attempt < attempts
            if not should_retry:
                raise

            mdc.write_warning(
                f"Transient Massive error for {symbol}; attempt {attempt}/{attempts} failed ({exc}). "
                f"Sleeping {sleep_seconds:.1f}s, recycling thread-local client, and retrying."
            )
            time.sleep(sleep_seconds)
            client_manager.reset_current()


def download_and_save_raw(
    symbol: str,
    massive_client: MassiveGatewayClient,
    *,
    snapshot_row: dict[str, float | str] | None = None,
    collected_symbol_frames: Dict[str, pd.DataFrame],
    collected_lock: Optional[threading.Lock] = None,
    existing_symbol_df: Optional[pd.DataFrame] = None,
) -> None:
    if _should_skip_blacklisted_market_symbol(symbol):
        return

    if existing_symbol_df is not None and not existing_symbol_df.empty:
        existing_df = _canonical_market_df(existing_symbol_df)
    else:
        existing_df = pd.DataFrame()
    existing_latest_date = _extract_latest_market_date(existing_df)
    from_date, to_date = _resolve_fetch_window(existing_latest_date=existing_latest_date)
    raw_text = ""
    df_daily = None
    snapshot_date = _extract_snapshot_date(snapshot_row)
    if _can_use_snapshot_for_incremental(
        existing_latest_date=existing_latest_date,
    ):
        if snapshot_date is not None and existing_latest_date is not None and snapshot_date <= existing_latest_date:
            # Snapshot confirms we are already at the latest obtainable daily bar.
            if not existing_df.empty:
                _set_collected_market_frame(
                    symbol=symbol,
                    frame=existing_df,
                    collected_symbol_frames=collected_symbol_frames,
                    collected_lock=collected_lock,
                )
            list_manager.add_to_whitelist(symbol)
            return
        df_daily = _snapshot_row_to_daily_df(snapshot_row)
        if df_daily is not None and existing_latest_date is not None:
            snapshot_dates = pd.to_datetime(df_daily["Date"], errors="coerce").dropna()
            if snapshot_dates.empty or snapshot_dates.max().date() < existing_latest_date:
                # Snapshot can lag around weekends/market close windows.
                if not existing_df.empty:
                    _set_collected_market_frame(
                        symbol=symbol,
                        frame=existing_df,
                        collected_symbol_frames=collected_symbol_frames,
                        collected_lock=collected_lock,
                    )
                list_manager.add_to_whitelist(symbol)
                return

    if df_daily is None:
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
                if not existing_df.empty:
                    _set_collected_market_frame(
                        symbol=symbol,
                        frame=existing_df,
                        collected_symbol_frames=collected_symbol_frames,
                        collected_lock=collected_lock,
                    )
                list_manager.add_to_whitelist(symbol)
                return
            raise BronzeCoverageUnavailableError(
                "header_only_daily_csv",
                detail=(
                    f"Massive returned header-only daily CSV for {symbol} in range {from_date}..{to_date}."
                ),
                payload={"symbol": symbol, "from_date": from_date, "to_date": to_date},
            )

    try:
        if df_daily is None:
            df_daily = _normalize_provider_daily_df(raw_text)

        has_new_daily_rows = _incoming_has_new_market_dates(
            existing_latest_date=existing_latest_date,
            incoming_df=df_daily,
        )
        if (
            existing_latest_date is not None
            and not has_new_daily_rows
            and _existing_has_complete_supplementals(existing_df, as_of_date=existing_latest_date)
        ):
            # No new market rows and supplemental metrics already populated.
            if not existing_df.empty:
                _set_collected_market_frame(
                    symbol=symbol,
                    frame=existing_df,
                    collected_symbol_frames=collected_symbol_frames,
                    collected_lock=collected_lock,
                )
            list_manager.add_to_whitelist(symbol)
            return

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

        df_daily = _merge_market_fundamentals(
            df_daily,
            short_interest_payload=short_interest_payload,
            short_volume_payload=short_volume_payload,
        )
        df_daily = _merge_existing_and_new_market_data(existing_df, df_daily)
        if not existing_df.empty and _market_frames_equal(existing_df, df_daily):
            if not existing_df.empty:
                _set_collected_market_frame(
                    symbol=symbol,
                    frame=existing_df,
                    collected_symbol_frames=collected_symbol_frames,
                    collected_lock=collected_lock,
                )
            list_manager.add_to_whitelist(symbol)
            return
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

    _set_collected_market_frame(
        symbol=symbol,
        frame=df_daily,
        collected_symbol_frames=collected_symbol_frames,
        collected_lock=collected_lock,
    )
    list_manager.add_to_whitelist(symbol)
    return


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


async def main_async() -> int:
    mdc.log_environment_diagnostics()
    _validate_environment()

    list_manager.load()
    mdc.write_line(
        f"Bronze market blacklist loaded with {len(list_manager.blacklist)} symbols (excluded from scheduling)."
    )

    sync_result = symbol_availability.sync_domain_availability("market")
    mdc.write_line(
        "Bronze market availability sync: "
        f"provider={sync_result.provider} listed_count={sync_result.listed_count} "
        f"inserted_count={sync_result.inserted_count} disabled_count={sync_result.disabled_count} "
        f"duration_ms={sync_result.duration_ms} lock_wait_ms={sync_result.lock_wait_ms}"
    )
    df_symbols = symbol_availability.get_domain_symbols("market")
    provider_available_count = int(df_symbols["Symbol"].dropna().shape[0]) if "Symbol" in df_symbols.columns else 0

    symbols: list[str] = []
    blacklist_skipped = 0
    for raw in df_symbols["Symbol"].dropna().astype(str).tolist():
        if "." in raw:
            continue
        if _should_skip_blacklisted_market_symbol(raw):
            blacklist_skipped += 1
            continue
        symbols.append(raw)
    # Preserve original ordering while de-duping.
    symbols = list(dict.fromkeys(symbols))

    debug_mode = bool(hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS)
    debug_filtered = 0
    if debug_mode:
        mdc.write_line(f"DEBUG MODE: Restricting to {cfg.DEBUG_SYMBOLS}")
        filtered_symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]
        debug_filtered = len(symbols) - len(filtered_symbols)
        symbols = filtered_symbols

    mdc.write_line(
        "Bronze market symbol selection: "
        f"provider_available_count={provider_available_count} "
        f"blacklist_skipped={blacklist_skipped} "
        f"debug_filtered={debug_filtered} "
        f"final_scheduled={len(symbols)}"
    )
    run_id = build_bronze_run_id(_DOMAIN)

    bronze_bucketing.bronze_layout_mode()
    mdc.write_line(f"Starting Massive Bronze Market Ingestion for {len(symbols)} symbols...")
    symbol_set = {str(s).strip().upper() for s in symbols}
    alpha26_existing_frames = _load_alpha26_existing_market_frames(symbols=symbol_set)
    # Seed with existing rows to preserve unchanged symbols under full 26-bucket rewrite.
    collected_symbol_frames: Dict[str, pd.DataFrame] = {k: v.copy() for k, v in alpha26_existing_frames.items()}
    collected_lock: Optional[threading.Lock] = threading.Lock()
    mdc.write_line(
        f"Loaded existing market alpha26 seed frames: symbols={len(collected_symbol_frames)}."
    )

    snapshot_rows_by_symbol: dict[str, dict[str, float | str]] = {}
    if symbols:
        try:
            mdc.write_line(
                f"Prefetching Massive unified snapshots in batches (chunk_size={_SNAPSHOT_BATCH_SIZE})..."
            )
            snapshot_rows_by_symbol = _fetch_snapshot_daily_rows(symbols)
            mdc.write_line(
                f"Massive unified snapshot prefetch complete: rows={len(snapshot_rows_by_symbol)} symbols."
            )
        except Exception as exc:
            mdc.write_warning(
                f"Massive unified snapshot prefetch failed; falling back to symbol-level daily fetches. ({exc})"
            )
            snapshot_rows_by_symbol = {}

    client_manager = _ThreadLocalMassiveClientManager()

    progress = {
        "processed": 0,
        "downloaded": 0,
        "failed": 0,
        "invalid_candidates": 0,
        "unavailable": 0,
        "blacklist_promotions": 0,
    }
    retry_next_run: set[str] = set()
    progress_lock = asyncio.Lock()

    def worker(symbol: str) -> None:
        if _should_skip_blacklisted_market_symbol(symbol):
            return

        _download_and_save_raw_with_recovery(
            symbol,
            client_manager,
            snapshot_row=snapshot_rows_by_symbol.get(symbol),
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=collected_lock,
            existing_symbol_df=alpha26_existing_frames.get(symbol),
        )

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
                try:
                    clear_invalid_candidate_marker(common_client=common_client, domain=_DOMAIN, symbol=symbol)
                except Exception as exc:
                    mdc.write_warning(f"Failed to clear market invalid-candidate marker for {symbol}: {exc}")
                should_log = debug_mode
                async with progress_lock:
                    progress["downloaded"] += 1
                    downloaded = progress["downloaded"]
                    should_log = should_log or _should_log_market_outcome(downloaded)
                if should_log:
                    mdc.write_line(
                        f"Bronze market symbol completed: symbol={symbol} downloaded={downloaded}"
                    )
            except BronzeCoverageUnavailableError as exc:
                should_log = debug_mode
                async with progress_lock:
                    progress["unavailable"] += 1
                    should_log = should_log or _should_log_market_outcome(progress["unavailable"])
                if should_log:
                    mdc.write_warning(
                        f"Bronze market coverage unavailable: symbol={symbol} reason={exc.reason_code} detail={exc}"
                    )
            except MassiveGatewayNotFoundError as exc:
                if not is_explicit_invalid_candidate(exc):
                    raise
                promotion = record_invalid_symbol_candidate(
                    common_client=common_client,
                    bronze_client=bronze_client,
                    domain=_DOMAIN,
                    symbol=symbol,
                    provider=_PROVIDER,
                    reason_code="provider_invalid_symbol",
                    run_id=run_id,
                )
                should_log = debug_mode
                async with progress_lock:
                    progress["invalid_candidates"] += 1
                    if promotion.get("promoted"):
                        progress["blacklist_promotions"] += 1
                    should_log = should_log or _should_log_market_outcome(progress["invalid_candidates"])
                if should_log:
                    message = (
                        f"Bronze market invalid symbol candidate: symbol={symbol} status=404 "
                        f"observed_runs={promotion.get('observedRunCount', 1)}"
                    )
                    if promotion.get("promoted"):
                        message += " promoted_to_domain_blacklist_after_2_runs=true"
                    mdc.write_warning(message)
            except MassiveGatewayRateLimitError as exc:
                should_log = debug_mode
                async with progress_lock:
                    progress["failed"] += 1
                    retry_next_run.add(symbol)
                    should_log = should_log or _should_log_market_outcome(progress["failed"])
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
                    should_log = should_log or _should_log_market_outcome(progress["failed"])
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
                    should_log = should_log or _should_log_market_outcome(progress["failed"])
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
                            "invalid_candidates={invalid_candidates} unavailable={unavailable} "
                            "blacklist_promotions={blacklist_promotions} failed={failed}".format(**progress)
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

    try:
        written_symbols, index_path = _write_alpha26_market_buckets(collected_symbol_frames)
        flat_deleted = _delete_flat_symbol_blobs()
        mdc.write_line(
            "Bronze market alpha26 buckets written: "
            f"symbols={written_symbols} index={index_path or 'n/a'} flat_deleted={flat_deleted}"
        )
    except Exception as exc:
        progress["failed"] += 1
        mdc.write_error(f"Bronze market alpha26 bucket write failed: {exc}")

    job_status, exit_code = resolve_job_run_status(
        failed_count=progress["failed"],
        warning_count=progress["invalid_candidates"],
    )
    mdc.write_line(
        "Bronze Massive market ingest complete: processed={processed} downloaded={downloaded} "
        "invalid_candidates={invalid_candidates} unavailable={unavailable} "
        "blacklist_promotions={blacklist_promotions} failed={failed} job_status={job_status}".format(
            **progress,
            job_status=job_status,
        )
    )
    if retry_next_run:
        preview = ", ".join(sorted(retry_next_run)[:50])
        suffix = " ..." if len(retry_next_run) > 50 else ""
        mdc.write_line(
            f"Retry-on-next-run candidates (not promoted): count={len(retry_next_run)} symbols={preview}{suffix}"
        )
    return exit_code


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "bronze-market-job"
    with mdc.JobLock(job_name):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(layer="bronze", domain="market", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )
