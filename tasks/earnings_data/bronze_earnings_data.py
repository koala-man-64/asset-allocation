from __future__ import annotations

import asyncio
import json
import os
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from io import BytesIO, StringIO
from typing import Any, Callable, Optional, Dict, Sequence

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
from tasks.common import bronze_bucketing
from tasks.common import domain_artifacts
from tasks.common.job_status import resolve_job_run_status
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
from tasks.common.silver_contracts import normalize_columns_to_snake_case


bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
common_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_COMMON)
list_manager = ListManager(
    bronze_client,
    cfg.EARNINGS_DATA_PREFIX,
    auto_flush=False,
    allow_blacklist_updates=False,
)


EARNINGS_STALE_DAYS = 7
_COVERAGE_DOMAIN = "earnings"
_COVERAGE_PROVIDER = "alpha-vantage"
_EARNINGS_CALENDAR_HORIZONS = frozenset({"3month", "6month", "12month"})
_EARNINGS_CALENDAR_EXPECTED_COLUMNS = (
    "symbol",
    "name",
    "reportDate",
    "fiscalDateEnding",
    "estimate",
    "currency",
    "timeOfTheDay",
)
_CANONICAL_EARNINGS_COLUMNS = [
    "symbol",
    "date",
    "report_date",
    "fiscal_date_ending",
    "reported_eps",
    "eps_estimate",
    "surprise",
    "record_type",
    "is_future_event",
    "calendar_time_of_day",
    "calendar_currency",
]
_BUCKET_COLUMNS = [
    *_CANONICAL_EARNINGS_COLUMNS,
    "ingested_at",
    "source_hash",
]


def _empty_coverage_summary() -> dict[str, int]:
    return {
        "coverage_checked": 0,
        "coverage_forced_refetch": 0,
        "coverage_marked_covered": 0,
        "coverage_marked_limited": 0,
        "coverage_skipped_limited_marker": 0,
    }


def _empty_event_summary() -> dict[str, int]:
    return {
        "scheduled_rows_retained": 0,
        "actual_over_scheduled_replacements": 0,
    }


def _utc_today() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc).date())


def _empty_canonical_earnings_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_BUCKET_COLUMNS)


def _normalize_calendar_horizon(value: object) -> str:
    text = str(value or "").strip().lower() or "12month"
    if text not in _EARNINGS_CALENDAR_HORIZONS:
        raise ValueError(
            f"Invalid ALPHA_VANTAGE_EARNINGS_CALENDAR_HORIZON={value!r}; expected one of 3month, 6month, 12month."
        )
    return text


def _format_payload_preview(payload: Any, *, max_chars: int = 500) -> Optional[str]:
    if payload is None:
        return None
    try:
        if isinstance(payload, (dict, list, tuple)):
            text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        else:
            text = str(payload)
    except Exception:
        try:
            text = repr(payload)
        except Exception:
            return None
    text = str(text).replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return None
    if len(text) > max_chars:
        return text[:max_chars] + "..."
    return text


def _format_invalid_payload_warning(symbol: str, exc: BaseException) -> str:
    message = f"Invalid earnings payload for {symbol}; automatic blacklist updates are disabled for job runs."
    preview_payload = getattr(exc, "payload", None)
    if preview_payload is None:
        preview_payload = {}
        status_code = getattr(exc, "status_code", None)
        if status_code is not None:
            preview_payload["status_code"] = status_code
        detail = getattr(exc, "detail", None)
        if detail:
            preview_payload["detail"] = detail
        exc_message = str(exc).strip()
        if exc_message and exc_message != detail:
            preview_payload["message"] = exc_message
        if not preview_payload:
            preview_payload = None
    preview = _format_payload_preview(preview_payload, max_chars=500)
    if preview:
        return f"{message} payload_preview={preview}"
    return message


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


def _coerce_datetime_column(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        parsed_default = pd.to_datetime(series, errors="coerce", utc=True)
        return parsed_default.dt.tz_localize(None)
    numeric = pd.to_numeric(series, errors="coerce")
    numeric_mask = numeric.notna()
    if numeric_mask.all():
        return pd.to_datetime(numeric, errors="coerce", unit="ms", utc=True).dt.tz_localize(None)
    if not numeric_mask.any():
        parsed_default = pd.to_datetime(series, errors="coerce", utc=True)
        return parsed_default.dt.tz_localize(None)
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]")
    parsed.loc[~numeric_mask] = pd.to_datetime(series.loc[~numeric_mask], errors="coerce", utc=True)
    parsed.loc[numeric_mask] = pd.to_datetime(numeric.loc[numeric_mask], errors="coerce", unit="ms", utc=True)
    return parsed.dt.tz_localize(None)


def _canonicalize_earnings_frame(df: Optional[pd.DataFrame], *, symbol: Optional[str] = None) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_canonical_earnings_frame()

    out = normalize_columns_to_snake_case(df).copy()
    if symbol is not None:
        out["symbol"] = str(symbol).strip().upper()
    elif "symbol" not in out.columns:
        out["symbol"] = pd.NA
    out["symbol"] = out["symbol"].astype("string").str.strip().str.upper()

    if "date" not in out.columns and "report_date" in out.columns:
        out["date"] = out["report_date"]
    for column in ("date", "report_date", "fiscal_date_ending"):
        if column in out.columns:
            out[column] = _coerce_datetime_column(out[column])
        else:
            out[column] = pd.NaT

    if "record_type" not in out.columns:
        out["record_type"] = "actual"
    out["record_type"] = out["record_type"].astype("string").str.strip().str.lower()
    out.loc[~out["record_type"].isin({"actual", "scheduled"}), "record_type"] = "actual"
    out.loc[out["record_type"].isna() | (out["record_type"] == ""), "record_type"] = "actual"

    actual_missing_fiscal = out["record_type"].eq("actual") & out["fiscal_date_ending"].isna()
    out.loc[actual_missing_fiscal, "fiscal_date_ending"] = out.loc[actual_missing_fiscal, "date"]
    scheduled_missing_date = out["record_type"].eq("scheduled") & out["date"].isna() & out["report_date"].notna()
    out.loc[scheduled_missing_date, "date"] = out.loc[scheduled_missing_date, "report_date"]

    for column in ("reported_eps", "eps_estimate", "surprise"):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
        else:
            out[column] = pd.NA

    for column in ("calendar_time_of_day", "calendar_currency", "ingested_at", "source_hash"):
        if column not in out.columns:
            out[column] = pd.NA

    if "is_future_event" in out.columns:
        parsed_future = pd.Series(
            pd.to_numeric(out["is_future_event"], errors="coerce"), index=out.index, dtype="Float64"
        )
    else:
        parsed_future = pd.Series(pd.NA, index=out.index, dtype="Float64")
    inferred_future = pd.Series(
        out["record_type"].eq("scheduled") & out["report_date"].notna() & (out["report_date"] >= _utc_today()),
        index=out.index,
        dtype="boolean",
    ).astype("Float64")
    out["is_future_event"] = parsed_future.fillna(inferred_future).fillna(0).astype(int)

    out = out.dropna(subset=["date"]).copy()
    return out[_BUCKET_COLUMNS].reset_index(drop=True)


def _event_identity_key(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="string")
    report_dates = pd.to_datetime(df["report_date"], errors="coerce")
    fiscal_dates = pd.to_datetime(df["fiscal_date_ending"], errors="coerce")
    base_dates = pd.to_datetime(df["date"], errors="coerce")
    # Earnings dates can move. When available, fiscal quarter end is the stable event identity.
    preferred = fiscal_dates.where(fiscal_dates.notna(), report_dates.where(report_dates.notna(), base_dates))
    return preferred.dt.strftime("%Y-%m-%d").fillna("")


def _select_actual_rows(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    canonical = _canonicalize_earnings_frame(df)
    if canonical.empty:
        return canonical
    return canonical.loc[canonical["record_type"] == "actual"].copy().reset_index(drop=True)


def _select_past_scheduled_rows(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    canonical = _canonicalize_earnings_frame(df)
    if canonical.empty:
        return canonical
    today = _utc_today()
    mask = (
        canonical["record_type"].eq("scheduled") & canonical["report_date"].notna() & (canonical["report_date"] < today)
    )
    return canonical.loc[mask].copy().reset_index(drop=True)


def _has_due_scheduled_rows(*frames: Optional[pd.DataFrame]) -> bool:
    today = _utc_today()
    for frame in frames:
        canonical = _canonicalize_earnings_frame(frame)
        if canonical.empty:
            continue
        if (
            canonical["record_type"].eq("scheduled")
            & canonical["report_date"].notna()
            & (canonical["report_date"] <= today)
        ).any():
            return True
    return False


def _dedupe_canonical_earnings_events(df: Optional[pd.DataFrame]) -> tuple[pd.DataFrame, int]:
    canonical = _canonicalize_earnings_frame(df)
    if canonical.empty:
        return canonical, 0

    work = canonical.copy()
    work["_event_identity"] = _event_identity_key(work)

    actual = (
        work.loc[work["record_type"] == "actual"]
        .sort_values(["symbol", "date", "report_date", "fiscal_date_ending"])
        .drop_duplicates(subset=["symbol", "_event_identity"], keep="last")
    )
    scheduled = (
        work.loc[work["record_type"] == "scheduled"]
        .sort_values(["symbol", "report_date", "date"])
        .drop_duplicates(subset=["symbol", "_event_identity"], keep="last")
    )
    actual_keys = {
        (str(symbol), str(event_identity))
        for symbol, event_identity in actual[["symbol", "_event_identity"]].itertuples(index=False, name=None)
    }
    scheduled_mask = scheduled.apply(
        lambda row: (str(row["symbol"]), str(row["_event_identity"])) not in actual_keys,
        axis=1,
    )
    filtered_scheduled = scheduled.loc[scheduled_mask].copy()
    replacements = int(len(scheduled) - len(filtered_scheduled))

    out = pd.concat([actual, filtered_scheduled], ignore_index=True, sort=False)
    out = out.drop(columns=["_event_identity"], errors="ignore")
    out = out.sort_values(["symbol", "date", "record_type"]).reset_index(drop=True)
    return out[_BUCKET_COLUMNS], replacements


def _stamp_canonical_earnings_frame(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    canonical = _canonicalize_earnings_frame(df)
    if canonical.empty:
        return canonical
    payload = canonical[_CANONICAL_EARNINGS_COLUMNS].to_json(orient="records", date_format="iso")
    now = datetime.now(timezone.utc).isoformat()
    source_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    canonical["ingested_at"] = now
    canonical["source_hash"] = source_hash
    return canonical[_BUCKET_COLUMNS].reset_index(drop=True)


def _canonical_payload_bytes(df: Optional[pd.DataFrame]) -> bytes:
    canonical = _canonicalize_earnings_frame(df)
    if canonical.empty:
        return b"[]"
    canonical = canonical.sort_values(["symbol", "date", "record_type"]).reset_index(drop=True)
    return canonical[_CANONICAL_EARNINGS_COLUMNS].to_json(orient="records", date_format="iso").encode("utf-8")


def _parse_historical_earnings_records(
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
                "symbol": symbol,
                "date": str(date_raw).strip(),
                "report_date": str(item.get("reportedDate") or "").strip() or None,
                "fiscal_date_ending": str(item.get("fiscalDateEnding") or "").strip() or None,
                "reported_eps": _coerce_float(item.get("reportedEPS")),
                "eps_estimate": _coerce_float(item.get("estimatedEPS")),
                "surprise": _coerce_surprise_fraction(item),
                "record_type": "actual",
                "is_future_event": 0,
                "calendar_time_of_day": None,
                "calendar_currency": None,
            }
        )

    df = pd.DataFrame(rows, columns=_CANONICAL_EARNINGS_COLUMNS)
    if df.empty:
        return _empty_canonical_earnings_frame()

    df = _canonicalize_earnings_frame(df, symbol=symbol)
    backfill_start_ts = pd.Timestamp(backfill_start) if backfill_start is not None else None
    df = filter_by_date(df, "date", backfill_start_ts, None)
    df = df.sort_values(["date"]).drop_duplicates(subset=["date", "symbol"], keep="last").reset_index(drop=True)
    return df


def _extract_source_earliest_earnings_date(payload: dict[str, Any]) -> Optional[date]:
    rows = payload.get("quarterlyEarnings")
    if not isinstance(rows, list):
        return None
    return extract_min_date_from_rows(rows, date_keys=("fiscalDateEnding", "reportedDate", "date"))


def _parse_earnings_calendar_csv(
    csv_text: str,
    *,
    symbols: Sequence[str],
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    text = str(csv_text or "").strip()
    if not text:
        raise ValueError("Alpha Vantage earnings calendar response was empty.")

    try:
        df = pd.read_csv(StringIO(text))
    except Exception as exc:
        preview = _format_payload_preview(text)
        raise ValueError(f"Unable to parse Alpha Vantage earnings calendar CSV. payload_preview={preview}") from exc

    missing_columns = [column for column in _EARNINGS_CALENDAR_EXPECTED_COLUMNS if column not in df.columns]
    if missing_columns:
        preview = _format_payload_preview(text)
        raise ValueError(
            "Alpha Vantage earnings calendar CSV missing required columns "
            f"{missing_columns}. payload_preview={preview}"
        )

    total_rows = int(len(df))
    normalized = normalize_columns_to_snake_case(df).copy()
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip().str.upper()
    normalized = normalized[normalized["symbol"].notna() & (normalized["symbol"] != "")].copy()
    normalized["report_date"] = _coerce_datetime_column(normalized["report_date"])
    normalized["fiscal_date_ending"] = _coerce_datetime_column(normalized["fiscal_date_ending"])
    normalized["estimate"] = pd.to_numeric(normalized.get("estimate"), errors="coerce")

    symbol_set = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
    matched = normalized[normalized["symbol"].isin(symbol_set)].copy()
    grouped: dict[str, pd.DataFrame] = {}
    for symbol, group in matched.groupby("symbol", dropna=True):
        grouped[str(symbol)] = group.reset_index(drop=True)

    max_report_date = matched["report_date"].dropna().max() if "report_date" in matched.columns else None
    summary = {
        "calendar_rows_fetched": total_rows,
        "calendar_symbols_matched": int(matched["symbol"].nunique()) if not matched.empty else 0,
        "calendar_symbols_ignored": int(normalized["symbol"].nunique() - matched["symbol"].nunique())
        if not normalized.empty
        else 0,
        "calendar_max_report_date": max_report_date.date().isoformat() if pd.notna(max_report_date) else None,
    }
    return grouped, summary


def _build_scheduled_earnings_rows(symbol: str, calendar_rows: Optional[pd.DataFrame]) -> pd.DataFrame:
    if calendar_rows is None or calendar_rows.empty:
        return _empty_canonical_earnings_frame()

    out = normalize_columns_to_snake_case(calendar_rows).copy()
    out["symbol"] = str(symbol).strip().upper()
    out["date"] = out.get("report_date")
    out["reported_eps"] = pd.NA
    out["eps_estimate"] = pd.to_numeric(out.get("estimate"), errors="coerce")
    out["surprise"] = pd.NA
    out["record_type"] = "scheduled"
    out["calendar_time_of_day"] = out.get("time_of_the_day")
    out["calendar_currency"] = out.get("currency")
    out["is_future_event"] = 1
    return _canonicalize_earnings_frame(out, symbol=symbol)


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
        return _empty_canonical_earnings_frame()
    return _canonicalize_earnings_frame(df)


def _extract_latest_earnings_date(df: pd.DataFrame) -> Optional[pd.Timestamp]:
    if df.empty or "date" not in df.columns:
        return None
    parsed = pd.to_datetime(df["date"], errors="coerce", utc=True).dropna()
    if parsed.empty:
        return None
    return parsed.max()


def _normalize_bucket_df(symbol: str, df: pd.DataFrame) -> pd.DataFrame:
    out = _canonicalize_earnings_frame(df, symbol=symbol)
    if out.empty:
        return out
    if out["source_hash"].isna().all() or out["ingested_at"].isna().all():
        out = _stamp_canonical_earnings_frame(out)
    return out[_BUCKET_COLUMNS].reset_index(drop=True)


def _write_alpha26_earnings_buckets(symbol_frames: Dict[str, pd.DataFrame]) -> tuple[int, Optional[str]]:
    bucket_frames = bronze_bucketing.empty_bucket_frames(_BUCKET_COLUMNS)
    symbol_to_bucket: dict[str, str] = {}
    for symbol, frame in symbol_frames.items():
        if frame is None or frame.empty:
            continue
        normalized = _normalize_bucket_df(symbol, frame)
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
            prefix=str(getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data")),
            bucket=bucket,
            df=frame,
            codec=bronze_bucketing.alpha26_codec(),
        )
        try:
            domain_artifacts.write_bucket_artifact(
                layer="bronze",
                domain="earnings",
                bucket=bucket,
                df=frame,
                date_column="date",
                client=bronze_client,
                job_name="bronze-earnings-job",
            )
        except Exception as exc:
            mdc.write_warning(f"Bronze earnings metadata bucket artifact write failed bucket={bucket}: {exc}")

    index_path = bronze_bucketing.write_symbol_index(
        domain="earnings",
        symbol_to_bucket=symbol_to_bucket,
    )
    if index_path:
        try:
            domain_artifacts.write_domain_artifact(
                layer="bronze",
                domain="earnings",
                date_column="date",
                client=bronze_client,
                symbol_count_override=len(symbol_to_bucket),
                symbol_index_path=index_path,
                job_name="bronze-earnings-job",
            )
        except Exception as exc:
            mdc.write_warning(f"Bronze earnings metadata artifact write failed: {exc}")
    return len(symbol_to_bucket), index_path


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
        self._clients: dict[int, AlphaVantageGatewayClient] = {}

    def get_client(self) -> AlphaVantageGatewayClient:
        thread_id = threading.get_ident()
        with self._lock:
            current = self._clients.get(thread_id)
            if current is not None:
                return current
            fresh_client = self._factory()
            self._clients[thread_id] = fresh_client
            return fresh_client

    def close_all(self) -> None:
        with self._lock:
            for client in list(self._clients.values()):
                _safe_close_alpha_vantage_client(client)
            self._clients.clear()


def _delete_flat_symbol_blobs() -> int:
    deleted = 0
    prefix = str(getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data")).strip("/")
    for blob in bronze_client.list_blob_infos(name_starts_with=f"{prefix}/"):
        name = str(blob.get("name") or "")
        if not name.endswith(".json"):
            continue
        if name.endswith("whitelist.csv") or name.endswith("blacklist.csv"):
            continue
        if "/buckets/" in name:
            continue
        try:
            bronze_client.delete_file(name)
            deleted += 1
        except Exception as exc:
            mdc.write_warning(f"Failed deleting flat earnings blob {name}: {exc}")
    return deleted


def _fetch_earnings_calendar_by_symbol(
    *,
    av: AlphaVantageGatewayClient,
    symbols: Sequence[str],
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    if not symbols:
        return {}, {
            "calendar_rows_fetched": 0,
            "calendar_symbols_matched": 0,
            "calendar_symbols_ignored": 0,
            "calendar_max_report_date": None,
        }

    horizon = _normalize_calendar_horizon(getattr(cfg, "ALPHA_VANTAGE_EARNINGS_CALENDAR_HORIZON", "12month"))
    csv_text = av.get_earnings_calendar_csv(horizon=horizon)
    grouped, summary = _parse_earnings_calendar_csv(csv_text, symbols=symbols)
    summary["calendar_horizon"] = horizon
    return grouped, summary


def fetch_and_save_raw(
    symbol: str,
    av: AlphaVantageGatewayClient,
    *,
    backfill_start: Optional[date] = None,
    coverage_summary: Optional[dict[str, int]] = None,
    event_summary: Optional[dict[str, int]] = None,
    calendar_rows: Optional[pd.DataFrame] = None,
    write_symbol_file: bool = True,
    collected_symbol_frames: Optional[Dict[str, pd.DataFrame]] = None,
    collected_lock: Optional[threading.Lock] = None,
    alpha26_mode: bool = False,
) -> bool:
    """
    Fetch earnings for a single symbol via the API-hosted Alpha Vantage gateway and store as Bronze JSON records.

    Returns True when a Bronze write occurred, False when skipped/no-op.
    """
    coverage_summary = coverage_summary if coverage_summary is not None else _empty_coverage_summary()
    event_summary = event_summary if event_summary is not None else _empty_event_summary()
    if list_manager.is_blacklisted(symbol):
        return False

    blob_path = f"{cfg.EARNINGS_DATA_PREFIX}/{symbol}.json"
    blob_exists: Optional[bool] = None
    resolved_backfill_start = normalize_date(backfill_start)
    existing_df = _empty_canonical_earnings_frame()
    existing_actual_df = _empty_canonical_earnings_frame()
    existing_min_date: Optional[date] = None
    force_backfill = False
    should_fetch_historical = True

    scheduled_rows = _build_scheduled_earnings_rows(symbol, calendar_rows)

    # Freshness gate only skips the per-symbol historical API fetch. Scheduled rows are still
    # merged every run from the bulk earnings-calendar feed.
    if not alpha26_mode:
        try:
            blob = bronze_client.get_blob_client(blob_path)
            blob_exists = bool(blob.exists())
            if blob_exists:
                existing_df = _load_existing_earnings_df(blob_path)
                existing_actual_df = _select_actual_rows(existing_df)
                props = blob.get_blob_properties()
                if resolved_backfill_start is not None:
                    coverage_summary["coverage_checked"] += 1
                    existing_min_date = extract_min_date_from_dataframe(existing_actual_df, date_col="date")
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
                if (
                    _is_fresh(props.last_modified, fresh_days=EARNINGS_STALE_DAYS)
                    and not force_backfill
                    and not existing_actual_df.empty
                    and not _has_due_scheduled_rows(existing_df, scheduled_rows)
                ):
                    should_fetch_historical = False
        except Exception:
            pass
    else:
        blob_exists = False

    payload: Optional[dict[str, Any]] = None
    source_earliest: Optional[date] = None
    has_source_records = False
    if should_fetch_historical:
        payload = av.get_earnings(symbol=symbol)
        if not isinstance(payload, dict):
            raise AlphaVantageGatewayError(
                "Unexpected Alpha Vantage earnings response type.", payload={"symbol": symbol}
            )

        source_records = payload.get("quarterlyEarnings") or []
        has_source_records = any(
            isinstance(item, dict) and (item.get("fiscalDateEnding") or item.get("reportedDate"))
            for item in source_records
        )
        source_earliest = _extract_source_earliest_earnings_date(payload)
        actual_rows = _parse_historical_earnings_records(symbol, payload, backfill_start=resolved_backfill_start)
    else:
        actual_rows = existing_actual_df.copy()
        source_earliest = extract_min_date_from_dataframe(actual_rows, date_col="date")
        has_source_records = not actual_rows.empty

    carry_forward_scheduled = _select_past_scheduled_rows(existing_df)
    merge_parts = [
        frame
        for frame in (actual_rows, carry_forward_scheduled, scheduled_rows)
        if frame is not None and not frame.empty
    ]
    if merge_parts:
        merged = pd.concat(merge_parts, ignore_index=True, sort=False)
    else:
        merged = _empty_canonical_earnings_frame()
    merged, actual_replacements = _dedupe_canonical_earnings_events(merged)
    if resolved_backfill_start is not None:
        merged = filter_by_date(merged, "date", pd.Timestamp(resolved_backfill_start), None)
    canonical_raw_json = _canonical_payload_bytes(merged)
    merged = _stamp_canonical_earnings_frame(merged)

    if merged is None or merged.empty:
        if not has_source_records and scheduled_rows.empty and carry_forward_scheduled.empty:
            raise AlphaVantageGatewayInvalidSymbolError(
                "No quarterly or scheduled earnings records found.",
                payload=payload or {"symbol": symbol},
            )
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
                mdc.write_line(f"No earnings rows on/after {cutoff_iso} for {symbol}; " f"deleted bronze {blob_path}.")
                list_manager.add_to_whitelist(symbol)
                return True
            list_manager.add_to_whitelist(symbol)
            return False
        raw_json = b"[]"
    else:
        raw_json = merged.to_json(orient="records").encode("utf-8")
        event_summary["scheduled_rows_retained"] += int(merged["record_type"].eq("scheduled").sum())
        event_summary["actual_over_scheduled_replacements"] += int(actual_replacements)

    if resolved_backfill_start is not None and force_backfill:
        marker_status = (
            "covered" if source_earliest is not None and source_earliest <= resolved_backfill_start else "limited"
        )
        _mark_coverage(
            symbol=symbol,
            backfill_start=resolved_backfill_start,
            status=marker_status,
            earliest_available=source_earliest,
            coverage_summary=coverage_summary,
        )

    if (not alpha26_mode) and blob_exists:
        if _canonical_payload_bytes(existing_df) == canonical_raw_json:
            list_manager.add_to_whitelist(symbol)
            return False

    if not write_symbol_file:
        if merged is not None and not merged.empty and collected_symbol_frames is not None:
            if collected_lock is not None:
                with collected_lock:
                    collected_symbol_frames[symbol] = merged.copy()
            else:
                collected_symbol_frames[symbol] = merged.copy()
        list_manager.add_to_whitelist(symbol)
        return bool(merged is not None and not merged.empty)

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

    alpha26_mode = bronze_bucketing.is_alpha26_mode()
    mdc.write_line(f"Starting Alpha Vantage Bronze Earnings Ingestion for {len(symbols)} symbols...")

    av = AlphaVantageGatewayClient.from_env()
    try:
        calendar_rows_by_symbol, calendar_summary = _fetch_earnings_calendar_by_symbol(av=av, symbols=symbols)
    finally:
        _safe_close_alpha_vantage_client(av)
    backfill_start = resolve_backfill_start_date()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to bronze earnings data: {backfill_start.isoformat()}")

    mdc.write_line(
        "Bronze AV earnings calendar: "
        f"horizon={calendar_summary.get('calendar_horizon', '12month')} "
        f"calendar_rows_fetched={calendar_summary.get('calendar_rows_fetched', 0)} "
        f"calendar_symbols_matched={calendar_summary.get('calendar_symbols_matched', 0)} "
        f"calendar_symbols_ignored={calendar_summary.get('calendar_symbols_ignored', 0)} "
        f"calendar_max_report_date={calendar_summary.get('calendar_max_report_date') or 'n/a'}"
    )

    max_workers = max(1, int(cfg.ALPHA_VANTAGE_MAX_WORKERS))
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="alpha-vantage-earnings")
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)
    client_manager = _ThreadLocalAlphaVantageClientManager()

    progress = {"processed": 0, "written": 0, "skipped": 0, "failed": 0, "blacklisted": 0}
    coverage_progress = _empty_coverage_summary()
    event_progress = _empty_event_summary()
    failure_counts: dict[str, int] = {}
    failure_examples: dict[str, str] = {}
    progress_lock = asyncio.Lock()
    collected_symbol_frames: Dict[str, pd.DataFrame] = {}
    collected_lock: Optional[threading.Lock] = threading.Lock() if alpha26_mode else None

    def worker(symbol: str) -> tuple[bool, dict[str, int], dict[str, int]]:
        av = client_manager.get_client()
        coverage_summary = _empty_coverage_summary()
        event_summary = _empty_event_summary()
        wrote = fetch_and_save_raw(
            symbol,
            av,
            backfill_start=backfill_start,
            coverage_summary=coverage_summary,
            event_summary=event_summary,
            calendar_rows=calendar_rows_by_symbol.get(symbol),
            write_symbol_file=not alpha26_mode,
            collected_symbol_frames=collected_symbol_frames if alpha26_mode else None,
            collected_lock=collected_lock if alpha26_mode else None,
            alpha26_mode=alpha26_mode,
        )
        return wrote, coverage_summary, event_summary

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
                wrote, coverage_summary, symbol_event_summary = await loop.run_in_executor(executor, worker, symbol)
                async with progress_lock:
                    if wrote:
                        progress["written"] += 1
                    else:
                        progress["skipped"] += 1
                    for key in coverage_progress:
                        coverage_progress[key] += int(coverage_summary.get(key, 0) or 0)
                    for key in event_progress:
                        event_progress[key] += int(symbol_event_summary.get(key, 0) or 0)
            except AlphaVantageGatewayInvalidSymbolError as exc:
                list_manager.add_to_blacklist(symbol)
                should_log = False
                async with progress_lock:
                    progress["blacklisted"] += 1
                    should_log = progress["blacklisted"] <= 20
                if should_log:
                    mdc.write_warning(_format_invalid_payload_warning(symbol, exc))
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
            client_manager.close_all()
        except Exception:
            pass
        try:
            list_manager.flush()
        except Exception as exc:
            mdc.write_warning(f"Failed to flush whitelist/blacklist updates: {exc}")

    if alpha26_mode:
        try:
            written_symbols, index_path = _write_alpha26_earnings_buckets(collected_symbol_frames)
            flat_deleted = _delete_flat_symbol_blobs()
            mdc.write_line(
                "Bronze earnings alpha26 buckets written: "
                f"symbols={written_symbols} index={index_path or 'n/a'} flat_deleted={flat_deleted}"
            )
        except Exception as exc:
            progress["failed"] += 1
            mdc.write_error(f"Bronze earnings alpha26 bucket write failed: {exc}")

    if failure_counts:
        ordered = sorted(failure_counts.items(), key=lambda item: item[1], reverse=True)
        summary = ", ".join(f"{name}={count}" for name, count in ordered[:8])
        mdc.write_warning(f"Bronze AV earnings failure summary: {summary}")
        for name, _ in ordered[:3]:
            example = failure_examples.get(name)
            if example:
                mdc.write_warning(f"Bronze AV earnings failure example ({name}): {example}")

    job_status, exit_code = resolve_job_run_status(
        failed_count=progress["failed"],
        warning_count=progress["blacklisted"],
    )
    mdc.write_line(
        "Bronze AV earnings ingest complete: processed={processed} written={written} skipped={skipped} "
        "blacklisted={blacklisted} failed={failed} coverage_checked={coverage_checked} "
        "coverage_forced_refetch={coverage_forced_refetch} coverage_marked_covered={coverage_marked_covered} "
        "coverage_marked_limited={coverage_marked_limited} coverage_skipped_limited_marker={coverage_skipped_limited_marker} "
        "scheduled_rows_retained={scheduled_rows_retained} actual_over_scheduled_replacements={actual_over_scheduled_replacements} "
        "job_status={job_status}".format(
            **progress,
            **coverage_progress,
            **event_progress,
            job_status=job_status,
        )
    )
    return exit_code


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "bronze-earnings-job"
    with mdc.JobLock(job_name):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(layer="bronze", domain="earnings", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )
