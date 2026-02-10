from __future__ import annotations

import csv
import hashlib
import io
import logging
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterator, Literal, Optional

from massive_provider import MassiveClient, MassiveConfig
from massive_provider.errors import (
    MassiveAuthError,
    MassiveError,
    MassiveNotFoundError,
    MassiveRateLimitError,
    MassiveServerError,
)
from massive_provider.utils import ms_to_iso_date

logger = logging.getLogger("asset-allocation.api.massive")
_FULL_HISTORY_START_DATE = "1900-01-01"

FinanceReport = Literal["balance_sheet", "cash_flow", "income_statement", "overview", "ratios"]


class MassiveNotConfiguredError(RuntimeError):
    pass


@dataclass(frozen=True)
class _ClientSnapshot:
    api_key_hash: str
    base_url: str
    timeout_seconds: float
    prefer_official_sdk: bool


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _normalize_caller_component(value: object, *, default: str) -> str:
    text = str(value or "").strip()
    if not text:
        return default
    if len(text) > 128:
        return text[:128]
    return text


_CALLER_JOB: ContextVar[str] = ContextVar("massive_caller_job", default="api")
_CALLER_EXECUTION: ContextVar[str] = ContextVar("massive_caller_execution", default="")


def get_current_caller_context() -> tuple[str, str]:
    return (
        _normalize_caller_component(_CALLER_JOB.get(), default="api"),
        _normalize_caller_component(_CALLER_EXECUTION.get(), default=""),
    )


@contextmanager
def massive_caller_context(
    *, caller_job: Optional[str], caller_execution: Optional[str] = None
) -> Iterator[None]:
    job_token = _CALLER_JOB.set(_normalize_caller_component(caller_job, default="api"))
    execution_token = _CALLER_EXECUTION.set(_normalize_caller_component(caller_execution, default=""))
    try:
        yield
    finally:
        _CALLER_JOB.reset(job_token)
        _CALLER_EXECUTION.reset(execution_token)


def _coerce_number(payload: dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except Exception:
            continue
    return None


def _extract_iso_date(payload: dict[str, Any]) -> Optional[str]:
    for key in (
        "date",
        "Date",
        "session",
        "day",
        "start",
        "start_date",
        "timestamp",
        "t",
        "time",
        "window_start",
    ):
        if key not in payload:
            continue
        value = payload.get(key)
        if value is None:
            continue

        if isinstance(value, (int, float)):
            try:
                iv = int(value)
                if abs(iv) > 10_000_000_000:
                    return ms_to_iso_date(iv)
                dt = datetime.fromtimestamp(iv, tz=timezone.utc)
                return dt.date().isoformat()
            except Exception:
                continue

        raw = str(value).strip()
        if not raw:
            continue
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            pass
        try:
            return date.fromisoformat(raw[:10]).isoformat()
        except Exception:
            continue
    return None


class MassiveGateway:
    """
    Process-local gateway for Massive provider calls.

    Responsibilities:
      - Construct and hold a shared MassiveClient.
      - Recreate the client if Massive env tuning changes.
      - Provide a constrained surface area used by API routes.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._client: MassiveClient | None = None
        self._snapshot: _ClientSnapshot | None = None

    def _build_snapshot(self) -> tuple[_ClientSnapshot, MassiveConfig]:
        try:
            cfg = MassiveConfig.from_env(require_api_key=True)
        except Exception as exc:
            raise MassiveNotConfiguredError("MASSIVE_API_KEY is not configured for the API service.") from exc

        snapshot = _ClientSnapshot(
            api_key_hash=_hash_secret(str(cfg.api_key)),
            base_url=str(cfg.base_url).rstrip("/"),
            timeout_seconds=float(cfg.timeout_seconds),
            prefer_official_sdk=bool(cfg.prefer_official_sdk),
        )
        return snapshot, cfg

    def get_client(self) -> MassiveClient:
        snapshot, cfg = self._build_snapshot()
        with self._lock:
            if self._client is None or self._snapshot != snapshot:
                old = self._client
                self._client = MassiveClient(cfg)
                self._snapshot = snapshot
                if old is not None:
                    try:
                        old.close()
                    except Exception:
                        pass
            return self._client

    def close(self) -> None:
        with self._lock:
            client = self._client
            self._client = None
            self._snapshot = None
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    def _normalize_ohlcv_rows(self, bars: list[dict[str, Any]]) -> list[dict[str, float | str]]:
        rows: list[dict[str, float | str]] = []
        for bar in bars:
            if not isinstance(bar, dict):
                continue

            as_of = _extract_iso_date(bar)
            open_ = _coerce_number(bar, "o", "open", "Open")
            high = _coerce_number(bar, "h", "high", "High")
            low = _coerce_number(bar, "l", "low", "Low")
            close = _coerce_number(bar, "c", "close", "Close")
            volume = _coerce_number(bar, "v", "volume", "Volume")

            if not as_of:
                continue
            if open_ is None or high is None or low is None or close is None:
                continue

            rows.append(
                {
                    "Date": as_of,
                    "Open": float(open_),
                    "High": float(high),
                    "Low": float(low),
                    "Close": float(close),
                    "Volume": float(volume or 0.0),
                }
            )

        rows.sort(key=lambda row: str(row["Date"]))
        return rows

    def _to_csv(self, rows: list[dict[str, float | str]]) -> str:
        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(["Date", "Open", "High", "Low", "Close", "Volume"])
        for row in rows:
            writer.writerow(
                [
                    row["Date"],
                    row["Open"],
                    row["High"],
                    row["Low"],
                    row["Close"],
                    row["Volume"],
                ]
            )
        return out.getvalue()

    def _normalize_daily_summary_row(self, payload: Any, *, fallback_date: str) -> Optional[dict[str, float | str]]:
        if not isinstance(payload, dict):
            return None

        as_of = _extract_iso_date(payload) or str(fallback_date)
        open_ = _coerce_number(payload, "open", "o", "Open")
        high = _coerce_number(payload, "high", "h", "High")
        low = _coerce_number(payload, "low", "l", "Low")
        close = _coerce_number(payload, "close", "c", "Close")
        volume = _coerce_number(payload, "volume", "v", "Volume")

        if not as_of:
            return None
        if open_ is None or high is None or low is None or close is None:
            return None

        return {
            "Date": str(as_of),
            "Open": float(open_),
            "High": float(high),
            "Low": float(low),
            "Close": float(close),
            "Volume": float(volume or 0.0),
        }

    def get_daily_time_series_csv(
        self,
        *,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        adjusted: bool = True,
    ) -> str:
        sym = str(symbol or "").strip().upper()
        if not sym:
            raise ValueError("symbol is required.")

        end_date = to_date or _utc_today_iso()
        start_date = from_date or _FULL_HISTORY_START_DATE

        if str(start_date) == str(end_date):
            try:
                summary_payload = self.get_client().get_daily_ticker_summary(
                    ticker=sym,
                    date=str(start_date),
                    adjusted=bool(adjusted),
                )
                row = self._normalize_daily_summary_row(summary_payload, fallback_date=str(start_date))
                if row is not None:
                    return self._to_csv([row])
            except MassiveNotFoundError:
                # Fallback to aggs endpoint below so callers still get a CSV response.
                pass

        bars = self.get_client().list_ohlcv(
            ticker=sym,
            multiplier=1,
            timespan="day",
            from_=str(start_date),
            to=str(end_date),
            adjusted=bool(adjusted),
            sort="asc",
            limit=50000,
            pagination=True,
        )
        rows = self._normalize_ohlcv_rows([b for b in bars if isinstance(b, dict)])
        return self._to_csv(rows)

    def get_short_interest(self, *, symbol: str) -> Any:
        return self.get_client().get_short_interest(
            ticker=str(symbol).strip().upper(),
            params={"sort": "settlement_date.asc", "limit": 50000},
            pagination=True,
        )

    def get_short_volume(self, *, symbol: str) -> Any:
        return self.get_client().get_short_volume(
            ticker=str(symbol).strip().upper(),
            params={"sort": "date.asc", "limit": 50000},
            pagination=True,
        )

    def get_float(self, *, symbol: str, as_of: Optional[str] = None) -> Any:
        return self.get_client().get_float(
            ticker=str(symbol).strip().upper(),
            as_of=as_of,
            params={"sort": "effective_date.asc", "limit": 5000},
            pagination=True,
        )

    def get_finance_report(self, *, symbol: str, report: FinanceReport) -> Any:
        by_report = {
            "balance_sheet": self.get_client().get_balance_sheet,
            "cash_flow": self.get_client().get_cash_flow_statement,
            "income_statement": self.get_client().get_income_statement,
            "overview": self.get_client().get_ratios,  # closest stable replacement for AV overview in existing flow
            "ratios": self.get_client().get_ratios,
        }
        handler = by_report.get(str(report))
        if handler is None:
            raise ValueError(f"Unknown finance report={report!r}")
        return handler(ticker=str(symbol).strip().upper())


__all__ = [
    "FinanceReport",
    "MassiveGateway",
    "MassiveNotConfiguredError",
    "MassiveError",
    "MassiveAuthError",
    "MassiveNotFoundError",
    "MassiveRateLimitError",
    "MassiveServerError",
    "massive_caller_context",
    "get_current_caller_context",
]
