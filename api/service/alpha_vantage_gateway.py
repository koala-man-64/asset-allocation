from __future__ import annotations

import hashlib
import logging
import os
import threading
from dataclasses import dataclass
from typing import Any, Literal, Optional

from alpha_vantage import (
    AlphaVantageClient,
    AlphaVantageConfig,
    AlphaVantageError,
    AlphaVantageInvalidSymbolError,
    AlphaVantageThrottleError,
)

logger = logging.getLogger("asset-allocation.api.alpha_vantage")

FinanceReport = Literal["balance_sheet", "cash_flow", "income_statement", "overview"]


class AlphaVantageNotConfiguredError(RuntimeError):
    pass


@dataclass(frozen=True)
class _ClientSnapshot:
    api_key_hash: str
    base_url: str
    rate_limit_per_min: int
    timeout_seconds: float
    max_workers: int
    max_retries: int
    backoff_base_seconds: float


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _env_int(name: str, default: int) -> int:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return int(default)
    try:
        return int(raw)
    except Exception:
        logger.warning("Invalid int for %s=%r; using default=%s", name, raw, default)
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return float(default)
    try:
        return float(raw)
    except Exception:
        logger.warning("Invalid float for %s=%r; using default=%s", name, raw, default)
        return float(default)


def _hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class AlphaVantageGateway:
    """
    Process-local gateway for Alpha Vantage calls.

    Responsibilities:
      - Construct and hold a shared AlphaVantageClient (rate limiting is process-local).
      - Recreate the client if allowlisted env tuning changes (rate limit/timeout/etc).
      - Provide a constrained surface area used by API routes.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._client: AlphaVantageClient | None = None
        self._snapshot: _ClientSnapshot | None = None

    def _build_snapshot(self) -> tuple[_ClientSnapshot, AlphaVantageConfig]:
        api_key = _strip_or_none(os.environ.get("ALPHA_VANTAGE_API_KEY"))
        if not api_key:
            raise AlphaVantageNotConfiguredError("ALPHA_VANTAGE_API_KEY is not configured for the API service.")

        base_url = _strip_or_none(os.environ.get("ALPHA_VANTAGE_BASE_URL")) or "https://www.alphavantage.co"

        cfg = AlphaVantageConfig(
            api_key=api_key,
            base_url=base_url,
            rate_limit_per_min=_env_int("ALPHA_VANTAGE_RATE_LIMIT_PER_MIN", 300),
            timeout=_env_float("ALPHA_VANTAGE_TIMEOUT_SECONDS", 15.0),
            max_workers=_env_int("ALPHA_VANTAGE_MAX_WORKERS", 32),
            max_retries=_env_int("ALPHA_VANTAGE_MAX_RETRIES", 5),
            backoff_base_seconds=_env_float("ALPHA_VANTAGE_BACKOFF_BASE_SECONDS", 0.5),
        )

        snapshot = _ClientSnapshot(
            api_key_hash=_hash_secret(api_key),
            base_url=str(cfg.base_url),
            rate_limit_per_min=int(cfg.rate_limit_per_min),
            timeout_seconds=float(cfg.timeout),
            max_workers=int(cfg.max_workers),
            max_retries=int(cfg.max_retries),
            backoff_base_seconds=float(cfg.backoff_base_seconds),
        )
        return snapshot, cfg

    def get_client(self) -> AlphaVantageClient:
        snapshot, cfg = self._build_snapshot()
        with self._lock:
            if self._client is None or self._snapshot != snapshot:
                old = self._client
                self._client = AlphaVantageClient(cfg)
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

    def get_listing_status_csv(self, *, state: Optional[str] = "active", date: Optional[str] = None) -> str:
        return str(self.get_client().get_listing_status(state=state, date=date))

    def get_daily_time_series_csv(
        self,
        *,
        symbol: str,
        outputsize: str = "compact",
        adjusted: bool = False,
    ) -> str:
        return str(
            self.get_client().get_daily_time_series(symbol, outputsize=outputsize, adjusted=adjusted, datatype="csv")
        )

    def get_earnings(self, *, symbol: str) -> dict[str, Any]:
        payload = self.get_client().fetch("EARNINGS", symbol)
        if not isinstance(payload, dict):
            raise AlphaVantageError("Unexpected Alpha Vantage earnings response type.", code="invalid_payload")
        return payload

    def get_finance_report(self, *, symbol: str, report: FinanceReport) -> dict[str, Any]:
        function_by_report: dict[str, str] = {
            "balance_sheet": "BALANCE_SHEET",
            "cash_flow": "CASH_FLOW",
            "income_statement": "INCOME_STATEMENT",
            "overview": "OVERVIEW",
        }
        func = function_by_report.get(str(report))
        if not func:
            raise ValueError(f"Unknown finance report={report!r}")
        payload = self.get_client().fetch(func, symbol)
        if not isinstance(payload, dict):
            raise AlphaVantageError("Unexpected Alpha Vantage finance response type.", code="invalid_payload")
        return payload

