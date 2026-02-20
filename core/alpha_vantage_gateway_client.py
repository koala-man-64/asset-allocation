from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)
_MIN_API_GATEWAY_TIMEOUT_SECONDS = 600.0
_DEFAULT_API_WARMUP_ENABLED = True
_DEFAULT_API_WARMUP_MAX_ATTEMPTS = 3
_DEFAULT_API_WARMUP_BASE_DELAY_SECONDS = 1.0
_DEFAULT_API_WARMUP_MAX_DELAY_SECONDS = 8.0
_DEFAULT_API_WARMUP_PROBE_TIMEOUT_SECONDS = 5.0
_API_WARMUP_PROBE_PATH = "/healthz"
_RETRYABLE_WARMUP_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


class AlphaVantageGatewayError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        detail: Optional[str] = None,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail
        self.payload = payload


class AlphaVantageGatewayAuthError(AlphaVantageGatewayError):
    pass


class AlphaVantageGatewayThrottleError(AlphaVantageGatewayError):
    pass


class AlphaVantageGatewayInvalidSymbolError(AlphaVantageGatewayError):
    pass


class AlphaVantageGatewayUnavailableError(AlphaVantageGatewayError):
    pass


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _env_float(name: str, default: float) -> float:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return bool(default)
    lowered = raw.lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_int(name: str, default: int) -> int:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


@dataclass(frozen=True)
class AlphaVantageGatewayClientConfig:
    base_url: str
    api_key: Optional[str]
    api_key_header: str
    timeout_seconds: float
    warmup_enabled: bool = _DEFAULT_API_WARMUP_ENABLED
    warmup_max_attempts: int = _DEFAULT_API_WARMUP_MAX_ATTEMPTS
    warmup_base_delay_seconds: float = _DEFAULT_API_WARMUP_BASE_DELAY_SECONDS
    warmup_max_delay_seconds: float = _DEFAULT_API_WARMUP_MAX_DELAY_SECONDS
    warmup_probe_timeout_seconds: float = _DEFAULT_API_WARMUP_PROBE_TIMEOUT_SECONDS


class AlphaVantageGatewayClient:
    """
    Minimal sync client for the API-hosted Alpha Vantage gateway.

    ETL jobs should use this instead of calling Alpha Vantage directly.
    """

    def __init__(
        self,
        config: AlphaVantageGatewayClientConfig,
        *,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        self.config = config
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client(timeout=httpx.Timeout(config.timeout_seconds), trust_env=False)
        self._warmup_lock = threading.Lock()
        self._warmup_attempted = False

    @staticmethod
    def from_env() -> "AlphaVantageGatewayClient":
        base_url = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_BASE_URL")) or _strip_or_none(
            os.environ.get("ASSET_ALLOCATION_API_URL")
        )
        if not base_url:
            raise ValueError("ASSET_ALLOCATION_API_BASE_URL is required for Alpha Vantage ETL via API gateway.")

        api_key = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_KEY")) or _strip_or_none(
            os.environ.get("API_KEY")
        )
        api_key_header = (
            _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_KEY_HEADER"))
            or _strip_or_none(os.environ.get("API_KEY_HEADER"))
            or "X-API-Key"
        )

        timeout_seconds = _env_float(
            "ASSET_ALLOCATION_API_TIMEOUT_SECONDS",
            _env_float("ALPHA_VANTAGE_TIMEOUT_SECONDS", _MIN_API_GATEWAY_TIMEOUT_SECONDS),
        )
        if timeout_seconds < _MIN_API_GATEWAY_TIMEOUT_SECONDS:
            logger.warning(
                "ASSET_ALLOCATION_API_TIMEOUT_SECONDS=%s is too low for Alpha Vantage cooldown waits; using %s.",
                timeout_seconds,
                _MIN_API_GATEWAY_TIMEOUT_SECONDS,
            )
            timeout_seconds = _MIN_API_GATEWAY_TIMEOUT_SECONDS

        warmup_enabled = _env_bool("ASSET_ALLOCATION_API_WARMUP_ENABLED", _DEFAULT_API_WARMUP_ENABLED)
        warmup_max_attempts = max(1, _env_int("ASSET_ALLOCATION_API_WARMUP_ATTEMPTS", _DEFAULT_API_WARMUP_MAX_ATTEMPTS))
        warmup_base_delay_seconds = max(
            0.0,
            _env_float("ASSET_ALLOCATION_API_WARMUP_BASE_SECONDS", _DEFAULT_API_WARMUP_BASE_DELAY_SECONDS),
        )
        warmup_max_delay_seconds = max(
            warmup_base_delay_seconds,
            _env_float("ASSET_ALLOCATION_API_WARMUP_MAX_SECONDS", _DEFAULT_API_WARMUP_MAX_DELAY_SECONDS),
        )
        warmup_probe_timeout_seconds = max(
            0.1,
            _env_float("ASSET_ALLOCATION_API_WARMUP_PROBE_TIMEOUT_SECONDS", _DEFAULT_API_WARMUP_PROBE_TIMEOUT_SECONDS),
        )

        return AlphaVantageGatewayClient(
            AlphaVantageGatewayClientConfig(
                base_url=str(base_url).rstrip("/"),
                api_key=api_key,
                api_key_header=str(api_key_header),
                timeout_seconds=float(timeout_seconds),
                warmup_enabled=warmup_enabled,
                warmup_max_attempts=warmup_max_attempts,
                warmup_base_delay_seconds=warmup_base_delay_seconds,
                warmup_max_delay_seconds=warmup_max_delay_seconds,
                warmup_probe_timeout_seconds=warmup_probe_timeout_seconds,
            )
        )

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def __enter__(self) -> "AlphaVantageGatewayClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.config.api_key:
            headers[str(self.config.api_key_header)] = str(self.config.api_key)
        caller_job = _strip_or_none(os.environ.get("CONTAINER_APP_JOB_NAME"))
        caller_execution = _strip_or_none(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME"))
        if caller_job:
            headers["X-Caller-Job"] = str(caller_job)
        if caller_execution:
            headers["X-Caller-Execution"] = str(caller_execution)
        return headers

    def _extract_detail(self, response: httpx.Response) -> str:
        try:
            payload = response.json()
        except Exception:
            text = (response.text or "").strip()
            return text or response.reason_phrase
        if isinstance(payload, dict):
            detail = payload.get("detail")
            if isinstance(detail, str) and detail.strip():
                return detail.strip()
            return json.dumps(payload, ensure_ascii=False)
        if isinstance(payload, str) and payload.strip():
            return payload.strip()
        return response.reason_phrase

    def _warm_up_gateway(self) -> None:
        if not self.config.warmup_enabled or self._warmup_attempted:
            return

        with self._warmup_lock:
            if not self.config.warmup_enabled or self._warmup_attempted:
                return
            try:
                delay_seconds = max(0.0, float(self.config.warmup_base_delay_seconds))
                max_delay_seconds = max(delay_seconds, float(self.config.warmup_max_delay_seconds))
                attempts = max(1, int(self.config.warmup_max_attempts))
                probe_timeout = min(float(self.config.timeout_seconds), float(self.config.warmup_probe_timeout_seconds))
                warmup_timeout = httpx.Timeout(probe_timeout)
                probe_url = f"{self.config.base_url}{_API_WARMUP_PROBE_PATH}"

                for attempt in range(1, attempts + 1):
                    should_retry = attempt < attempts
                    try:
                        resp = self._http.get(probe_url, headers=self._build_headers(), timeout=warmup_timeout)
                        if resp.status_code < 400:
                            if attempt > 1:
                                logger.info(
                                    "Alpha Vantage gateway warm-up recovered after %s attempts (url=%s).",
                                    attempt,
                                    probe_url,
                                )
                            return

                        if resp.status_code not in _RETRYABLE_WARMUP_STATUS_CODES or not should_retry:
                            logger.warning(
                                "Alpha Vantage gateway warm-up probe failed (status=%s, attempt=%s/%s, url=%s).",
                                resp.status_code,
                                attempt,
                                attempts,
                                probe_url,
                            )
                            return
                        logger.info(
                            "Alpha Vantage gateway warm-up probe retrying after status=%s (attempt=%s/%s, sleep=%.1fs).",
                            resp.status_code,
                            attempt,
                            attempts,
                            delay_seconds,
                        )
                    except httpx.TimeoutException as exc:
                        if not should_retry:
                            logger.warning(
                                "Alpha Vantage gateway warm-up probe timed out after %s attempts (url=%s): %s",
                                attempts,
                                probe_url,
                                exc,
                            )
                            return
                        logger.info(
                            "Alpha Vantage gateway warm-up timeout (attempt=%s/%s, sleep=%.1fs): %s",
                            attempt,
                            attempts,
                            delay_seconds,
                            exc,
                        )
                    except Exception as exc:
                        if not should_retry:
                            logger.warning(
                                "Alpha Vantage gateway warm-up probe failed after %s attempts (url=%s): %s: %s",
                                attempts,
                                probe_url,
                                type(exc).__name__,
                                exc,
                            )
                            return
                        logger.info(
                            "Alpha Vantage gateway warm-up transient failure (attempt=%s/%s, sleep=%.1fs): %s: %s",
                            attempt,
                            attempts,
                            delay_seconds,
                            type(exc).__name__,
                            exc,
                        )

                    if delay_seconds > 0.0:
                        time.sleep(delay_seconds)
                    delay_seconds = min(max_delay_seconds, max(delay_seconds * 2.0, 0.1))
            finally:
                self._warmup_attempted = True

    def _request(self, path: str, *, params: Optional[dict[str, Any]] = None) -> httpx.Response:
        self._warm_up_gateway()
        url = f"{self.config.base_url}{path}"
        try:
            resp = self._http.get(url, params=params or {}, headers=self._build_headers())
        except httpx.TimeoutException as exc:
            raise AlphaVantageGatewayError(f"API gateway timeout calling {path}", payload={"path": path}) from exc
        except Exception as exc:
            raise AlphaVantageGatewayError(
                f"API gateway call failed: {type(exc).__name__}: {exc}", payload={"path": path}
            ) from exc

        if resp.status_code < 400:
            return resp

        detail = self._extract_detail(resp)
        payload = {"path": path, "status_code": int(resp.status_code), "detail": detail}

        if resp.status_code in {401, 403}:
            raise AlphaVantageGatewayAuthError(
                "API gateway auth failed.", status_code=resp.status_code, detail=detail, payload=payload
            )
        if resp.status_code == 404:
            raise AlphaVantageGatewayInvalidSymbolError(
                detail or "Symbol not found.", status_code=resp.status_code, detail=detail, payload=payload
            )
        if resp.status_code == 429:
            raise AlphaVantageGatewayThrottleError(
                detail or "Throttled.", status_code=resp.status_code, detail=detail, payload=payload
            )
        if resp.status_code == 503:
            raise AlphaVantageGatewayUnavailableError(
                detail or "Gateway unavailable.", status_code=resp.status_code, detail=detail, payload=payload
            )
        raise AlphaVantageGatewayError(
            f"API gateway error (status={resp.status_code}).",
            status_code=resp.status_code,
            detail=detail,
            payload=payload,
        )

    def get_listing_status_csv(self, *, state: str = "active", date: Optional[str] = None) -> str:
        params: dict[str, Any] = {"state": state}
        if date:
            params["date"] = date
        resp = self._request("/api/providers/alpha-vantage/listing-status", params=params)
        return str(resp.text or "")

    def get_daily_time_series_csv(
        self,
        *,
        symbol: str,
        outputsize: str = "compact",
        adjusted: bool = False,
    ) -> str:
        resp = self._request(
            "/api/providers/alpha-vantage/time-series/daily",
            params={"symbol": symbol, "outputsize": outputsize, "adjusted": "true" if adjusted else "false"},
        )
        return str(resp.text or "")

    def get_earnings(self, *, symbol: str) -> dict[str, Any]:
        resp = self._request("/api/providers/alpha-vantage/earnings", params={"symbol": symbol})
        return resp.json()

    def get_finance_report(self, *, symbol: str, report: str) -> dict[str, Any]:
        resp = self._request(f"/api/providers/alpha-vantage/finance/{report}", params={"symbol": symbol})
        return resp.json()
