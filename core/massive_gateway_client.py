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
_MIN_API_GATEWAY_TIMEOUT_SECONDS = 60.0
_DEFAULT_API_WARMUP_ENABLED = True
_DEFAULT_API_WARMUP_MAX_ATTEMPTS = 3
_DEFAULT_API_WARMUP_BASE_DELAY_SECONDS = 1.0
_DEFAULT_API_WARMUP_MAX_DELAY_SECONDS = 8.0
_DEFAULT_API_WARMUP_PROBE_TIMEOUT_SECONDS = 5.0
_DEFAULT_API_READINESS_ENABLED = True
_DEFAULT_API_READINESS_MAX_ATTEMPTS = 6
_DEFAULT_API_READINESS_SLEEP_SECONDS = 10.0
_API_WARMUP_PROBE_PATH = "/healthz"
_RETRYABLE_WARMUP_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


class MassiveGatewayError(RuntimeError):
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


class MassiveGatewayAuthError(MassiveGatewayError):
    pass


class MassiveGatewayRateLimitError(MassiveGatewayError):
    pass


class MassiveGatewayNotFoundError(MassiveGatewayError):
    pass


class MassiveGatewayUnavailableError(MassiveGatewayError):
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
class MassiveGatewayClientConfig:
    base_url: str
    api_key: Optional[str]
    api_key_header: str
    timeout_seconds: float
    fallback_base_url: Optional[str] = None
    warmup_enabled: bool = _DEFAULT_API_WARMUP_ENABLED
    warmup_max_attempts: int = _DEFAULT_API_WARMUP_MAX_ATTEMPTS
    warmup_base_delay_seconds: float = _DEFAULT_API_WARMUP_BASE_DELAY_SECONDS
    warmup_max_delay_seconds: float = _DEFAULT_API_WARMUP_MAX_DELAY_SECONDS
    warmup_probe_timeout_seconds: float = _DEFAULT_API_WARMUP_PROBE_TIMEOUT_SECONDS
    readiness_enabled: bool = _DEFAULT_API_READINESS_ENABLED
    readiness_max_attempts: int = _DEFAULT_API_READINESS_MAX_ATTEMPTS
    readiness_sleep_seconds: float = _DEFAULT_API_READINESS_SLEEP_SECONDS


class MassiveGatewayClient:
    """
    Minimal sync client for the API-hosted Massive gateway.

    ETL jobs should use this instead of calling Massive directly.
    """

    def __init__(
        self,
        config: MassiveGatewayClientConfig,
        *,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        self.config = config
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client(timeout=httpx.Timeout(config.timeout_seconds), trust_env=False)
        self._active_base_url = str(config.base_url).rstrip("/")
        self._fallback_base_url = _strip_or_none(config.fallback_base_url)
        if self._fallback_base_url:
            self._fallback_base_url = str(self._fallback_base_url).rstrip("/")
        if self._fallback_base_url == self._active_base_url:
            self._fallback_base_url = None
        self._warmup_lock = threading.Lock()
        self._warmup_attempted = False
        self._warmup_succeeded = not config.warmup_enabled
        self._readiness_lock = threading.Lock()
        self._readiness_attempted = False
        self._readiness_succeeded = not config.readiness_enabled

    @staticmethod
    def from_env() -> "MassiveGatewayClient":
        base_url = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_BASE_URL")) or _strip_or_none(
            os.environ.get("ASSET_ALLOCATION_API_URL")
        )
        if not base_url:
            raise ValueError("ASSET_ALLOCATION_API_BASE_URL is required for Massive ETL via API gateway.")
        base_url = str(base_url).rstrip("/")

        fallback_base_url = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_FALLBACK_BASE_URL"))
        if fallback_base_url:
            fallback_base_url = str(fallback_base_url).rstrip("/")
            if fallback_base_url == base_url:
                fallback_base_url = None

        api_key = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_KEY")) or _strip_or_none(os.environ.get("API_KEY"))
        api_key_header = (
            _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_KEY_HEADER"))
            or _strip_or_none(os.environ.get("API_KEY_HEADER"))
            or "X-API-Key"
        )

        timeout_seconds = _env_float(
            "ASSET_ALLOCATION_API_TIMEOUT_SECONDS",
            _env_float("MASSIVE_TIMEOUT_SECONDS", _MIN_API_GATEWAY_TIMEOUT_SECONDS),
        )
        if timeout_seconds < _MIN_API_GATEWAY_TIMEOUT_SECONDS:
            logger.warning(
                "ASSET_ALLOCATION_API_TIMEOUT_SECONDS=%s is too low for Massive market requests; using %s.",
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
        readiness_enabled = _env_bool("ASSET_ALLOCATION_API_READINESS_ENABLED", _DEFAULT_API_READINESS_ENABLED)
        readiness_max_attempts = max(
            1,
            _env_int("ASSET_ALLOCATION_API_READINESS_ATTEMPTS", _DEFAULT_API_READINESS_MAX_ATTEMPTS),
        )
        readiness_sleep_seconds = max(
            0.0,
            _env_float("ASSET_ALLOCATION_API_READINESS_SLEEP_SECONDS", _DEFAULT_API_READINESS_SLEEP_SECONDS),
        )

        return MassiveGatewayClient(
            MassiveGatewayClientConfig(
                base_url=base_url,
                fallback_base_url=fallback_base_url,
                api_key=api_key,
                api_key_header=str(api_key_header),
                timeout_seconds=float(timeout_seconds),
                warmup_enabled=warmup_enabled,
                warmup_max_attempts=warmup_max_attempts,
                warmup_base_delay_seconds=warmup_base_delay_seconds,
                warmup_max_delay_seconds=warmup_max_delay_seconds,
                warmup_probe_timeout_seconds=warmup_probe_timeout_seconds,
                readiness_enabled=readiness_enabled,
                readiness_max_attempts=readiness_max_attempts,
                readiness_sleep_seconds=readiness_sleep_seconds,
            )
        )

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def __enter__(self) -> "MassiveGatewayClient":
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

    def _current_base_url(self) -> str:
        current = _strip_or_none(self._active_base_url)
        return str(current or self.config.base_url).rstrip("/")

    def _warmup_probe_url(self) -> str:
        return f"{self._current_base_url()}{_API_WARMUP_PROBE_PATH}"

    @staticmethod
    def _is_connectivity_exception(exc: Exception) -> bool:
        if isinstance(exc, httpx.ConnectError):
            return True
        if isinstance(exc, httpx.RequestError):
            lowered = str(exc).strip().lower()
            markers = (
                "connection refused",
                "name or service not known",
                "temporary failure in name resolution",
                "nodename nor servname provided",
                "failed to establish a new connection",
            )
            return any(marker in lowered for marker in markers)
        return False

    def _activate_fallback_base_url(self, *, reason: str) -> bool:
        fallback = _strip_or_none(self._fallback_base_url)
        if not fallback:
            return False
        current = self._current_base_url()
        if fallback == current:
            return False
        self._active_base_url = str(fallback).rstrip("/")
        logger.warning(
            "Massive gateway switched to fallback base URL (from=%s, to=%s, reason=%s).",
            current,
            self._active_base_url,
            reason,
        )
        return True

    def _warm_up_gateway(self) -> bool:
        if not self.config.warmup_enabled:
            return True
        if self._warmup_attempted:
            return self._warmup_succeeded

        with self._warmup_lock:
            if not self.config.warmup_enabled:
                self._warmup_succeeded = True
                return True
            if self._warmup_attempted:
                return self._warmup_succeeded
            warmup_succeeded = False
            try:
                delay_seconds = max(0.0, float(self.config.warmup_base_delay_seconds))
                max_delay_seconds = max(delay_seconds, float(self.config.warmup_max_delay_seconds))
                attempts = max(1, int(self.config.warmup_max_attempts))
                probe_timeout = min(float(self.config.timeout_seconds), float(self.config.warmup_probe_timeout_seconds))
                warmup_timeout = httpx.Timeout(probe_timeout)

                for attempt in range(1, attempts + 1):
                    should_retry = attempt < attempts
                    probe_url = self._warmup_probe_url()
                    try:
                        resp = self._http.get(probe_url, headers=self._build_headers(), timeout=warmup_timeout)
                        if resp.status_code < 400:
                            warmup_succeeded = True
                            if attempt > 1:
                                logger.info(
                                    "Massive gateway warm-up recovered after %s attempts (url=%s).",
                                    attempt,
                                    probe_url,
                                )
                            return True

                        if resp.status_code not in _RETRYABLE_WARMUP_STATUS_CODES or not should_retry:
                            logger.warning(
                                "Massive gateway warm-up probe failed (status=%s, attempt=%s/%s, url=%s).",
                                resp.status_code,
                                attempt,
                                attempts,
                                probe_url,
                            )
                            return False
                        logger.info(
                            "Massive gateway warm-up probe retrying after status=%s (attempt=%s/%s, sleep=%.1fs).",
                            resp.status_code,
                            attempt,
                            attempts,
                            delay_seconds,
                        )
                    except httpx.TimeoutException as exc:
                        if should_retry:
                            self._activate_fallback_base_url(reason=f"timeout: {exc}")
                        if not should_retry:
                            logger.warning(
                                "Massive gateway warm-up probe timed out after %s attempts (url=%s): %s",
                                attempts,
                                probe_url,
                                exc,
                            )
                            return False
                        logger.info(
                            "Massive gateway warm-up timeout (attempt=%s/%s, sleep=%.1fs): %s",
                            attempt,
                            attempts,
                            delay_seconds,
                            exc,
                        )
                    except Exception as exc:
                        if should_retry and self._is_connectivity_exception(exc):
                            self._activate_fallback_base_url(reason=f"{type(exc).__name__}: {exc}")
                        if not should_retry:
                            logger.warning(
                                "Massive gateway warm-up probe failed after %s attempts (url=%s): %s: %s",
                                attempts,
                                probe_url,
                                type(exc).__name__,
                                exc,
                            )
                            return False
                        logger.info(
                            "Massive gateway warm-up transient failure (attempt=%s/%s, sleep=%.1fs): %s: %s",
                            attempt,
                            attempts,
                            delay_seconds,
                            type(exc).__name__,
                            exc,
                        )

                    if delay_seconds > 0.0:
                        time.sleep(delay_seconds)
                    delay_seconds = min(max_delay_seconds, max(delay_seconds * 2.0, 0.1))
                return warmup_succeeded
            finally:
                self._warmup_attempted = True
                self._warmup_succeeded = warmup_succeeded

    def warm_up_gateway(self, *, force: bool = False) -> bool:
        if force:
            with self._warmup_lock:
                self._warmup_attempted = False
                self._warmup_succeeded = not self.config.warmup_enabled
        return self._warm_up_gateway()

    def _ensure_gateway_ready(self) -> bool:
        if not self.config.readiness_enabled:
            return self._warm_up_gateway()
        if self._readiness_attempted:
            return self._readiness_succeeded

        with self._readiness_lock:
            if self._readiness_attempted:
                return self._readiness_succeeded

            attempts = max(1, int(self.config.readiness_max_attempts))
            pause = max(0.0, float(self.config.readiness_sleep_seconds))
            ready = False

            for attempt in range(1, attempts + 1):
                ready = self.warm_up_gateway(force=attempt > 1)
                if ready:
                    if attempt > 1:
                        logger.info(
                            "Massive gateway readiness recovered after %s attempts (url=%s).",
                            attempt,
                            self._warmup_probe_url(),
                        )
                    break

                if attempt >= attempts:
                    logger.warning(
                        "Massive gateway readiness failed after %s attempts (url=%s).",
                        attempts,
                        self._warmup_probe_url(),
                    )
                    break

                logger.info(
                    "Massive gateway readiness retrying (attempt=%s/%s, sleep=%.1fs).",
                    attempt,
                    attempts,
                    pause,
                )
                if pause > 0.0:
                    time.sleep(pause)

            self._readiness_attempted = True
            self._readiness_succeeded = ready
            return ready

    def _request(self, path: str, *, params: Optional[dict[str, Any]] = None) -> httpx.Response:
        if not self._ensure_gateway_ready():
            raise MassiveGatewayUnavailableError(
                "API gateway readiness check failed.",
                status_code=503,
                detail="Gateway health probe did not become ready.",
                payload={"path": path, "probe_path": _API_WARMUP_PROBE_PATH},
            )
        request_max_attempts = 2
        resp: Optional[httpx.Response] = None
        for request_attempt in range(1, request_max_attempts + 1):
            url = f"{self._current_base_url()}{path}"
            try:
                resp = self._http.get(url, params=params or {}, headers=self._build_headers())
                break
            except httpx.TimeoutException as exc:
                switched = request_attempt < request_max_attempts and self._activate_fallback_base_url(
                    reason=f"request timeout: {exc}"
                )
                if switched:
                    logger.warning("Massive gateway retrying request via fallback base URL (path=%s).", path)
                    continue
                raise MassiveGatewayError(f"API gateway timeout calling {path}", payload={"path": path}) from exc
            except Exception as exc:
                switched = (
                    request_attempt < request_max_attempts
                    and self._is_connectivity_exception(exc)
                    and self._activate_fallback_base_url(reason=f"request {type(exc).__name__}: {exc}")
                )
                if switched:
                    logger.warning(
                        "Massive gateway retrying request via fallback base URL after connectivity failure (path=%s).",
                        path,
                    )
                    continue
                raise MassiveGatewayError(
                    f"API gateway call failed: {type(exc).__name__}: {exc}",
                    payload={"path": path},
                ) from exc

        if resp is None:
            raise MassiveGatewayError("API gateway call failed before receiving a response.", payload={"path": path})

        if resp.status_code < 400:
            return resp

        detail = self._extract_detail(resp)
        payload = {"path": path, "status_code": int(resp.status_code), "detail": detail}

        if resp.status_code in {401, 403}:
            raise MassiveGatewayAuthError(
                "API gateway auth failed.",
                status_code=resp.status_code,
                detail=detail,
                payload=payload,
            )
        if resp.status_code == 404:
            raise MassiveGatewayNotFoundError(
                detail or "Not found.",
                status_code=resp.status_code,
                detail=detail,
                payload=payload,
            )
        if resp.status_code == 429:
            raise MassiveGatewayRateLimitError(
                detail or "Rate limited.",
                status_code=resp.status_code,
                detail=detail,
                payload=payload,
            )
        if resp.status_code == 503:
            raise MassiveGatewayUnavailableError(
                detail or "Gateway unavailable.",
                status_code=resp.status_code,
                detail=detail,
                payload=payload,
            )
        raise MassiveGatewayError(
            f"API gateway error (status={resp.status_code}).",
            status_code=resp.status_code,
            detail=detail,
            payload=payload,
        )

    def get_daily_time_series_csv(
        self,
        *,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        adjusted: bool = True,
    ) -> str:
        params: dict[str, Any] = {"symbol": symbol, "adjusted": "true" if adjusted else "false"}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        resp = self._request("/api/providers/massive/time-series/daily", params=params)
        return str(resp.text or "")

    def get_unified_snapshot(
        self,
        *,
        symbols: list[str],
        asset_type: str = "stocks",
    ) -> dict[str, Any]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in symbols:
            symbol = str(raw or "").strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
        if not normalized:
            raise ValueError("symbols is required.")

        params: dict[str, Any] = {"symbols": ",".join(normalized)}
        type_filter = str(asset_type or "").strip()
        if type_filter:
            params["type"] = type_filter
        resp = self._request("/api/providers/massive/snapshot", params=params)
        return resp.json()

    def get_short_interest(
        self,
        *,
        symbol: str,
        settlement_date_gte: Optional[str] = None,
        settlement_date_lte: Optional[str] = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"symbol": symbol}
        if settlement_date_gte:
            params["settlement_date.gte"] = settlement_date_gte
        if settlement_date_lte:
            params["settlement_date.lte"] = settlement_date_lte
        resp = self._request("/api/providers/massive/fundamentals/short-interest", params=params)
        return resp.json()

    def get_short_volume(
        self,
        *,
        symbol: str,
        date_gte: Optional[str] = None,
        date_lte: Optional[str] = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"symbol": symbol}
        if date_gte:
            params["date.gte"] = date_gte
        if date_lte:
            params["date.lte"] = date_lte
        resp = self._request("/api/providers/massive/fundamentals/short-volume", params=params)
        return resp.json()

    def get_float(self, *, symbol: str) -> dict[str, Any]:
        params = {"symbol": symbol}
        resp = self._request("/api/providers/massive/fundamentals/float", params=params)
        return resp.json()

    def get_finance_report(self, *, symbol: str, report: str) -> dict[str, Any]:
        resp = self._request(f"/api/providers/massive/financials/{report}", params={"symbol": symbol})
        return resp.json()
