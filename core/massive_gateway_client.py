from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)
_MIN_API_GATEWAY_TIMEOUT_SECONDS = 60.0


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


@dataclass(frozen=True)
class MassiveGatewayClientConfig:
    base_url: str
    api_key: Optional[str]
    api_key_header: str
    timeout_seconds: float


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

    @staticmethod
    def from_env() -> "MassiveGatewayClient":
        base_url = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_BASE_URL")) or _strip_or_none(
            os.environ.get("ASSET_ALLOCATION_API_URL")
        )
        if not base_url:
            raise ValueError("ASSET_ALLOCATION_API_BASE_URL is required for Massive ETL via API gateway.")

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

        return MassiveGatewayClient(
            MassiveGatewayClientConfig(
                base_url=str(base_url).rstrip("/"),
                api_key=api_key,
                api_key_header=str(api_key_header),
                timeout_seconds=float(timeout_seconds),
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

    def _request(self, path: str, *, params: Optional[dict[str, Any]] = None) -> httpx.Response:
        url = f"{self.config.base_url}{path}"
        try:
            resp = self._http.get(url, params=params or {}, headers=self._build_headers())
        except httpx.TimeoutException as exc:
            raise MassiveGatewayError(f"API gateway timeout calling {path}", payload={"path": path}) from exc
        except Exception as exc:
            raise MassiveGatewayError(
                f"API gateway call failed: {type(exc).__name__}: {exc}",
                payload={"path": path},
            ) from exc

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

    def get_short_interest(self, *, symbol: str) -> dict[str, Any]:
        resp = self._request("/api/providers/massive/fundamentals/short-interest", params={"symbol": symbol})
        return resp.json()

    def get_short_volume(self, *, symbol: str) -> dict[str, Any]:
        resp = self._request("/api/providers/massive/fundamentals/short-volume", params={"symbol": symbol})
        return resp.json()

    def get_float(self, *, symbol: str, as_of: Optional[str] = None) -> dict[str, Any]:
        params: dict[str, Any] = {"symbol": symbol}
        if as_of:
            params["as_of"] = as_of
        resp = self._request("/api/providers/massive/fundamentals/float", params=params)
        return resp.json()

    def get_finance_report(self, *, symbol: str, report: str) -> dict[str, Any]:
        resp = self._request(f"/api/providers/massive/financials/{report}", params={"symbol": symbol})
        return resp.json()
