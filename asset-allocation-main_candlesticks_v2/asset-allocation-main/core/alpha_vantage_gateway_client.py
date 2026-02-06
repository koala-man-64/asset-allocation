from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class AlphaVantageGatewayClientConfig:
    base_url: str
    api_key: Optional[str]
    api_key_header: str
    timeout_seconds: float


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

    @staticmethod
    def from_env() -> "AlphaVantageGatewayClient":
        base_url = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_BASE_URL")) or _strip_or_none(
            os.environ.get("ASSET_ALLOCATION_API_URL")
        )
        if not base_url:
            raise ValueError("ASSET_ALLOCATION_API_BASE_URL is required for Alpha Vantage ETL via API gateway.")

        api_key = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_KEY")) or _strip_or_none(os.environ.get("API_KEY"))
        api_key_header = (
            _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_KEY_HEADER"))
            or _strip_or_none(os.environ.get("API_KEY_HEADER"))
            or "X-API-Key"
        )

        timeout_seconds = _env_float("ASSET_ALLOCATION_API_TIMEOUT_SECONDS", _env_float("ALPHA_VANTAGE_TIMEOUT_SECONDS", 120.0))

        return AlphaVantageGatewayClient(
            AlphaVantageGatewayClientConfig(
                base_url=str(base_url).rstrip("/"),
                api_key=api_key,
                api_key_header=str(api_key_header),
                timeout_seconds=float(timeout_seconds),
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
            raise AlphaVantageGatewayError(f"API gateway timeout calling {path}", payload={"path": path}) from exc
        except Exception as exc:
            raise AlphaVantageGatewayError(f"API gateway call failed: {type(exc).__name__}: {exc}", payload={"path": path}) from exc

        if resp.status_code < 400:
            return resp

        detail = self._extract_detail(resp)
        payload = {"path": path, "status_code": int(resp.status_code), "detail": detail}

        if resp.status_code in {401, 403}:
            raise AlphaVantageGatewayAuthError("API gateway auth failed.", status_code=resp.status_code, detail=detail, payload=payload)
        if resp.status_code == 404:
            raise AlphaVantageGatewayInvalidSymbolError(detail or "Symbol not found.", status_code=resp.status_code, detail=detail, payload=payload)
        if resp.status_code == 429:
            raise AlphaVantageGatewayThrottleError(detail or "Throttled.", status_code=resp.status_code, detail=detail, payload=payload)
        if resp.status_code == 503:
            raise AlphaVantageGatewayUnavailableError(detail or "Gateway unavailable.", status_code=resp.status_code, detail=detail, payload=payload)
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

