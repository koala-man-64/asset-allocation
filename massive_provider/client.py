from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Optional

import httpx

from massive_provider.config import MassiveConfig
from massive_provider.errors import (
    MassiveAuthError,
    MassiveError,
    MassiveNotConfiguredError,
    MassiveNotFoundError,
    MassiveRateLimitError,
    MassiveServerError,
)
from massive_provider.utils import filter_none, to_jsonable

logger = logging.getLogger(__name__)


try:  # Optional dependency
    from massive import RESTClient as _SDKRestClient  # type: ignore
except Exception:  # pragma: no cover
    _SDKRestClient = None


@dataclass(frozen=True)
class MassiveHTTPResponse:
    """Normalized Massive REST response payload.

    Massive endpoints are not fully uniform across products and versions.
    This container provides a consistent shape for downstream code.
    """

    status_code: int
    url: str
    payload: Any


class MassiveClient:
    """Project-specific façade over Massive REST + optional official SDK.

    This client supports:
      * OHLCV bars (via ``/v2/aggs`` or ``RESTClient.list_aggs``)
      * Fundamentals: short interest, short volume, float
      * Financial statements: income statement, cash flow, balance sheet
      * Ratios

    The official Massive SDK (``pip install massive``) is *optional* here.
    If installed and enabled, it is used for high-volume OHLCV pagination.
    """

    def __init__(
        self,
        config: MassiveConfig,
        *,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        if not config.api_key:
            raise MassiveNotConfiguredError("MASSIVE_API_KEY is not configured.")

        self.config = config
        self._owns_http = http_client is None
        self._http = http_client or httpx.Client(
            timeout=httpx.Timeout(config.timeout_seconds),
            base_url=str(config.base_url).rstrip("/"),
            headers={"Authorization": f"Bearer {config.api_key}"},
            trust_env=False,
        )

        self._sdk = None
        if bool(config.prefer_official_sdk) and _SDKRestClient is not None:
            try:
                # The SDK supports pagination control in the constructor.
                self._sdk = _SDKRestClient(
                    api_key=config.api_key,
                    base_url=str(config.base_url).rstrip("/"),
                    pagination=True,
                )
            except Exception:
                self._sdk = None

    def close(self) -> None:
        if self._owns_http:
            try:
                self._http.close()
            except Exception:
                pass

        if self._sdk is not None:
            try:
                self._sdk.close()
            except Exception:
                pass

    def __enter__(self) -> "MassiveClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------
    # Low-level HTTP
    # ------------------------------

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
            # Some Massive endpoints return {"error": "..."}
            err = payload.get("error")
            if isinstance(err, str) and err.strip():
                return err.strip()
            return json.dumps(payload, ensure_ascii=False)
        if isinstance(payload, str) and payload.strip():
            return payload.strip()
        return response.reason_phrase

    def _request_json(self, path_or_url: str, *, params: Optional[dict[str, Any]] = None) -> MassiveHTTPResponse:
        """GET JSON from Massive.

        ``path_or_url`` can be a relative API path ("/v2/...") or an absolute
        next_url returned by Massive.
        """

        url = str(path_or_url)
        try:
            resp = self._http.get(url, params=params or {})
        except httpx.TimeoutException as exc:
            raise MassiveError(f"Massive timeout calling {path_or_url}", payload={"path": path_or_url}) from exc
        except Exception as exc:
            raise MassiveError(
                f"Massive call failed: {type(exc).__name__}: {exc}",
                payload={"path": path_or_url},
            ) from exc

        if resp.status_code < 400:
            try:
                payload = resp.json()
            except Exception:
                payload = resp.text
            return MassiveHTTPResponse(status_code=int(resp.status_code), url=str(resp.url), payload=payload)

        detail = self._extract_detail(resp)
        payload = {"path": path_or_url, "status_code": int(resp.status_code), "detail": detail}

        if resp.status_code in {401, 403}:
            raise MassiveAuthError("Massive auth failed.", status_code=resp.status_code, detail=detail, payload=payload)
        if resp.status_code == 404:
            raise MassiveNotFoundError(detail or "Not found.", status_code=resp.status_code, detail=detail, payload=payload)
        if resp.status_code == 429:
            raise MassiveRateLimitError(detail or "Rate limited.", status_code=resp.status_code, detail=detail, payload=payload)
        if 500 <= resp.status_code <= 599:
            raise MassiveServerError(detail or "Massive server error.", status_code=resp.status_code, detail=detail, payload=payload)

        raise MassiveError(
            f"Massive error (status={resp.status_code}).",
            status_code=resp.status_code,
            detail=detail,
            payload=payload,
        )

    # ------------------------------
    # OHLCV
    # ------------------------------

    def list_ohlcv(
        self,
        *,
        ticker: str,
        multiplier: int = 1,
        timespan: str = "day",
        from_: str,
        to: str,
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 50000,
        pagination: bool = True,
    ) -> list[dict[str, Any]]:
        """Return OHLCV bars for a ticker.

        When the official SDK is installed, this method uses ``RESTClient.list_aggs``.
        Otherwise it calls ``/v2/aggs`` directly.
        """

        sym = str(ticker or "").strip().upper()
        if not sym:
            raise ValueError("ticker is required")

        if self._sdk is not None:
            out: list[dict[str, Any]] = []
            for bar in self._sdk.list_aggs(
                ticker=sym,
                multiplier=int(multiplier),
                timespan=str(timespan),
                from_=str(from_),
                to=str(to),
                limit=int(limit),
            ):
                out.append(to_jsonable(bar))
            return out

        # Direct REST fallback
        path = f"/v2/aggs/ticker/{sym}/range/{int(multiplier)}/{str(timespan)}/{str(from_)}/{str(to)}"
        params = {
            "adjusted": "true" if adjusted else "false",
            "sort": str(sort),
            "limit": int(limit),
        }

        bars: list[dict[str, Any]] = []
        next_url: Optional[str] = None

        resp = self._request_json(path, params=params)
        payload = resp.payload
        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list):
                bars.extend([to_jsonable(r) for r in results])
            next_url = payload.get("next_url") if pagination else None
        else:
            raise MassiveError("Unexpected Massive aggs response.", payload={"url": resp.url})

        while pagination and next_url:
            resp = self._request_json(str(next_url))
            payload = resp.payload
            if not isinstance(payload, dict):
                break
            results = payload.get("results")
            if isinstance(results, list):
                bars.extend([to_jsonable(r) for r in results])
            next_url = payload.get("next_url")

        return bars

    # ------------------------------
    # Fundamentals & Financials
    # ------------------------------

    def get_short_interest(self, *, ticker: str, params: Optional[dict[str, Any]] = None) -> Any:
        """Short interest.

        REST endpoint: ``GET /stocks/v1/short-interest``.
        """

        q = {"ticker": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_json("/stocks/v1/short-interest", params=filter_none(q)).payload

    def get_short_volume(self, *, ticker: str, params: Optional[dict[str, Any]] = None) -> Any:
        """Short volume.

        REST endpoint: ``GET /stocks/v1/short-volume``.
        """

        q = {"ticker": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_json("/stocks/v1/short-volume", params=filter_none(q)).payload

    def get_float(self, *, ticker: str, as_of: Optional[str] = None, params: Optional[dict[str, Any]] = None) -> Any:
        """Company float (experimental).

        REST endpoint: ``GET /stocks/vX/float``.
        """

        q: dict[str, Any] = {"ticker": str(ticker).strip().upper(), "as_of": as_of}
        if params:
            q.update(params)
        return self._request_json("/stocks/vX/float", params=filter_none(q)).payload

    def get_income_statement(self, *, ticker: str, params: Optional[dict[str, Any]] = None) -> Any:
        """Income statements.

        REST endpoint: ``GET /stocks/financials/v1/income-statements``.
        """

        q = {"ticker": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_json("/stocks/financials/v1/income-statements", params=filter_none(q)).payload

    def get_cash_flow_statement(self, *, ticker: str, params: Optional[dict[str, Any]] = None) -> Any:
        """Cash-flow statements.

        REST endpoint: ``GET /stocks/financials/v1/cash-flow-statements``.
        """

        q = {"ticker": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_json("/stocks/financials/v1/cash-flow-statements", params=filter_none(q)).payload

    def get_balance_sheet(self, *, ticker: str, params: Optional[dict[str, Any]] = None) -> Any:
        """Balance sheets.

        REST endpoint: ``GET /stocks/financials/v1/balance-sheets``.
        """

        q = {"ticker": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_json("/stocks/financials/v1/balance-sheets", params=filter_none(q)).payload

    def get_ratios(self, *, ticker: str, params: Optional[dict[str, Any]] = None) -> Any:
        """Financial ratios.

        REST endpoint: ``GET /stocks/financials/v1/ratios``.
        """

        q = {"ticker": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_json("/stocks/financials/v1/ratios", params=filter_none(q)).payload
