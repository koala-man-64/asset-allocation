from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from api.service.dependencies import validate_auth
from api.service.massive_gateway import (
    FinanceReport,
    MassiveAuthError,
    MassiveError,
    MassiveGateway,
    MassiveNotConfiguredError,
    MassiveNotFoundError,
    MassiveRateLimitError,
    MassiveServerError,
    massive_caller_context,
)

logger = logging.getLogger("asset-allocation.api.massive")

router = APIRouter()


def _parse_iso_date(value: Optional[str], *, field_name: str) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}={value!r} (expected YYYY-MM-DD).") from exc
    return parsed.isoformat()


def _get_gateway(request: Request) -> MassiveGateway:
    gateway = getattr(request.app.state, "massive_gateway", None)
    if isinstance(gateway, MassiveGateway):
        return gateway
    raise HTTPException(status_code=500, detail="Massive gateway is not initialized.")


def _handle_massive_error(exc: Exception) -> None:
    if isinstance(exc, MassiveNotConfiguredError):
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if isinstance(exc, MassiveRateLimitError):
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    if isinstance(exc, MassiveNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, (MassiveAuthError, MassiveServerError, MassiveError)):
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=f"Unexpected error: {type(exc).__name__}: {exc}") from exc


def _caller_context(request: Request):
    return massive_caller_context(
        caller_job=request.headers.get("X-Caller-Job"),
        caller_execution=request.headers.get("X-Caller-Execution"),
    )


@router.get("/time-series/daily")
def get_daily_time_series(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g. AAPL)."),
    from_date: Optional[str] = Query(default=None, alias="from", description="Optional start date (YYYY-MM-DD)."),
    to_date: Optional[str] = Query(default=None, alias="to", description="Optional end date (YYYY-MM-DD)."),
    adjusted: bool = Query(default=True, description="Adjusted bars when true."),
    gateway: MassiveGateway = Depends(_get_gateway),
) -> Response:
    validate_auth(request)
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required.")

    parsed_from = _parse_iso_date(from_date, field_name="from")
    parsed_to = _parse_iso_date(to_date, field_name="to")
    if parsed_from and parsed_to and parsed_from > parsed_to:
        raise HTTPException(status_code=400, detail="'from' must be <= 'to'.")

    try:
        with _caller_context(request):
            csv_text = gateway.get_daily_time_series_csv(
                symbol=sym,
                from_date=parsed_from,
                to_date=parsed_to,
                adjusted=bool(adjusted),
            )
    except Exception as exc:
        _handle_massive_error(exc)
        raise

    return Response(content=csv_text, media_type="text/csv", headers={"Cache-Control": "no-store"})


@router.get("/fundamentals/short-interest")
def get_short_interest(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g. AAPL)."),
    settlement_date_gte: Optional[str] = Query(default=None, description="Optional settlement date lower bound (YYYY-MM-DD)."),
    settlement_date_lte: Optional[str] = Query(default=None, description="Optional settlement date upper bound (YYYY-MM-DD)."),
    gateway: MassiveGateway = Depends(_get_gateway),
) -> JSONResponse:
    validate_auth(request)
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required.")
    parsed_settlement_date_gte = _parse_iso_date(settlement_date_gte, field_name="settlement_date_gte")
    parsed_settlement_date_lte = _parse_iso_date(settlement_date_lte, field_name="settlement_date_lte")
    if parsed_settlement_date_gte and parsed_settlement_date_lte and parsed_settlement_date_gte > parsed_settlement_date_lte:
        raise HTTPException(status_code=400, detail="'settlement_date_gte' must be <= 'settlement_date_lte'.")
    try:
        with _caller_context(request):
            query = {}
            if parsed_settlement_date_gte is not None:
                query["settlement_date_gte"] = parsed_settlement_date_gte
            if parsed_settlement_date_lte is not None:
                query["settlement_date_lte"] = parsed_settlement_date_lte
            payload = gateway.get_short_interest(
                symbol=sym,
                **query,
            )
    except Exception as exc:
        _handle_massive_error(exc)
        raise
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.get("/fundamentals/short-volume")
def get_short_volume(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g. AAPL)."),
    date_gte: Optional[str] = Query(default=None, description="Optional trade date lower bound (YYYY-MM-DD)."),
    date_lte: Optional[str] = Query(default=None, description="Optional trade date upper bound (YYYY-MM-DD)."),
    gateway: MassiveGateway = Depends(_get_gateway),
) -> JSONResponse:
    validate_auth(request)
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required.")
    parsed_date_gte = _parse_iso_date(date_gte, field_name="date_gte")
    parsed_date_lte = _parse_iso_date(date_lte, field_name="date_lte")
    if parsed_date_gte and parsed_date_lte and parsed_date_gte > parsed_date_lte:
        raise HTTPException(status_code=400, detail="'date_gte' must be <= 'date_lte'.")
    try:
        with _caller_context(request):
            query = {}
            if parsed_date_gte is not None:
                query["date_gte"] = parsed_date_gte
            if parsed_date_lte is not None:
                query["date_lte"] = parsed_date_lte
            payload = gateway.get_short_volume(
                symbol=sym,
                **query,
            )
    except Exception as exc:
        _handle_massive_error(exc)
        raise
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.get("/fundamentals/float")
def get_float(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g. AAPL)."),
    as_of: Optional[str] = Query(default=None, description="Optional as-of date (YYYY-MM-DD)."),
    gateway: MassiveGateway = Depends(_get_gateway),
) -> JSONResponse:
    validate_auth(request)
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required.")
    parsed_as_of = _parse_iso_date(as_of, field_name="as_of")
    try:
        with _caller_context(request):
            payload = gateway.get_float(symbol=sym, as_of=parsed_as_of)
    except Exception as exc:
        _handle_massive_error(exc)
        raise
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.get("/financials/{report}")
@router.get("/finance/{report}")  # Backwards-compatible alias
def get_finance_report(
    request: Request,
    report: FinanceReport,
    symbol: str = Query(..., description="Ticker symbol (e.g. AAPL)."),
    gateway: MassiveGateway = Depends(_get_gateway),
) -> JSONResponse:
    validate_auth(request)
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required.")
    try:
        with _caller_context(request):
            payload = gateway.get_finance_report(symbol=sym, report=report)
    except Exception as exc:
        _handle_massive_error(exc)
        raise
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})
