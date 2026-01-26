from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request

from api.service.dependencies import get_settings, validate_auth
from core.delta_core import load_delta
from core.pipeline import DataPaths
from core.postgres import PostgresError, connect
from ..data_service import DataService

router = APIRouter()


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_postgres_dsn(request: Request) -> Optional[str]:
    """
    Prefer POSTGRES_DSN when present (core scripts), otherwise fall back to BACKTEST_POSTGRES_DSN.

    Normalizes SQLAlchemy-style DSNs (postgresql+asyncpg://...) to psycopg-friendly (postgresql://...).
    """
    raw = os.environ.get("POSTGRES_DSN")
    dsn = _strip_or_none(raw) or _strip_or_none(get_settings(request).postgres_dsn)
    if not dsn:
        return None
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn.removeprefix("postgresql+asyncpg://")
    return dsn


def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date={value!r} (expected YYYY-MM-DD).") from exc


def _first_present(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    existing = {str(c): True for c in columns}
    for name in candidates:
        if name in existing:
            return name
    return None


def _safe_numeric(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return float(int(value))
        return float(value)
    except (TypeError, ValueError):
        return None


def _find_latest_market_date(
    *,
    gold_container: str,
    gold_path: str,
    max_lookback_days: int = 14,
) -> Optional[date]:
    today = datetime.utcnow().date()
    lookback = max(1, min(int(max_lookback_days), 60))
    for days_ago in range(0, lookback):
        candidate = today - timedelta(days=days_ago)
        candidate_dt = datetime(candidate.year, candidate.month, candidate.day)
        ym = candidate.strftime("%Y-%m")

        df = load_delta(
            gold_container,
            gold_path,
            columns=["date", "symbol"],
            filters=[("year_month", "=", ym), ("date", "=", candidate_dt)],
        )
        if df is not None and not df.empty:
            return candidate
    return None


def _query_symbols(conn, *, q: Optional[str] = None) -> pd.DataFrame:  # type: ignore[no-untyped-def]
    query = """
        SELECT symbol, name, sector, industry, country, is_optionable
        FROM core.symbols
        ORDER BY symbol
    """
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return df

    df["symbol"] = df["symbol"].astype(str).str.upper()
    df = df[~df["symbol"].str.contains(r"\.", na=False)]
    if q:
        needle = str(q).strip().upper()
        if needle:
            name_col = "name" if "name" in df.columns else None
            if name_col:
                mask = df["symbol"].str.contains(needle, na=False) | df[name_col].astype(str).str.upper().str.contains(needle, na=False)
            else:
                mask = df["symbol"].str.contains(needle, na=False)
            df = df[mask]
    return df.reset_index(drop=True)


@router.get("/symbols")
def list_symbols(
    request: Request,
    q: Optional[str] = Query(default=None, description="Search string (symbol/name)"),
    limit: int = Query(default=5000, ge=1, le=20000),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    """
    Returns the symbol universe from Postgres (`core.symbols`).
    """
    validate_auth(request)
    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN or BACKTEST_POSTGRES_DSN).")

    try:
        with connect(dsn) as conn:
            df = _query_symbols(conn, q=q)
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Symbols unavailable: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Symbols query failed: {exc}") from exc

    total = int(len(df))
    page = df.iloc[int(offset) : int(offset) + int(limit)].copy()
    page = page.rename(columns={"is_optionable": "isOptionable"})
    page = page.where(pd.notnull(page), None)

    return {
        "total": total,
        "limit": int(limit),
        "offset": int(offset),
        "symbols": page.to_dict(orient="records"),
    }


@router.get("/screener")
def get_stock_screener(
    request: Request,
    q: Optional[str] = Query(default=None, description="Search string (symbol/name)"),
    limit: int = Query(default=250, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
    as_of: Optional[str] = Query(default=None, description="As-of date (YYYY-MM-DD). Defaults to latest available."),
    sort: str = Query(
        default="volume",
        description="Sort key: symbol|close|volume|return_1d|return_5d|vol_20d|drawdown_1y|atr_14d|compression_score",
    ),
    direction: str = Query(default="desc", description="Sort direction: asc|desc"),
) -> Dict[str, Any]:
    """
    Daily stock screener snapshot combining:
      - Silver: latest OHLCV for the as-of date.
      - Gold: engineered features for the as-of date.
      - Postgres: symbol metadata (core.symbols).
    """
    validate_auth(request)

    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN or BACKTEST_POSTGRES_DSN).")

    gold_container = os.environ.get("AZURE_CONTAINER_GOLD") or os.environ.get("AZURE_CONTAINER_MARKET") or ""
    silver_container = os.environ.get("AZURE_CONTAINER_SILVER") or ""
    gold_container = gold_container.strip()
    silver_container = silver_container.strip()
    if not (gold_container and silver_container):
        raise HTTPException(status_code=503, detail="Storage containers are not configured (AZURE_CONTAINER_SILVER/AZURE_CONTAINER_GOLD).")

    gold_path = DataPaths.get_gold_features_by_date_path()
    silver_path = DataPaths.get_market_data_by_date_path()

    requested = _parse_iso_date(as_of)
    resolved_date = requested or _find_latest_market_date(gold_container=gold_container, gold_path=gold_path)
    if resolved_date is None:
        raise HTTPException(status_code=503, detail="No Gold market feature data found for recent dates.")

    ym = resolved_date.strftime("%Y-%m")
    resolved_dt = datetime(resolved_date.year, resolved_date.month, resolved_date.day)

    try:
        with connect(dsn) as conn:
            symbols_df = _query_symbols(conn, q=q)
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Symbols unavailable: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Symbols query failed: {exc}") from exc

    gold_cols = [
        "return_1d",
        "return_5d",
        "vol_20d",
        "drawdown_1y",
        "atr_14d",
        "gap_atr",
        "sma_50d",
        "sma_200d",
        "trend_50_200",
        "above_sma_50",
        "bb_width_20d",
        "compression_score",
        "volume_z_20d",
        "volume_pct_rank_252d",
    ]
    gold_df = load_delta(
        gold_container,
        gold_path,
        columns=["date", "year_month", "symbol", *gold_cols],
        filters=[("year_month", "=", ym), ("date", "=", resolved_dt)],
    )

    silver_df = load_delta(
        silver_container,
        silver_path,
        columns=["year_month", "Date", "Symbol", "Open", "High", "Low", "Close", "Volume"],
        filters=[("year_month", "=", ym), ("Date", "=", resolved_dt)],
    )

    if gold_df is None or gold_df.empty:
        raise HTTPException(status_code=503, detail=f"Gold market features unavailable for {resolved_date.isoformat()}.")
    if silver_df is None or silver_df.empty:
        raise HTTPException(status_code=503, detail=f"Silver market data unavailable for {resolved_date.isoformat()}.")

    gold_df = gold_df.copy()
    gold_df["symbol"] = gold_df["symbol"].astype(str).str.upper()

    silver_df = silver_df.copy()
    symbol_col = _first_present(silver_df.columns.tolist(), ["Symbol", "symbol"])
    date_col = _first_present(silver_df.columns.tolist(), ["Date", "date"])
    if not symbol_col or not date_col:
        raise HTTPException(status_code=500, detail="Silver market-data-by-date schema missing Symbol/Date columns.")

    silver_df = silver_df.rename(
        columns={
            symbol_col: "symbol",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    silver_df["symbol"] = silver_df["symbol"].astype(str).str.upper()

    merged = symbols_df.merge(silver_df[["symbol", "open", "high", "low", "close", "volume"]], on="symbol", how="left")
    merged = merged.merge(gold_df[["symbol", *gold_cols]], on="symbol", how="left", suffixes=("", "_gold"))

    merged["has_silver"] = merged["close"].notna().astype(int)
    merged["has_gold"] = merged["return_1d"].notna().astype(int)

    sort_key = str(sort or "").strip()
    allowed_sorts = {
        "symbol": "symbol",
        "close": "close",
        "volume": "volume",
        "return_1d": "return_1d",
        "return_5d": "return_5d",
        "vol_20d": "vol_20d",
        "drawdown_1y": "drawdown_1y",
        "atr_14d": "atr_14d",
        "compression_score": "compression_score",
    }
    col = allowed_sorts.get(sort_key, "volume")
    ascending = str(direction or "").strip().lower() == "asc"

    merged = merged.sort_values(by=[col, "symbol"], ascending=[ascending, True], na_position="last")

    total = int(len(merged))
    page = merged.iloc[int(offset) : int(offset) + int(limit)].copy()

    # JSON-safe conversion.
    page = page.rename(
        columns={
            "is_optionable": "isOptionable",
            "return_1d": "return1d",
            "return_5d": "return5d",
            "vol_20d": "vol20d",
            "drawdown_1y": "drawdown1y",
            "atr_14d": "atr14d",
            "gap_atr": "gapAtr",
            "sma_50d": "sma50d",
            "sma_200d": "sma200d",
            "trend_50_200": "trend50_200",
            "above_sma_50": "aboveSma50",
            "bb_width_20d": "bbWidth20d",
            "compression_score": "compressionScore",
            "volume_z_20d": "volumeZ20d",
            "volume_pct_rank_252d": "volumePctRank252d",
            "has_silver": "hasSilver",
            "has_gold": "hasGold",
        }
    )
    page = page.where(pd.notnull(page), None)

    return {
        "asOf": resolved_date.isoformat(),
        "total": total,
        "limit": int(limit),
        "offset": int(offset),
        "rows": page.to_dict(orient="records"),
    }

@router.get("/{layer}/{domain}")
def get_data_generic(
    layer: str,
    domain: str,
    request: Request,
    ticker: Optional[str] = None,
):
    """
    Generic endpoint for retrieving data from Silver/Gold layers.
    Delegates to DataService for logic.
    """
    validate_auth(request)
    if layer not in ["silver", "gold"]:
        raise HTTPException(status_code=400, detail="Layer must be 'silver' or 'gold'. Use /ranking for platinum.")
    
    try:
        return DataService.get_data(layer, domain, ticker)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{layer}/finance/{sub_domain}")
def get_finance_data(
    layer: str,
    sub_domain: str,
    request: Request,
    ticker: str = Query(..., description="Ticker is required for finance reports"),
):
    """
    Specialized endpoint for Finance data.
    """
    validate_auth(request)
    if layer not in ["silver", "gold"]:
         raise HTTPException(status_code=400, detail="Layer must be 'silver' or 'gold'")

    try:
        return DataService.get_finance_data(layer, sub_domain, ticker)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
