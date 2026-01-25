from datetime import date
from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from api import dependencies as deps
from api.service.dependencies import get_settings, validate_auth
from core.postgres import PostgresError, connect

router = APIRouter()

@router.get("/strategies")
def get_strategies(request: Request):
    """
    Retrieves strategy performance data (Platinum Layer).
    """
    validate_auth(request)
    container = deps.resolve_container("platinum")
    # Path logic for strategies? 
    # Usually 'strategies' or similar in Ranking container.
    # Assuming 'strategies' table exists or similar path.
    # If not defined in pipeline.py, we might need to assume or create it.
    path = "strategies" 
    
    try:
        dt = deps.get_delta_table(container, path)
        df = dt.to_pandas()
        return df.to_dict(orient="records")
    except Exception as e:
        # Fallback for now if table doesn't exist
        return {"error": "Strategies table not found", "details": str(e)}


@router.get("/signals")
def get_signals(
    request: Request,
    signal_date: Optional[str] = Query(default=None, alias="date"),
    limit: int = Query(default=500, ge=1, le=5000),
) -> List[dict[str, Any]]:
    """
    Returns the most recent ranking signals from Postgres (or a specified date).

    Backed by `ranking.ranking_signal` populated by the ranking job.
    """
    validate_auth(request)
    settings = get_settings(request)
    dsn = (settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (BACKTEST_POSTGRES_DSN).")

    resolved_date: Optional[date] = None
    if signal_date:
        try:
            resolved_date = date.fromisoformat(signal_date)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid date={signal_date!r} (expected YYYY-MM-DD).") from exc

    try:
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                if resolved_date is None:
                    cur.execute("SELECT MAX(date) FROM ranking.ranking_signal")
                    row = cur.fetchone()
                    if row is None or row[0] is None:
                        return []
                    resolved_date = row[0]

                cur.execute(
                    """
                    SELECT date, symbol, strategy, rank_percentile, rank, n_symbols, score
                    FROM ranking.ranking_signal
                    WHERE date = %s
                    ORDER BY rank_percentile DESC, strategy, symbol
                    LIMIT %s
                    """,
                    (resolved_date, int(limit)),
                )
                rows = cur.fetchall()
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Signals unavailable: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Signals query failed: {exc}") from exc

    signals: List[dict[str, Any]] = []
    for row in rows:
        dt, symbol, strategy, percentile, rank, n_symbols, score = row
        strength = int(round(float(percentile or 0.0) * 100.0))
        if strength >= 90:
            signal_type = "BUY"
        elif strength <= 10:
            signal_type = "SELL"
        else:
            signal_type = "EXIT"

        signals.append(
            {
                "id": f"{dt.isoformat()}:{strategy}:{symbol}",
                "date": dt.isoformat(),
                "symbol": str(symbol),
                "strategyId": str(strategy),
                "strategyName": str(strategy),
                "signalType": signal_type,
                "strength": strength,
                "confidence": float(percentile or 0.0),
                "rank": int(rank),
                "nSymbols": int(n_symbols),
                "score": float(score) if score is not None else None,
            }
        )

    return signals

@router.get("/{strategy_id}")
def get_strategy_details(strategy_id: str, request: Request):
    validate_auth(request)
    return {"message": f"Details for {strategy_id} not implemented yet"}
