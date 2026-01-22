from fastapi import APIRouter, HTTPException

from backend.api import dependencies as deps
from scripts.common import pipeline

router = APIRouter()


@router.get("/market/{layer}/{ticker}")
def get_market_data_alias(layer: str, ticker: str):
    """
    Alias endpoint for UI callers.

    Canonical endpoint remains:
      GET /data/{layer}/market?ticker={ticker}
    """
    if layer not in {"silver", "gold"}:
        raise HTTPException(status_code=400, detail="Layer must be 'silver' or 'gold'.")

    try:
        container = deps.resolve_container(layer, "market")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if layer == "silver":
        path = pipeline.DataPaths.get_market_data_path(ticker)
    else:
        path = pipeline.DataPaths.get_gold_features_path(ticker)

    try:
        dt = deps.get_delta_table(container, path)
        df = dt.to_pandas()
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Data not found: {exc}") from exc

    return df.to_dict(orient="records")


@router.get("/finance/{layer}/{sub_domain}/{ticker}")
def get_finance_data_alias(layer: str, sub_domain: str, ticker: str):
    """
    Alias endpoint for UI callers.

    Canonical endpoint remains:
      GET /data/{layer}/finance/{sub_domain}?ticker={ticker}
    """
    if layer not in {"silver", "gold"}:
        raise HTTPException(status_code=400, detail="Layer must be 'silver' or 'gold'.")

    try:
        container = deps.resolve_container(layer, "finance")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if layer == "silver":
        folder_map = {
            "balance_sheet": ("Balance Sheet", "quarterly_balance-sheet"),
            "income_statement": ("Income Statement", "quarterly_financials"),
            "cash_flow": ("Cash Flow", "quarterly_cash-flow"),
            "valuation": ("Valuation", "quarterly_valuation_measures"),
        }

        if sub_domain not in folder_map:
            raise HTTPException(status_code=400, detail=f"Unknown finance sub-domain: {sub_domain}")

        folder, suffix = folder_map[sub_domain]
        path = pipeline.DataPaths.get_finance_path(folder, ticker, suffix)
    else:
        path = pipeline.DataPaths.get_gold_finance_path(ticker)

    try:
        dt = deps.get_delta_table(container, path)
        df = dt.to_pandas()
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Data not found: {exc}") from exc

    return df.to_dict(orient="records")


@router.get("/strategies")
def get_strategies_alias():
    """
    Alias endpoint for UI callers.

    Canonical endpoint remains:
      GET /ranking/strategies
    """
    container = deps.resolve_container("platinum")
    path = "strategies"

    try:
        dt = deps.get_delta_table(container, path)
        df = dt.to_pandas()
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Strategies data not found: {exc}") from exc

    return df.to_dict(orient="records")

