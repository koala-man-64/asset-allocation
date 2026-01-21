from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List, Dict, Any
from backend.api import dependencies as deps
from scripts.common import pipeline
import pandas as pd

router = APIRouter()

@router.get("/{layer}/{domain}")
def get_data_generic(
    layer: str,
    domain: str,
    ticker: Optional[str] = None,
    delta_table = Depends(deps.get_delta_table), # This would need refactoring as dependencies usually take args or we resolve inside
):
    """
    Generic endpoint for retrieving data from Silver/Gold layers.
    Note: 'delta_table' dependency injection with dynamic args is tricky in FastAPI.
    We'll resolve logic inside the handler for simplicity.
    """
    if layer not in ["silver", "gold"]:
        raise HTTPException(status_code=400, detail="Layer must be 'silver' or 'gold'. Use /ranking for platinum.")

    # Resolve Container
    try:
        container = deps.resolve_container(layer)
        if layer == "gold":
             container = deps.resolve_gold_container(domain)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Resolve Path
    # This logic mimics pipeline.py but needs to be dynamic
    path = ""
    if domain == "market":
        if ticker:
             path = pipeline.DataPaths.get_market_data_path(ticker) if layer == "silver" else pipeline.DataPaths.get_gold_features_path(ticker)
        else:
             path = pipeline.DataPaths.get_market_data_by_date_path() if layer == "silver" else pipeline.DataPaths.get_gold_features_by_date_path()
    
    elif domain == "earnings":
        if ticker:
            path = pipeline.DataPaths.get_earnings_path(ticker) if layer == "silver" else pipeline.DataPaths.get_gold_earnings_path(ticker)
        else:
            path = pipeline.DataPaths.get_earnings_by_date_path() if layer == "silver" else pipeline.DataPaths.get_gold_earnings_by_date_path()

    elif domain == "price-target":
         if ticker:
            path = pipeline.DataPaths.get_price_target_path(ticker) if layer == "silver" else pipeline.DataPaths.get_gold_price_targets_path(ticker)
         else:
            path = pipeline.DataPaths.get_price_targets_by_date_path() if layer == "silver" else pipeline.DataPaths.get_gold_price_targets_by_date_path()
            
    else:
        raise HTTPException(status_code=404, detail=f"Domain '{domain}' not supported on generic endpoint")

    # Fetch Data
    try:
        dt = deps.get_delta_table(container, path)
        df = dt.to_pandas()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Data not found: {str(e)}")

@router.get("/{layer}/finance/{sub_domain}")
def get_finance_data(
    layer: str,
    sub_domain: str,
    ticker: str = Query(..., description="Ticker is required for finance reports"),
):
    """
    Specialized endpoint for Finance data (Balance Sheet, Income Statement, etc.)
    """
    if layer not in ["silver", "gold"]:
         raise HTTPException(status_code=400, detail="Layer must be 'silver' or 'gold'")

    try:
        container = deps.resolve_container(layer)
        if layer == "gold":
             container = deps.resolve_gold_container("finance")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Resolve Path
    # Silver Finance is stored by (Folder, Ticker, Suffix)
    # We map sub_domain to Folder and Suffix
    if layer == "silver":
        folder_map = {
            "balance_sheet": ("Balance Sheet", "quarterly_balance-sheet"),
            "income_statement": ("Income Statement", "quarterly_financials"),
            "cash_flow": ("Cash Flow", "quarterly_cash-flow"),
            "valuation": ("Valuation", "quarterly_valuation_measures")
        }
        
        if sub_domain not in folder_map:
             raise HTTPException(status_code=400, detail=f"Unknown finance sub-domain: {sub_domain}")
             
        folder, suffix = folder_map[sub_domain]
        path = pipeline.DataPaths.get_finance_path(folder, ticker, suffix)
        
    else:
        # Gold Finance is usually unified or structurally different?
        # Per pipeline.py: get_gold_finance_path(ticker) -> finance/{ticker}
        # It seems Gold finance might be a single wide table or we haven't split it yet?
        # logic: return entire gold finance table for ticker
        if sub_domain == "all":
             path = pipeline.DataPaths.get_gold_finance_path(ticker)
        else:
             # If Gold is also split, we'd need that logic. 
             # For now, default to the main path.
             path = pipeline.DataPaths.get_gold_finance_path(ticker)

    # Fetch
    try:
        dt = deps.get_delta_table(container, path)
        df = dt.to_pandas()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Data not found: {str(e)}")
