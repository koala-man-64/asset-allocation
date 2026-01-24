from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List, Dict, Any
from ..data_service import DataService

router = APIRouter()

@router.get("/{layer}/{domain}")
def get_data_generic(
    layer: str,
    domain: str,
    ticker: Optional[str] = None,
):
    """
    Generic endpoint for retrieving data from Silver/Gold layers.
    Delegates to DataService for logic.
    """
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
    ticker: str = Query(..., description="Ticker is required for finance reports"),
):
    """
    Specialized endpoint for Finance data.
    """
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
