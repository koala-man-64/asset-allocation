from fastapi import APIRouter, HTTPException
from api import dependencies as deps

router = APIRouter()

@router.get("/strategies")
def get_strategies():
    """
    Retrieves strategy performance data (Platinum Layer).
    """
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

@router.get("/{strategy_id}")
def get_strategy_details(strategy_id: str):
    return {"message": f"Details for {strategy_id} not implemented yet"}
