import logging
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from api.service.dependencies import validate_auth


from core.strategy_repository import StrategyRepository

logger = logging.getLogger(__name__)

router = APIRouter()

class StrategyConfigModel(BaseModel):
    name: str
    config: Dict[str, Any]
    description: Optional[str] = ""
    type: Optional[str] = "configured"

@router.get("/", response_model=List[Dict[str, Any]])
async def list_strategies(request: Request) -> List[Dict[str, Any]]:
    """
    List all available strategies.
    """
    validate_auth(request)
    settings = request.app.state.settings
    repo = StrategyRepository(settings.postgres_dsn)
    return repo.list_strategies()

@router.get("/{name}", response_model=Dict[str, Any])
async def get_strategy(name: str, request: Request) -> Dict[str, Any]:
    """
    Get configuration for a specific strategy by name.
    """
    validate_auth(request)
    settings = request.app.state.settings
    repo = StrategyRepository(settings.postgres_dsn)
    config = repo.get_strategy_config(name)
    if not config:
        # Check if it exists in list to return metadata + empty config, or just 404?
        # For now, if config is missing, return 404
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' configuration not found")
    
    # We might want to return metadata + config merged, but repo.get_strategy_config returns just the JSON config
    # Let's return the config object directly for now as per repository contract
    return config

@router.post("/")
async def save_strategy(strategy: StrategyConfigModel, request: Request) -> Dict[str, str]:
    """
    Create or update a strategy configuration.
    Requires authentication.
    """
    validate_auth(request)
    settings = request.app.state.settings
    repo = StrategyRepository(settings.postgres_dsn)
    
    try:
        repo.save_strategy(
            name=strategy.name,
            config=strategy.config,
            strategy_type=strategy.type or "configured",
            description=strategy.description or ""
        )
        return {"status": "success", "message": f"Strategy '{strategy.name}' saved successfully"}
    except Exception as e:
        logger.error(f"Error saving strategy: {e}")
        raise HTTPException(status_code=500, detail=str(e))
