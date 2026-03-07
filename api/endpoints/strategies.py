import logging
from datetime import datetime
from typing import Any, List

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from api.service.dependencies import validate_auth

from core.strategy_engine import StrategyConfig
from core.strategy_repository import StrategyRepository

logger = logging.getLogger(__name__)

router = APIRouter()


class StrategySummaryResponse(BaseModel):
    name: str
    type: str = "configured"
    description: str = ""
    updated_at: datetime | None = None


class StrategyDetailResponse(StrategySummaryResponse):
    config: StrategyConfig


class StrategyUpsertRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    config: StrategyConfig
    description: str = ""
    type: str = "configured"


def _normalize_strategy_config(config: Any) -> dict[str, Any]:
    return StrategyConfig.model_validate(config or {}).model_dump(exclude_none=True)


def _build_strategy_detail_response(strategy: dict[str, Any]) -> StrategyDetailResponse:
    return StrategyDetailResponse(
        name=str(strategy.get("name") or ""),
        type=str(strategy.get("type") or "configured"),
        description=str(strategy.get("description") or ""),
        updated_at=strategy.get("updated_at"),
        config=StrategyConfig.model_validate(strategy.get("config") or {}),
    )


@router.get("/", response_model=List[StrategySummaryResponse])
async def list_strategies(request: Request) -> List[dict[str, Any]]:
    """
    List all available strategies.
    """
    validate_auth(request)
    settings = request.app.state.settings
    repo = StrategyRepository(settings.postgres_dsn)
    return repo.list_strategies()


@router.get("/{name}/detail", response_model=StrategyDetailResponse)
async def get_strategy_detail(name: str, request: Request) -> StrategyDetailResponse:
    """
    Get normalized metadata and configuration for a specific strategy.
    """
    validate_auth(request)
    settings = request.app.state.settings
    repo = StrategyRepository(settings.postgres_dsn)
    strategy = repo.get_strategy(name)
    if not strategy:
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' not found")
    return _build_strategy_detail_response(strategy)


@router.get("/{name}", response_model=StrategyConfig)
async def get_strategy(name: str, request: Request) -> StrategyConfig:
    """
    Get configuration for a specific strategy by name.
    """
    validate_auth(request)
    settings = request.app.state.settings
    repo = StrategyRepository(settings.postgres_dsn)
    config = repo.get_strategy_config(name)
    if config is None:
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' configuration not found")
    return StrategyConfig.model_validate(config)


@router.post("/")
async def save_strategy(strategy: StrategyUpsertRequest, request: Request) -> dict[str, str]:
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
            config=_normalize_strategy_config(strategy.config),
            strategy_type=strategy.type or "configured",
            description=strategy.description or "",
        )
        return {"status": "success", "message": f"Strategy '{strategy.name}' saved successfully"}
    except Exception as e:
        logger.error(f"Error saving strategy: {e}")
        raise HTTPException(status_code=500, detail=str(e))
