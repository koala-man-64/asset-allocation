from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


RunStatus = Literal["queued", "running", "completed", "failed"]


class BacktestSubmitRequest(BaseModel):
    config: Optional[Dict[str, Any]] = None
    config_yaml: Optional[str] = None
    run_id: Optional[str] = None
    strict: bool = True

    @model_validator(mode="after")
    def _validate_one_config_source(self) -> "BacktestSubmitRequest":
        if bool(self.config) == bool(self.config_yaml):
            raise ValueError("Provide exactly one of: config, config_yaml.")
        return self


class BacktestSubmitResponse(BaseModel):
    run_id: str
    status: RunStatus


class RunRecordResponse(BaseModel):
    run_id: str
    status: RunStatus
    submitted_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    run_name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    output_dir: Optional[str] = None
    adls_container: Optional[str] = None
    adls_prefix: Optional[str] = None
    error: Optional[str] = None


class RunListResponse(BaseModel):
    runs: List[RunRecordResponse]
    limit: int
    offset: int


class ArtifactInfoResponse(BaseModel):
    name: str
    size_bytes: int
    last_modified: Optional[str] = None


class ArtifactListResponse(BaseModel):
    local: List[ArtifactInfoResponse] = Field(default_factory=list)
    remote: Optional[List[ArtifactInfoResponse]] = None
    remote_error: Optional[str] = None


class TimeseriesPointResponse(BaseModel):
    date: str
    portfolio_value: float
    drawdown: float
    daily_return: Optional[float] = None
    cumulative_return: Optional[float] = None
    cash: Optional[float] = None
    gross_exposure: Optional[float] = None
    net_exposure: Optional[float] = None
    turnover: Optional[float] = None
    commission: Optional[float] = None
    slippage_cost: Optional[float] = None


class TimeseriesResponse(BaseModel):
    points: List[TimeseriesPointResponse]
    total_points: int
    truncated: bool


class RollingMetricPointResponse(BaseModel):
    date: str
    window_days: int
    rolling_return: Optional[float] = None
    rolling_volatility: Optional[float] = None
    rolling_sharpe: Optional[float] = None
    rolling_max_drawdown: Optional[float] = None
    turnover_sum: Optional[float] = None
    commission_sum: Optional[float] = None
    slippage_cost_sum: Optional[float] = None
    n_trades_sum: Optional[float] = None
    gross_exposure_avg: Optional[float] = None
    net_exposure_avg: Optional[float] = None


class RollingMetricsResponse(BaseModel):
    points: List[RollingMetricPointResponse]
    total_points: int
    truncated: bool


class TradeResponse(BaseModel):
    execution_date: str
    symbol: str
    quantity: float
    price: float
    notional: float
    commission: float
    slippage_cost: float
    cash_after: float


class TradeListResponse(BaseModel):
    trades: List[TradeResponse]
    total: int
    limit: int
    offset: int
