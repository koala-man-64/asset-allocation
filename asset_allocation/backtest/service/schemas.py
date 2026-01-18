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

