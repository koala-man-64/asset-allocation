from __future__ import annotations

from datetime import date
from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

RankingTransformType = Literal[
    "percentile_rank",
    "zscore",
    "minmax",
    "clip",
    "winsorize",
    "coalesce",
    "log1p",
    "negate",
    "abs",
]
RankingMissingValuePolicy = Literal["exclude", "zero"]
RankingDirection = Literal["asc", "desc"]
RankingCatalogValueKind = Literal["number", "boolean"]
RankingParamValue: TypeAlias = str | int | float | bool | None

_IDENTIFIER_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*$"


class RankingTransform(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: RankingTransformType
    params: dict[str, RankingParamValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_transform(self) -> "RankingTransform":
        allowed: dict[RankingTransformType, set[str]] = {
            "percentile_rank": set(),
            "zscore": set(),
            "minmax": set(),
            "clip": {"lower", "upper"},
            "winsorize": {"lowerQuantile", "upperQuantile"},
            "coalesce": {"value"},
            "log1p": set(),
            "negate": set(),
            "abs": set(),
        }
        supported = allowed[self.type]
        unknown = set(self.params.keys()).difference(supported)
        if unknown:
            raise ValueError(f"{self.type} received unsupported params: {sorted(unknown)}")

        if self.type == "clip" and not {"lower", "upper"}.intersection(self.params):
            raise ValueError("clip requires lower and/or upper.")
        if self.type == "winsorize":
            if not {"lowerQuantile", "upperQuantile"}.intersection(self.params):
                raise ValueError("winsorize requires lowerQuantile and/or upperQuantile.")
            for key in ("lowerQuantile", "upperQuantile"):
                value = self.params.get(key)
                if value is None:
                    continue
                if not isinstance(value, (int, float)) or float(value) < 0 or float(value) > 1:
                    raise ValueError(f"{key} must be between 0 and 1.")
        if self.type == "coalesce" and "value" not in self.params:
            raise ValueError("coalesce requires value.")
        return self


class RankingFactor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=128)
    table: str = Field(..., min_length=1, max_length=128, pattern=_IDENTIFIER_PATTERN)
    column: str = Field(..., min_length=1, max_length=128, pattern=_IDENTIFIER_PATTERN)
    weight: float = Field(default=1.0, gt=0)
    direction: RankingDirection = "desc"
    missingValuePolicy: RankingMissingValuePolicy = "exclude"
    transforms: list[RankingTransform] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("name is required.")
        return normalized

    @field_validator("table", "column")
    @classmethod
    def normalize_identifier(cls, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if not normalized:
            raise ValueError("identifier is required.")
        return normalized


class RankingGroup(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=128)
    weight: float = Field(default=1.0, gt=0)
    factors: list[RankingFactor] = Field(..., min_length=1)
    transforms: list[RankingTransform] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("name is required.")
        return normalized

    @model_validator(mode="after")
    def validate_unique_factor_names(self) -> "RankingGroup":
        seen: set[str] = set()
        for factor in self.factors:
            if factor.name in seen:
                raise ValueError(f"Duplicate factor name '{factor.name}' in group '{self.name}'.")
            seen.add(factor.name)
        return self


class RankingSchemaConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    universeConfigName: str | None = Field(default=None, min_length=1, max_length=128)
    groups: list[RankingGroup] = Field(..., min_length=1)
    overallTransforms: list[RankingTransform] = Field(default_factory=list)

    @field_validator("universeConfigName", mode="before")
    @classmethod
    def normalize_universe_config_name(cls, value: object) -> object:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @model_validator(mode="after")
    def validate_unique_group_names(self) -> "RankingSchemaConfig":
        seen: set[str] = set()
        for group in self.groups:
            if group.name in seen:
                raise ValueError(f"Duplicate group name '{group.name}'.")
            seen.add(group.name)
        return self


class RankingPreviewRow(BaseModel):
    symbol: str
    rank: int
    score: float


class RankingMaterializationSummary(BaseModel):
    runId: str
    strategyName: str
    rankingSchemaName: str
    rankingSchemaVersion: int
    outputTableName: str
    startDate: date | None = None
    endDate: date | None = None
    rowCount: int
    dateCount: int
