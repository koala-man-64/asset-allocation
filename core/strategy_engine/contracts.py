from __future__ import annotations

from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ExitRuleType = Literal[
    "stop_loss_fixed",
    "take_profit_fixed",
    "trailing_stop_pct",
    "trailing_stop_atr",
    "time_stop",
]
ExitScope = Literal["position"]
ExitAction = Literal["exit_full"]
PriceField = Literal["open", "high", "low", "close"]
ExitReference = Literal["entry_price", "highest_since_entry"]
IntrabarConflictPolicy = Literal["stop_first", "take_profit_first", "priority_order"]
UniverseSource = Literal["postgres_gold"]
UniverseGroupOperator = Literal["and", "or"]
UniverseConditionOperator = Literal[
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
]
UniverseValue: TypeAlias = str | int | float | bool

_IDENTIFIER_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*$"


class UniverseCondition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["condition"] = "condition"
    table: str = Field(..., min_length=1, max_length=128, pattern=_IDENTIFIER_PATTERN)
    column: str = Field(..., min_length=1, max_length=128, pattern=_IDENTIFIER_PATTERN)
    operator: UniverseConditionOperator
    value: UniverseValue | None = None
    values: list[UniverseValue] | None = None

    @model_validator(mode="after")
    def validate_condition(self) -> "UniverseCondition":
        self.table = str(self.table or "").strip().lower()
        self.column = str(self.column or "").strip().lower()

        if self.operator in {"is_null", "is_not_null"}:
            if self.value is not None or self.values is not None:
                raise ValueError(f"{self.operator} does not accept value or values.")
            return self

        if self.operator in {"in", "not_in"}:
            if self.value is not None:
                raise ValueError(f"{self.operator} only supports values.")
            if not self.values:
                raise ValueError(f"{self.operator} requires at least one value.")
            return self

        if self.value is None:
            raise ValueError(f"{self.operator} requires value.")
        if self.values is not None:
            raise ValueError(f"{self.operator} does not support values.")
        return self


class UniverseGroup(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["group"] = "group"
    operator: UniverseGroupOperator = "and"
    clauses: list["UniverseGroup | UniverseCondition"] = Field(..., min_length=1)


class UniverseDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: UniverseSource = "postgres_gold"
    root: UniverseGroup


UniverseGroup.model_rebuild()
UniverseDefinition.model_rebuild()


class ExitRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., min_length=1, max_length=128)
    enabled: bool = True
    type: ExitRuleType
    scope: ExitScope = "position"
    priceField: PriceField | None = None
    value: float | None = None
    atrColumn: str | None = Field(default=None, min_length=1, max_length=128)
    priority: int | None = Field(default=None, ge=0)
    action: ExitAction = "exit_full"
    minHoldBars: int = Field(default=0, ge=0)
    reference: ExitReference | None = None

    @model_validator(mode="after")
    def validate_rule(self) -> "ExitRule":
        if self.type == "stop_loss_fixed":
            self._require_positive_value()
            self._default_reference("entry_price")
            self._default_price_field("low")
            self._reject_atr_column()
        elif self.type == "take_profit_fixed":
            self._require_positive_value()
            self._default_reference("entry_price")
            self._default_price_field("high")
            self._reject_atr_column()
        elif self.type == "trailing_stop_pct":
            self._require_positive_value()
            self._default_reference("highest_since_entry")
            self._default_price_field("low")
            self._reject_atr_column()
        elif self.type == "trailing_stop_atr":
            self._require_positive_value()
            self._default_reference("highest_since_entry")
            self._default_price_field("low")
            if not self.atrColumn:
                raise ValueError("trailing_stop_atr requires atrColumn.")
        elif self.type == "time_stop":
            self._require_positive_value(integer_only=True)
            self._reject_reference()
            self._reject_atr_column()
            if self.priceField is None:
                self.priceField = "close"
            elif self.priceField != "close":
                raise ValueError("time_stop only supports priceField='close'.")

        return self

    def _require_positive_value(self, *, integer_only: bool = False) -> None:
        if self.value is None or self.value <= 0:
            raise ValueError(f"{self.type} requires value > 0.")
        if integer_only and not float(self.value).is_integer():
            raise ValueError(f"{self.type} requires an integer value.")

    def _default_reference(self, expected: ExitReference) -> None:
        if self.reference is None:
            self.reference = expected
            return
        if self.reference != expected:
            raise ValueError(f"{self.type} only supports reference='{expected}'.")

    def _default_price_field(self, expected: PriceField) -> None:
        if self.priceField is None:
            self.priceField = expected
            return
        if self.priceField not in {"open", "high", "low", "close"}:
            raise ValueError(f"{self.type} received unsupported priceField='{self.priceField}'.")

    def _reject_atr_column(self) -> None:
        if self.atrColumn is not None:
            raise ValueError(f"{self.type} does not support atrColumn.")

    def _reject_reference(self) -> None:
        if self.reference is not None:
            raise ValueError(f"{self.type} does not support reference.")


class StrategyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    universeConfigName: str | None = Field(default=None, min_length=1, max_length=128)
    universe: UniverseDefinition | None = None
    rebalance: str = Field(default="monthly", min_length=1, max_length=64)
    longOnly: bool = True
    topN: int = Field(default=20, ge=1)
    lookbackWindow: int = Field(default=63, ge=1)
    holdingPeriod: int = Field(default=21, ge=1)
    costModel: str = Field(default="default", min_length=1, max_length=64)
    rankingSchemaName: str | None = Field(default=None, min_length=1, max_length=128)
    intrabarConflictPolicy: IntrabarConflictPolicy = "stop_first"
    exits: list[ExitRule] = Field(default_factory=list)

    @field_validator("universeConfigName", mode="before")
    @classmethod
    def normalize_universe_config_name(cls, value: object) -> object:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("rankingSchemaName", mode="before")
    @classmethod
    def normalize_ranking_schema_name(cls, value: object) -> object:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @model_validator(mode="after")
    def normalize_exits(self) -> "StrategyConfig":
        if not self.universeConfigName and self.universe is None:
            raise ValueError("universeConfigName is required.")
        seen_ids: set[str] = set()
        for idx, rule in enumerate(self.exits):
            if rule.id in seen_ids:
                raise ValueError(f"Duplicate exit rule id '{rule.id}'.")
            seen_ids.add(rule.id)
            if rule.priority is None:
                rule.priority = idx
        return self
