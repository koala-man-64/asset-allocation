from __future__ import annotations

import json
import secrets
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml


def _parse_date(value: Any, *, field_name: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be YYYY-MM-DD (got {value!r}).") from exc
    raise ValueError(f"{field_name} must be a date or YYYY-MM-DD string (got {type(value)!r}).")


def generate_run_id(*, now: Optional[datetime] = None, suffix_len: int = 6) -> str:
    timestamp = now or datetime.now(timezone.utc)
    date_part = timestamp.strftime("%Y%m%d")
    suffix = secrets.token_hex(max(1, suffix_len // 2))[:suffix_len]
    return f"RUN{date_part}-{suffix}"

_STRICT_ALLOWED_TOP_LEVEL_KEYS = {
    "run_name",
    "start_date",
    "end_date",
    "initial_cash",
    "universe",
    "data",
    "strategy",
    "sizing",
    "constraints",
    "broker",
    "output",
}

_STRICT_ALLOWED_SECTIONS: Dict[str, set[str]] = {
    "universe": {"symbols", "asset_class", "currency"},
    "data": {"price_source", "price_path", "signal_path", "price_fields", "frequency"},
    "strategy": {"class", "class_name", "module", "parameters"},
    "sizing": {"class", "class_name", "module", "parameters"},
    "constraints": {
        "max_leverage",
        "max_position_size",
        "allow_short",
        "stop_loss",
        "max_turnover",
        "max_net_exposure",
        "net_exposure_min",
        "net_exposure_max",
        "min_weight_change",
    },
    "broker": {
        "slippage_bps",
        "spread_bps",
        "commission",
        "fill_policy",
        "allow_fractional_shares",
        "lot_size",
        "rounding_mode",
        "min_trade_notional",
        "min_trade_shares",
        "on_missing_price",
        "max_participation_rate",
    },
    "output": {
        "local_dir",
        "adls_dir",
        "save_trades",
        "save_daily_metrics",
        "save_plots",
        "save_positions_snapshot",
        "save_metrics_parquet",
        "save_resolved_config_json",
        "save_metrics_json",
        "save_constraint_hits",
        "save_run_index_parquet",
    },
}


def validate_config_dict_strict(data: Dict[str, Any]) -> None:
    """
    Best-effort strict validation to catch YAML typos early.

    Only validates known keys; full semantic validation still occurs in BacktestConfig.validate().
    """
    if not isinstance(data, dict):
        raise ValueError("BacktestConfig must be an object.")

    unknown_top = set(data.keys()) - _STRICT_ALLOWED_TOP_LEVEL_KEYS
    if unknown_top:
        raise ValueError(f"Unknown top-level config field(s): {sorted(unknown_top)}")

    for section, allowed in _STRICT_ALLOWED_SECTIONS.items():
        if section not in data or data[section] is None:
            continue
        payload = data[section]
        if not isinstance(payload, dict):
            raise ValueError(f"{section} must be an object.")
        unknown = set(payload.keys()) - allowed
        if unknown:
            raise ValueError(f"Unknown {section} field(s): {sorted(unknown)}")


@dataclass(frozen=True)
class UniverseConfig:
    symbols: List[str]
    asset_class: str = "Equity"
    currency: str = "USD"

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "UniverseConfig":
        symbols = data.get("symbols")
        if not isinstance(symbols, list) or not symbols or not all(isinstance(s, str) and s.strip() for s in symbols):
            raise ValueError("universe.symbols must be a non-empty list of symbols.")
        return UniverseConfig(
            symbols=[s.strip() for s in symbols],
            asset_class=str(data.get("asset_class") or "Equity"),
            currency=str(data.get("currency") or "USD"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbols": list(self.symbols),
            "asset_class": self.asset_class,
            "currency": self.currency,
        }


@dataclass(frozen=True)
class DataConfig:
    price_source: Literal["local", "ADLS"] = "local"
    price_path: Optional[str] = None
    signal_path: Optional[str] = None
    price_fields: List[str] = field(default_factory=lambda: ["Open", "High", "Low", "Close", "Volume"])
    frequency: str = "Daily"

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "DataConfig":
        price_source = str(data.get("price_source") or "local")
        if price_source not in {"local", "ADLS"}:
            raise ValueError("data.price_source must be 'local' or 'ADLS'.")
        price_fields = data.get("price_fields") or ["Open", "High", "Low", "Close", "Volume"]
        if not isinstance(price_fields, list) or not all(isinstance(x, str) for x in price_fields):
            raise ValueError("data.price_fields must be a list of strings.")
        return DataConfig(
            price_source=price_source,  # type: ignore[arg-type]
            price_path=data.get("price_path"),
            signal_path=data.get("signal_path"),
            price_fields=price_fields,
            frequency=str(data.get("frequency") or "Daily"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "price_source": self.price_source,
            "price_path": self.price_path,
            "signal_path": self.signal_path,
            "price_fields": list(self.price_fields),
            "frequency": self.frequency,
        }


@dataclass(frozen=True)
class ComponentConfig:
    class_name: str
    module: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(data: Dict[str, Any], *, label: str) -> "ComponentConfig":
        class_name = data.get("class") or data.get("class_name")
        if not isinstance(class_name, str) or not class_name.strip():
            raise ValueError(f"{label}.class is required.")
        params = data.get("parameters") or {}
        if not isinstance(params, dict):
            raise ValueError(f"{label}.parameters must be an object.")
        module = data.get("module")
        return ComponentConfig(class_name=class_name.strip(), module=str(module) if module else None, parameters=params)

    def to_dict(self) -> Dict[str, Any]:
        out = {"class": self.class_name}
        if self.module:
            out["module"] = self.module
        if self.parameters:
            out["parameters"] = self.parameters
        return out


@dataclass(frozen=True)
class ConstraintsConfig:
    max_leverage: float = 1.0
    max_position_size: float = 1.0
    allow_short: bool = False
    stop_loss: Optional[float] = None
    max_turnover: Optional[float] = None
    max_net_exposure: Optional[float] = None
    net_exposure_min: Optional[float] = None
    net_exposure_max: Optional[float] = None
    min_weight_change: Optional[float] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ConstraintsConfig":
        max_leverage = float(data.get("max_leverage", 1.0))
        max_position_size = float(data.get("max_position_size", 1.0))
        allow_short = bool(data.get("allow_short", False))
        stop_loss = data.get("stop_loss")
        if stop_loss is not None:
            stop_loss = float(stop_loss)
        max_turnover = data.get("max_turnover")
        if max_turnover is not None:
            max_turnover = float(max_turnover)
        max_net_exposure = data.get("max_net_exposure")
        if max_net_exposure is not None:
            max_net_exposure = float(max_net_exposure)
        net_exposure_min = data.get("net_exposure_min")
        if net_exposure_min is not None:
            net_exposure_min = float(net_exposure_min)
        net_exposure_max = data.get("net_exposure_max")
        if net_exposure_max is not None:
            net_exposure_max = float(net_exposure_max)
        min_weight_change = data.get("min_weight_change")
        if min_weight_change is not None:
            min_weight_change = float(min_weight_change)
        return ConstraintsConfig(
            max_leverage=max_leverage,
            max_position_size=max_position_size,
            allow_short=allow_short,
            stop_loss=stop_loss,
            max_turnover=max_turnover,
            max_net_exposure=max_net_exposure,
            net_exposure_min=net_exposure_min,
            net_exposure_max=net_exposure_max,
            min_weight_change=min_weight_change,
        )

    def validate(self) -> None:
        if self.max_leverage <= 0:
            raise ValueError("constraints.max_leverage must be > 0.")
        if not (0 < self.max_position_size <= 1.0):
            raise ValueError("constraints.max_position_size must be in (0, 1].")
        if self.stop_loss is not None and not (0 < self.stop_loss < 1.0):
            raise ValueError("constraints.stop_loss must be in (0, 1) when set.")
        if self.max_turnover is not None and not (0 < float(self.max_turnover) <= 10.0):
            raise ValueError("constraints.max_turnover must be in (0, 10] when set.")
        if self.max_net_exposure is not None and not (0 <= float(self.max_net_exposure) <= 10.0):
            raise ValueError("constraints.max_net_exposure must be in [0, 10] when set.")
        if self.net_exposure_min is not None and not (-10.0 <= float(self.net_exposure_min) <= 10.0):
            raise ValueError("constraints.net_exposure_min must be in [-10, 10] when set.")
        if self.net_exposure_max is not None and not (-10.0 <= float(self.net_exposure_max) <= 10.0):
            raise ValueError("constraints.net_exposure_max must be in [-10, 10] when set.")
        if (
            self.net_exposure_min is not None
            and self.net_exposure_max is not None
            and float(self.net_exposure_min) > float(self.net_exposure_max)
        ):
            raise ValueError("constraints.net_exposure_min must be <= constraints.net_exposure_max when both are set.")
        if self.min_weight_change is not None and not (0.0 <= float(self.min_weight_change) <= 10.0):
            raise ValueError("constraints.min_weight_change must be in [0, 10] when set.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_leverage": self.max_leverage,
            "max_position_size": self.max_position_size,
            "allow_short": self.allow_short,
            "stop_loss": self.stop_loss,
            "max_turnover": self.max_turnover,
            "max_net_exposure": self.max_net_exposure,
            "net_exposure_min": self.net_exposure_min,
            "net_exposure_max": self.net_exposure_max,
            "min_weight_change": self.min_weight_change,
        }


@dataclass(frozen=True)
class BrokerConfig:
    slippage_bps: float = 0.0
    spread_bps: float = 0.0
    commission: float = 0.0
    fill_policy: str = "next_open"
    allow_fractional_shares: bool = True
    lot_size: int = 1
    rounding_mode: str = "toward_zero"
    min_trade_notional: float = 0.0
    min_trade_shares: float = 0.0
    on_missing_price: Literal["skip", "reject"] = "skip"
    max_participation_rate: Optional[float] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "BrokerConfig":
        max_participation_rate = data.get("max_participation_rate")
        if max_participation_rate is not None:
            max_participation_rate = float(max_participation_rate)
        return BrokerConfig(
            slippage_bps=float(data.get("slippage_bps", 0.0)),
            spread_bps=float(data.get("spread_bps", 0.0)),
            commission=float(data.get("commission", 0.0)),
            fill_policy=str(data.get("fill_policy") or "next_open"),
            allow_fractional_shares=bool(data.get("allow_fractional_shares", True)),
            lot_size=int(data.get("lot_size", 1)),
            rounding_mode=str(data.get("rounding_mode") or "toward_zero"),
            min_trade_notional=float(data.get("min_trade_notional", 0.0)),
            min_trade_shares=float(data.get("min_trade_shares", 0.0)),
            on_missing_price=str(data.get("on_missing_price") or "skip"),
            max_participation_rate=max_participation_rate,
        )

    def validate(self) -> None:
        if self.slippage_bps < 0:
            raise ValueError("broker.slippage_bps must be >= 0.")
        if self.spread_bps < 0:
            raise ValueError("broker.spread_bps must be >= 0.")
        if self.commission < 0:
            raise ValueError("broker.commission must be >= 0.")
        if self.fill_policy != "next_open":
            raise ValueError("broker.fill_policy only supports 'next_open' in the current engine.")
        if self.lot_size <= 0:
            raise ValueError("broker.lot_size must be > 0.")
        if self.rounding_mode not in {"toward_zero", "nearest", "floor", "ceil"}:
            raise ValueError("broker.rounding_mode must be one of: toward_zero, nearest, floor, ceil.")
        if self.min_trade_notional < 0:
            raise ValueError("broker.min_trade_notional must be >= 0.")
        if self.min_trade_shares < 0:
            raise ValueError("broker.min_trade_shares must be >= 0.")
        if self.on_missing_price not in {"skip", "reject"}:
            raise ValueError("broker.on_missing_price must be 'skip' or 'reject'.")
        if self.max_participation_rate is not None and not (0.0 < float(self.max_participation_rate) <= 1.0):
            raise ValueError("broker.max_participation_rate must be in (0, 1] when set.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "slippage_bps": self.slippage_bps,
            "spread_bps": self.spread_bps,
            "commission": self.commission,
            "fill_policy": self.fill_policy,
            "allow_fractional_shares": self.allow_fractional_shares,
            "lot_size": self.lot_size,
            "rounding_mode": self.rounding_mode,
            "min_trade_notional": self.min_trade_notional,
            "min_trade_shares": self.min_trade_shares,
            "on_missing_price": self.on_missing_price,
            "max_participation_rate": self.max_participation_rate,
        }


@dataclass(frozen=True)
class OutputConfig:
    local_dir: str = "./backtest_results"
    adls_dir: Optional[str] = None
    save_trades: bool = True
    save_daily_metrics: bool = True
    save_plots: bool = False
    save_positions_snapshot: bool = True
    save_metrics_parquet: bool = True
    save_resolved_config_json: bool = True
    save_metrics_json: bool = True
    save_constraint_hits: bool = True
    save_run_index_parquet: bool = True

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "OutputConfig":
        return OutputConfig(
            local_dir=str(data.get("local_dir") or "./backtest_results"),
            adls_dir=str(data.get("adls_dir")) if data.get("adls_dir") else None,
            save_trades=bool(data.get("save_trades", True)),
            save_daily_metrics=bool(data.get("save_daily_metrics", True)),
            save_plots=bool(data.get("save_plots", False)),
            save_positions_snapshot=bool(data.get("save_positions_snapshot", True)),
            save_metrics_parquet=bool(data.get("save_metrics_parquet", True)),
            save_resolved_config_json=bool(data.get("save_resolved_config_json", True)),
            save_metrics_json=bool(data.get("save_metrics_json", True)),
            save_constraint_hits=bool(data.get("save_constraint_hits", True)),
            save_run_index_parquet=bool(data.get("save_run_index_parquet", True)),
        )

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "local_dir": self.local_dir,
            "save_trades": self.save_trades,
            "save_daily_metrics": self.save_daily_metrics,
            "save_plots": self.save_plots,
            "save_positions_snapshot": self.save_positions_snapshot,
            "save_metrics_parquet": self.save_metrics_parquet,
            "save_resolved_config_json": self.save_resolved_config_json,
            "save_metrics_json": self.save_metrics_json,
            "save_constraint_hits": self.save_constraint_hits,
            "save_run_index_parquet": self.save_run_index_parquet,
        }
        if self.adls_dir:
            out["adls_dir"] = self.adls_dir
        return out


@dataclass(frozen=True)
class BacktestConfig:
    start_date: date
    end_date: date
    universe: UniverseConfig
    strategy: ComponentConfig
    sizing: ComponentConfig = field(default_factory=lambda: ComponentConfig(class_name="EqualWeightSizer"))
    constraints: ConstraintsConfig = field(default_factory=ConstraintsConfig)
    broker: BrokerConfig = field(default_factory=BrokerConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    data: Optional[DataConfig] = None
    run_name: Optional[str] = None
    initial_cash: float = 1_000_000.0

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "BacktestConfig":
        if not isinstance(data, dict):
            raise ValueError("BacktestConfig must be an object.")

        start_date = _parse_date(data.get("start_date"), field_name="start_date")
        end_date = _parse_date(data.get("end_date"), field_name="end_date")
        universe = UniverseConfig.from_dict(data.get("universe") or {})
        strategy = ComponentConfig.from_dict(data.get("strategy") or {}, label="strategy")
        sizing = ComponentConfig.from_dict(data.get("sizing") or {"class": "EqualWeightSizer"}, label="sizing")
        constraints = ConstraintsConfig.from_dict(data.get("constraints") or {})
        broker = BrokerConfig.from_dict(data.get("broker") or {})
        output = OutputConfig.from_dict(data.get("output") or {})
        data_cfg = data.get("data")
        data_section = DataConfig.from_dict(data_cfg) if isinstance(data_cfg, dict) else None
        run_name = data.get("run_name")
        initial_cash = float(data.get("initial_cash", 1_000_000.0))

        cfg = BacktestConfig(
            run_name=str(run_name) if run_name else None,
            start_date=start_date,
            end_date=end_date,
            initial_cash=initial_cash,
            universe=universe,
            data=data_section,
            strategy=strategy,
            sizing=sizing,
            constraints=constraints,
            broker=broker,
            output=output,
        )
        cfg.validate()
        return cfg

    @staticmethod
    def from_yaml(path: str | Path, *, strict: bool = False) -> "BacktestConfig":
        raw = Path(path).read_text(encoding="utf-8")
        data = yaml.safe_load(raw) or {}
        if strict:
            validate_config_dict_strict(data)
        return BacktestConfig.from_dict(data)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "run_name": self.run_name,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "initial_cash": self.initial_cash,
            "universe": self.universe.to_dict(),
            "strategy": self.strategy.to_dict(),
            "sizing": self.sizing.to_dict(),
            "constraints": self.constraints.to_dict(),
            "broker": self.broker.to_dict(),
            "output": self.output.to_dict(),
        }
        if self.data:
            out["data"] = self.data.to_dict()
        return out

    def to_yaml(self, path: str | Path) -> None:
        Path(path).write_text(yaml.safe_dump(self.to_dict(), sort_keys=False), encoding="utf-8")

    def canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    def validate(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date.")
        if self.initial_cash <= 0:
            raise ValueError("initial_cash must be > 0.")
        self.constraints.validate()
        self.broker.validate()
