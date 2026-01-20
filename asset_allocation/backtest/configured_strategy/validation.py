from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from asset_allocation.backtest.configured_strategy.exits import build_exit_engine
from asset_allocation.backtest.configured_strategy.holding import build_holding_policy
from asset_allocation.backtest.configured_strategy.rebalance import build_rebalance_schedule
from asset_allocation.backtest.configured_strategy.scoring import build_scoring_model
from asset_allocation.backtest.configured_strategy.selection import build_selector
from asset_allocation.backtest.configured_strategy.signals import build_signal_provider


DEFAULT_PRICE_COLUMNS = ["symbol", "date", "open", "close"]


def validate_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(cfg, dict):
        raise ValueError("ConfiguredStrategy config must be an object.")

    rebalance = dict(cfg.get("rebalance") or {"freq": "daily"})
    universe = dict(cfg.get("universe") or {"source": "signals", "filters": [], "require_columns": list(DEFAULT_PRICE_COLUMNS)})
    signals = dict(cfg.get("signals") or {"provider": "platinum_signals_daily"})
    scoring = dict(cfg.get("scoring") or {})
    selection = dict(cfg.get("selection") or {})
    holding_policy = dict(cfg.get("holding_policy") or {"type": "replace_all", "replace_all": {"exit_if_not_selected": False}})
    exits = dict(cfg.get("exits") or {"precedence": "exit_over_scale", "rules": []})
    postprocess = dict(cfg.get("postprocess") or {"steps": []})
    debug = dict(cfg.get("debug") or {})

    # Validate core component shapes by building them.
    build_rebalance_schedule(rebalance)
    # Universe is validated at runtime (needs prices/signals), but validate minimal structure.
    if "source" in universe:
        src = str(universe.get("source"))
        if src not in {"signals", "prices", "intersection"}:
            raise ValueError(f"universe.source must be one of signals/prices/intersection (got {src!r}).")
    if "require_columns" in universe and not isinstance(universe.get("require_columns"), list):
        raise ValueError("universe.require_columns must be a list.")
    if "filters" in universe and not isinstance(universe.get("filters"), list):
        raise ValueError("universe.filters must be a list.")

    build_signal_provider(signals)
    build_scoring_model(scoring)
    build_selector(selection)
    build_holding_policy(holding_policy)
    build_exit_engine(exits)

    steps = postprocess.get("steps") or []
    if not isinstance(steps, list):
        raise ValueError("postprocess.steps must be a list.")
    if debug and not isinstance(debug, dict):
        raise ValueError("debug must be an object.")

    return {
        "rebalance": rebalance,
        "universe": universe,
        "signals": signals,
        "scoring": scoring,
        "selection": selection,
        "holding_policy": holding_policy,
        "exits": exits,
        "postprocess": postprocess,
        "debug": debug,
    }
