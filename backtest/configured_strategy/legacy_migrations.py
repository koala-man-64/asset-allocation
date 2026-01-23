from __future__ import annotations

from typing import Any, Dict, Optional, Union


LegacyRebalance = Union[str, int]


def _rebalance_cfg(rebalance: Optional[LegacyRebalance]) -> Dict[str, Any]:
    if rebalance is None:
        return {"freq": "daily"}

    if isinstance(rebalance, int):
        n = int(rebalance)
        if n <= 0:
            return {"freq": "daily"}
        return {"freq": "every_n_days", "every_n_days": n}

    value = str(rebalance).strip().lower()
    if not value:
        return {"freq": "daily"}

    if value.isdigit():
        n = int(value)
        if n <= 0:
            return {"freq": "daily"}
        return {"freq": "every_n_days", "every_n_days": n}

    if value in {"daily", "weekly", "monthly", "quarterly", "annually"}:
        return {"freq": value}

    return {"freq": "daily"}


def configured_config_for_topn_signal_strategy(
    *,
    signal_column: str,
    top_n: int,
    min_signal: Optional[float] = None,
    higher_is_better: bool = True,
    rebalance: Optional[LegacyRebalance] = None,
) -> Dict[str, Any]:
    return {
        "rebalance": _rebalance_cfg(rebalance),
        "universe": {"source": "signals", "filters": [], "require_columns": ["symbol", "date", "open", "close"]},
        "signals": {"provider": "platinum_signals_daily", "columns": [str(signal_column)]},
        "scoring": {"type": "column", "column": str(signal_column), "higher_is_better": True, "fillna": "drop"},
        "selection": {
            "type": "topn",
            "topn": {
                "n": int(top_n),
                "side": "long",
                "min_score": float(min_signal) if min_signal is not None else None,
                "higher_is_better": bool(higher_is_better),
            },
        },
        "holding_policy": {"type": "replace_all", "replace_all": {"exit_if_not_selected": True}},
        "exits": {"precedence": "exit_over_scale", "rules": []},
        "postprocess": {"steps": []},
        "debug": {"record_intermediates": False, "record_reasons": False},
    }


def configured_config_for_long_short_topn_strategy(
    *,
    signal_column: str,
    k_long: int,
    k_short: int,
    long_if_high: bool = True,
    min_abs_score: float = 0.0,
    trailing_ma_days: Optional[int] = None,
    stop_loss_pct: Optional[float] = None,
    use_low_for_stop: bool = True,
    partial_exit_days: Optional[int] = None,
    partial_exit_fraction: float = 0.5,
    max_hold_days: Optional[int] = None,
    rebalance: Optional[LegacyRebalance] = None,
) -> Dict[str, Any]:
    rules: list[dict[str, Any]] = []
    if trailing_ma_days is not None:
        rules.append({"type": "trailing_ma", "days": int(trailing_ma_days), "price_col": "close", "side_aware": True})
    if max_hold_days is not None:
        rules.append({"type": "time_stop", "days": int(max_hold_days)})
    if stop_loss_pct is not None:
        rules.append(
            {
                "type": "stop_loss",
                "pct": float(stop_loss_pct),
                "use_intraday_extremes": bool(use_low_for_stop),
            }
        )
    if partial_exit_days is not None:
        rules.append(
            {
                "type": "partial_exit_after_days",
                "days": int(partial_exit_days),
                "fraction": float(partial_exit_fraction),
                "once": True,
            }
        )

    return {
        "rebalance": _rebalance_cfg(rebalance),
        "universe": {"source": "signals", "filters": [], "require_columns": ["symbol", "date", "open", "close"]},
        "signals": {"provider": "platinum_signals_daily", "columns": [str(signal_column)]},
        "scoring": {"type": "column", "column": str(signal_column), "higher_is_better": True, "fillna": "drop"},
        "selection": {
            "type": "long_short_topn",
            "long_short_topn": {
                "long_n": int(k_long),
                "short_n": int(k_short),
                "min_abs_score": float(min_abs_score),
                "allow_short": bool(int(k_short) > 0),
                "long_if_high": bool(long_if_high),
            },
        },
        "holding_policy": {
            "type": "replace_all",
            "replace_all": {"exit_if_not_selected": False, "refresh_held_scores": True, "refresh_mode": "abs_signed"},
        },
        "exits": {"precedence": "exit_over_scale", "rules": rules},
        "postprocess": {"steps": []},
        "debug": {"record_intermediates": False, "record_reasons": False},
    }

