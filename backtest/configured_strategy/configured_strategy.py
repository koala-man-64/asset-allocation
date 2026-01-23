from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from backtest.models import PortfolioSnapshot
from backtest.strategy import Strategy, StrategyDecision
from backtest.configured_strategy.exits import ExitEngineResult, build_exit_engine
from backtest.configured_strategy.holding import build_holding_policy
from backtest.configured_strategy.postprocess import apply_postprocess
from backtest.configured_strategy.rebalance import build_rebalance_schedule
from backtest.configured_strategy.scoring import build_scoring_model
from backtest.configured_strategy.selection import build_selector
from backtest.configured_strategy.signals import build_signal_provider
from backtest.configured_strategy.state import PositionStateStore
from backtest.configured_strategy.universe import UniverseResult, build_universe
from backtest.configured_strategy.validation import validate_config


def _maybe_mkdir(path: Optional[Path]) -> Optional[Path]:
    if path is None:
        return None
    Path(path).mkdir(parents=True, exist_ok=True)
    return Path(path)


def _find_enforce_min_candidates(post_cfg: dict) -> Optional[dict]:
    steps = post_cfg.get("steps") or []
    if not isinstance(steps, list):
        return None
    for step in steps:
        if isinstance(step, dict) and str(step.get("type") or "").strip() == "enforce_min_candidates":
            return step
    return None


def _scales_changed(prev: Dict[str, float], curr: Dict[str, float], *, eps: float = 1e-12) -> bool:
    keys = set(prev.keys()) | set(curr.keys())
    for k in keys:
        before = float(prev.get(k, 1.0))
        after = float(curr.get(k, 1.0))
        if abs(before - after) > eps:
            return True
    return False


class ConfiguredStrategy(Strategy):
    """
    A single configurable strategy pipeline.

    Decision timing remains aligned with the engine: decide at close(T) and execute at open(T+1).
    """

    def __init__(self, *, config: Dict[str, Any], debug_output_dir: Optional[Path] = None):
        super().__init__(rebalance="daily")
        self._cfg = validate_config(config)

        self._schedule = build_rebalance_schedule(self._cfg["rebalance"])
        self._universe_cfg = dict(self._cfg["universe"])
        self._signals_provider = build_signal_provider(self._cfg["signals"])
        self._scoring_model = build_scoring_model(self._cfg["scoring"])
        self._selector = build_selector(self._cfg["selection"])
        self._holding_policy = build_holding_policy(self._cfg["holding_policy"])
        self._exit_engine = build_exit_engine(self._cfg["exits"])
        self._post_cfg = dict(self._cfg["postprocess"])
        self._debug_cfg = dict(self._cfg.get("debug") or {})

        self._state = PositionStateStore()
        self._debug_dir = _maybe_mkdir(Path(debug_output_dir) / "strategy_debug" if debug_output_dir else None)
        self._last_selected_scores: Dict[str, float] = {}
        self._last_emitted_scales: Dict[str, float] = {}

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[StrategyDecision]:
        # 0) Sync shared position state store from broker/portfolio.
        self._state.sync(as_of, prices=prices, portfolio=portfolio)

        held_symbols = [str(s) for s, sh in (portfolio.positions or {}).items() if abs(float(sh)) >= 1e-12]

        # 1) Rebalance gate controls entry/refresh logic, but risk exits/scales are evaluated daily.
        wants_rebalance = self._schedule.should_rebalance(as_of, portfolio=portfolio, commit=False)
        is_rebalance = bool(wants_rebalance)

        # 2) Universe + signals + raw scores (only needed for rebalance decisions or for tracing).
        universe: UniverseResult = build_universe(
            as_of=as_of,
            prices=prices,
            signals_today=signals,
            cfg=self._universe_cfg,
        )

        signals_today = self._signals_provider.load_today(
            as_of=as_of,
            prices=prices,
            signals_today=signals,
            eligible_symbols=universe.eligible_symbols,
        )

        raw_scores = self._scoring_model.score(
            as_of=as_of,
            prices=prices,
            signals_today=signals_today,
            eligible_symbols=universe.eligible_symbols,
        )

        selected_scores: Dict[str, float] = {}
        if is_rebalance:
            selected_scores = self._selector.select(scores=raw_scores)

            enforce = _find_enforce_min_candidates(self._post_cfg)
            if enforce:
                min_total = int(enforce.get("min_total", 0))
                action = str(enforce.get("action") or "skip_rebalance")
                if min_total > 0 and len(selected_scores) < min_total:
                    if action == "skip_rebalance":
                        is_rebalance = False
                        selected_scores = {}
                    else:
                        raise ValueError(f"enforce_min_candidates failed: selected={len(selected_scores)} < {min_total}")
        if is_rebalance:
            # Commit the rebalance schedule only if we executed a rebalance pass.
            self._schedule.should_rebalance(as_of, portfolio=portfolio, commit=True)
            self._last_selected_scores = dict(selected_scores)

        # 3) Holding policy merges selected candidates with existing holdings.
        selected_for_policy = selected_scores if is_rebalance else dict(self._last_selected_scores)
        merged_scores, holding_scales = self._holding_policy.apply(
            as_of=as_of,
            portfolio=portfolio,
            raw_scores=raw_scores,
            selected_scores=selected_for_policy,
            state_store=self._state,
            is_rebalance=is_rebalance,
        )

        # 4) Exit engine evaluates held positions daily.
        exit_result: ExitEngineResult = self._exit_engine.evaluate(
            symbols=held_symbols,
            as_of=as_of,
            prices=prices,
            portfolio=portfolio,
            state_store=self._state,
        )

        # 5) Apply exits/scales.
        for sym in exit_result.exit_symbols:
            merged_scores.pop(str(sym), None)
            holding_scales.pop(str(sym), None)

        scales: Dict[str, float] = {}
        holding_scales = {str(k): float(v) for k, v in (holding_scales or {}).items()}
        exit_scales = {str(k): float(v) for k, v in (exit_result.scale_updates or {}).items()}

        for sym in merged_scores.keys():
            scale = 1.0
            if sym in holding_scales:
                scale = min(scale, float(holding_scales[sym]))
            st = self._state.get(sym)
            if st is not None:
                scale = min(scale, float(st.target_scale))
            if sym in exit_scales:
                scale = min(scale, float(exit_scales[sym]))
            if abs(scale - 1.0) > 1e-12:
                scales[str(sym)] = float(scale)

        # 6) Postprocess scores/scales.
        post = apply_postprocess(steps_cfg=list(self._post_cfg.get("steps") or []), scores=merged_scores, scales=scales, state_store=self._state)
        scores_out = post.scores
        scales_out = {str(k): float(v) for k, v in (post.scales or {}).items() if abs(float(v) - 1.0) > 1e-12}

        changed_non_rebalance = bool(exit_result.exit_symbols) or _scales_changed(self._last_emitted_scales, scales_out)
        if not is_rebalance and not changed_non_rebalance:
            return None

        # 7) Update state.last_score for symbols we are targeting/holding.
        for sym, score in (scores_out or {}).items():
            st = self._state.get(sym)
            if st is not None:
                st.last_score = float(score)

        # 8) Debug artifacts (best-effort, local-only).
        self._emit_debug(
            as_of=as_of,
            universe=universe,
            raw_scores=raw_scores,
            selected_scores=selected_scores,
            merged_scores=merged_scores,
            exit_result=exit_result,
            final_scores=scores_out,
            final_scales=scales_out,
        )

        self._last_emitted_scales = dict(scales_out)
        return StrategyDecision(scores=scores_out, scales=scales_out)

    def _emit_debug(
        self,
        *,
        as_of: date,
        universe: UniverseResult,
        raw_scores: pd.Series,
        selected_scores: Dict[str, float],
        merged_scores: Dict[str, float],
        exit_result: ExitEngineResult,
        final_scores: Dict[str, float],
        final_scales: Dict[str, float],
    ) -> None:
        if self._debug_dir is None:
            return
        record_intermediates = bool(self._debug_cfg.get("record_intermediates", False))
        record_reasons = bool(self._debug_cfg.get("record_reasons", False))
        if not record_intermediates and not record_reasons:
            return

        prefix = as_of.isoformat()
        try:
            if record_intermediates:
                pd.DataFrame({"symbol": universe.eligible_symbols}).to_csv(self._debug_dir / f"{prefix}_universe.csv", index=False)
                pd.DataFrame({"symbol": list(raw_scores.index), "raw_score": list(raw_scores.values)}).to_csv(
                    self._debug_dir / f"{prefix}_raw_scores.csv", index=False
                )
                pd.DataFrame({"symbol": list(selected_scores.keys()), "selected_score": list(selected_scores.values())}).to_csv(
                    self._debug_dir / f"{prefix}_selected.csv", index=False
                )
                pd.DataFrame({"symbol": list(merged_scores.keys()), "held_score": list(merged_scores.values())}).to_csv(
                    self._debug_dir / f"{prefix}_held.csv", index=False
                )
                pd.DataFrame({"symbol": list(final_scores.keys()), "score": list(final_scores.values())}).to_csv(
                    self._debug_dir / f"{prefix}_scores.csv", index=False
                )
                pd.DataFrame({"symbol": list(final_scales.keys()), "scale": list(final_scales.values())}).to_csv(
                    self._debug_dir / f"{prefix}_scales.csv", index=False
                )

            if record_reasons:
                exit_rows = []
                for sym in sorted(set(exit_result.exit_symbols) | set(exit_result.scale_updates.keys()) | set(exit_result.reasons.keys())):
                    exit_rows.append(
                        {
                            "symbol": str(sym),
                            "exit": bool(sym in exit_result.exit_symbols),
                            "scale": float(exit_result.scale_updates.get(sym)) if sym in exit_result.scale_updates else None,
                            "reasons": ";".join(exit_result.reasons.get(sym, [])),
                        }
                    )
                pd.DataFrame(exit_rows).to_csv(self._debug_dir / f"{prefix}_exits.csv", index=False)
        except Exception:
            # Debug artifacts should never fail the strategy.
            return
