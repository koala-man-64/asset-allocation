from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

import pandas as pd

from asset_allocation.backtest.blend import BlendConfig, normalize_alphas, normalize_exposure, weighted_sum
from asset_allocation.backtest.broker import SimulatedBroker
from asset_allocation.backtest.models import MarketSnapshot, PortfolioSnapshot
from asset_allocation.backtest.portfolio import Portfolio
from asset_allocation.backtest.sizer import Sizer
from asset_allocation.backtest.strategy import Strategy, StrategyDecision


_EPS = 1e-12


def _safe_float(value: object) -> Optional[float]:
    try:
        f = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _close_prices_for_date(prices: pd.DataFrame, *, as_of: date) -> Dict[str, float]:
    if prices is None or prices.empty:
        return {}
    if "date" not in prices.columns or "symbol" not in prices.columns:
        return {}
    if "close" not in prices.columns and "Close" not in prices.columns:
        return {}
    close_col = "close" if "close" in prices.columns else "Close"
    df = prices[prices["date"] == as_of]
    if df.empty:
        return {}
    out: Dict[str, float] = {}
    for _, row in df.iterrows():
        sym = str(row.get("symbol"))
        px = _safe_float(row.get(close_col))
        if not sym or px is None:
            continue
        out[sym] = float(px)
    return out


@dataclass(frozen=True)
class StrategyLeg:
    name: str
    alpha: float
    strategy: Strategy
    sizer: Sizer
    normalize_leg: str = "none"  # gross|net|none
    target_gross: Optional[float] = None
    target_net: Optional[float] = None
    enabled: bool = True


@dataclass
class _LegRuntime:
    cfg: StrategyLeg
    portfolio: Portfolio
    broker: SimulatedBroker
    last_target_weights: Dict[str, float] = field(default_factory=dict)
    pending_target_weights: Optional[Dict[str, float]] = None


@dataclass(frozen=True)
class CompositeLegResult:
    name: str
    alpha: float
    decision: Optional[StrategyDecision]
    target_weights: Dict[str, float]


@dataclass(frozen=True)
class CompositeDecision:
    leg_results: List[CompositeLegResult]
    blended_weights_pre_constraints: Dict[str, float]
    blend: BlendConfig


class CompositeStrategy(Strategy):
    """
    Multi-leg strategy that blends per-leg *target weights* into a single portfolio target.

    Timing remains aligned with the engine:
      - decide at close(T)
      - execute at open(T+1)

    Each leg runs against its own sleeve portfolio (to avoid cross-leg coupling via holdings/exits).
    """

    def __init__(
        self,
        *,
        legs: List[StrategyLeg],
        blend: BlendConfig,
        broker_config,
        initial_cash: float,
    ):
        super().__init__(rebalance="daily")
        if not legs:
            raise ValueError("CompositeStrategy requires at least one leg.")
        self._blend = blend

        raw_alphas = [float(l.alpha) for l in legs]
        alphas = normalize_alphas(raw_alphas, eps=_EPS)

        normalized_legs: list[StrategyLeg] = []
        for leg, alpha in zip(legs, alphas):
            name = str(leg.name).strip()
            if not name:
                raise ValueError("CompositeStrategy leg.name cannot be empty.")
            normalized_legs.append(
                StrategyLeg(
                    name=name,
                    alpha=float(alpha),
                    strategy=leg.strategy,
                    sizer=leg.sizer,
                    normalize_leg=str(leg.normalize_leg or "none").strip().lower(),
                    target_gross=float(leg.target_gross) if leg.target_gross is not None else None,
                    target_net=float(leg.target_net) if leg.target_net is not None else None,
                    enabled=bool(leg.enabled),
                )
            )

        self._legs: list[_LegRuntime] = []
        for leg in normalized_legs:
            sleeve_cash = float(initial_cash) * float(leg.alpha)
            portfolio = Portfolio(cash=sleeve_cash)
            broker = SimulatedBroker(config=broker_config, portfolio=portfolio)
            self._legs.append(_LegRuntime(cfg=leg, portfolio=portfolio, broker=broker))

        self._pending_post_constraints_by_leg: Dict[str, Dict[str, float]] = {}
        self._last_decision: Optional[CompositeDecision] = None
        self._last_final_weights: Dict[str, float] = {}

    @property
    def blend(self) -> BlendConfig:
        return self._blend

    def on_execution(self, *, market: MarketSnapshot) -> None:
        """
        Called by the engine immediately after executing portfolio-level targets at open(T).

        The composite uses this hook to execute each leg's sleeve targets (post-constraints allocation)
        so that per-leg strategies see correct sleeve holdings at close(T).
        """
        if not self._pending_post_constraints_by_leg:
            return

        for leg in self._legs:
            if not leg.cfg.enabled:
                continue
            pending = self._pending_post_constraints_by_leg.get(leg.cfg.name)
            if pending is None:
                continue
            leg.broker.execute_target_weights(market, target_weights=pending)
            leg.pending_target_weights = None

        self._pending_post_constraints_by_leg = {}

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[CompositeDecision]:
        close_prices = _close_prices_for_date(prices, as_of=as_of)

        leg_results: list[CompositeLegResult] = []
        any_leg_emitted = False

        for leg in self._legs:
            cfg = leg.cfg
            if not cfg.enabled:
                leg_results.append(CompositeLegResult(name=cfg.name, alpha=cfg.alpha, decision=None, target_weights={}))
                continue

            sleeve_snapshot = PortfolioSnapshot(
                as_of=as_of,
                cash=float(leg.portfolio.cash),
                positions=dict(leg.portfolio.positions),
                equity=float(leg.portfolio.equity(close_prices)),
                bar_index=portfolio.bar_index,
                position_states=leg.broker.get_position_states(),
            )

            decision = cfg.strategy.on_bar(
                as_of,
                prices=prices,
                signals=signals,
                portfolio=sleeve_snapshot,
            )

            target_weights = dict(leg.last_target_weights)
            if decision is not None:
                any_leg_emitted = True
                sized = cfg.sizer.size(as_of, decision=decision, prices=prices, portfolio=sleeve_snapshot)
                target_weights = dict(sized.weights or {})
                target_weights = normalize_exposure(
                    target_weights,
                    mode=cfg.normalize_leg,  # type: ignore[arg-type]
                    target_gross=float(cfg.target_gross) if cfg.target_gross is not None else 1.0,
                    target_net=float(cfg.target_net) if cfg.target_net is not None else 1.0,
                )
                leg.last_target_weights = dict(target_weights)

            leg_results.append(
                CompositeLegResult(
                    name=cfg.name,
                    alpha=cfg.alpha,
                    decision=decision,
                    target_weights=dict(target_weights),
                )
            )

        if not any_leg_emitted:
            return None

        alphas = [lr.alpha for lr in leg_results]
        blended = weighted_sum([lr.target_weights for lr in leg_results], alphas=alphas)
        blended = normalize_exposure(
            blended,
            mode=self._blend.normalize_final,
            target_gross=float(self._blend.target_gross) if self._blend.target_gross is not None else 1.0,
            target_net=float(self._blend.target_net) if self._blend.target_net is not None else 1.0,
        )

        decision_out = CompositeDecision(
            leg_results=leg_results,
            blended_weights_pre_constraints=blended,
            blend=self._blend,
        )
        self._last_decision = decision_out
        return decision_out

    def set_pending_post_constraints_targets(
        self,
        *,
        as_of: date,
        decision: CompositeDecision,
        final_weights: Dict[str, float],
    ) -> None:
        """
        Called by the engine after applying global constraints to store post-constraint leg targets.
        """
        pre = decision.blended_weights_pre_constraints or {}
        post = final_weights or {}

        # V1 overlap semantics:
        # - If allow_overlap=false: a symbol can appear in at most one leg.
        # - If allow_overlap=true: multiple legs can hold the same symbol, but all contributions must share sign.
        if not decision.blend.allow_overlap:
            for sym in set(pre.keys()):
                contributors = 0
                for lr in decision.leg_results:
                    if abs(float(lr.target_weights.get(sym, 0.0))) >= _EPS:
                        contributors += 1
                if contributors > 1:
                    raise ValueError(f"CompositeStrategy overlap is disabled but multiple legs target {sym!r}.")

        else:
            for sym in set(pre.keys()):
                signs = set()
                for lr in decision.leg_results:
                    w = float(lr.target_weights.get(sym, 0.0))
                    if abs(w) < _EPS:
                        continue
                    signs.add(1 if w > 0 else -1)
                if len(signs) > 1:
                    raise ValueError(
                        f"CompositeStrategy does not support opposing leg exposures for {sym!r} (v1)."
                    )

        by_leg: Dict[str, Dict[str, float]] = {}
        for lr in decision.leg_results:
            if lr.alpha <= _EPS:
                by_leg[lr.name] = {}
                continue
            out: Dict[str, float] = {}
            for sym, post_w in post.items():
                pre_w = float(pre.get(sym, 0.0))
                if abs(pre_w) < _EPS:
                    continue
                contrib_pre = float(lr.alpha) * float(lr.target_weights.get(sym, 0.0))
                contrib_post = contrib_pre * (float(post_w) / pre_w)
                sleeve_w = contrib_post / float(lr.alpha)
                if abs(sleeve_w) >= _EPS:
                    out[str(sym)] = float(sleeve_w)
            by_leg[lr.name] = out

        self._pending_post_constraints_by_leg = by_leg
        self._last_final_weights = dict(final_weights or {})

