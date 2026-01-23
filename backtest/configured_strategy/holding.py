from __future__ import annotations

from datetime import date
from typing import Dict, Literal, Optional, Protocol, Tuple

import pandas as pd

from backtest.models import PortfolioSnapshot
from backtest.configured_strategy.state import PositionStateStore


class HoldingPolicy(Protocol):
    def apply(
        self,
        *,
        as_of: date,
        portfolio: PortfolioSnapshot,
        raw_scores: pd.Series,
        selected_scores: Dict[str, float],
        state_store: PositionStateStore,
        is_rebalance: bool,
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        ...


def _bar_index(portfolio: PortfolioSnapshot) -> Optional[int]:
    try:
        return int(portfolio.bar_index) if portfolio.bar_index is not None else None
    except Exception:
        return None


class ReplaceAllPolicy:
    def __init__(
        self,
        *,
        exit_if_not_selected: bool = True,
        refresh_held_scores: bool = False,
        refresh_mode: Literal["raw", "abs_signed"] = "abs_signed",
    ) -> None:
        self._exit_if_not_selected = bool(exit_if_not_selected)
        self._refresh_held_scores = bool(refresh_held_scores)
        self._refresh_mode: Literal["raw", "abs_signed"] = str(refresh_mode)  # type: ignore[assignment]
        if self._refresh_mode not in {"raw", "abs_signed"}:
            raise ValueError("holding_policy.replace_all.refresh_mode must be 'raw' or 'abs_signed'.")

    def _held_score(
        self,
        *,
        symbol: str,
        shares: float,
        raw_scores: pd.Series,
        state_store: PositionStateStore,
    ) -> float:
        side = 1.0 if float(shares) > 0 else -1.0
        if self._refresh_held_scores and raw_scores is not None and not raw_scores.empty:
            val = raw_scores.get(str(symbol))
            if val is not None:
                try:
                    fval = float(val)
                except (TypeError, ValueError):
                    fval = None
                if fval is not None and pd.notna(fval):
                    if self._refresh_mode == "raw":
                        return float(fval)
                    return abs(float(fval)) * float(side)

        st = state_store.get(symbol)
        if st and st.last_score is not None:
            return float(st.last_score)
        return float(side)

    def apply(
        self,
        *,
        as_of: date,
        portfolio: PortfolioSnapshot,
        raw_scores: pd.Series,
        selected_scores: Dict[str, float],
        state_store: PositionStateStore,
        is_rebalance: bool,
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        if is_rebalance:
            if self._exit_if_not_selected:
                return dict(selected_scores), {}
            # Keep existing holdings even if not selected.
            merged: Dict[str, float] = {}
            for sym, sh in (portfolio.positions or {}).items():
                if abs(float(sh)) < 1e-12:
                    continue
                merged[str(sym)] = self._held_score(symbol=str(sym), shares=float(sh), raw_scores=raw_scores, state_store=state_store)
            merged.update({str(k): float(v) for k, v in selected_scores.items()})
            return merged, {}

        # Non-rebalance days: preserve current holdings; ignore selected_scores.
        kept: Dict[str, float] = {}
        for sym, sh in (portfolio.positions or {}).items():
            if abs(float(sh)) < 1e-12:
                continue
            kept[str(sym)] = self._held_score(symbol=str(sym), shares=float(sh), raw_scores=raw_scores, state_store=state_store)
        return kept, {}


class StickyPolicy:
    def __init__(
        self,
        *,
        keep_if_rank_within_multiple: float = 2.0,
        keep_min_abs_score: Optional[float] = None,
        reentry_cooldown_days: Optional[int] = None,
    ) -> None:
        self._multiple = float(keep_if_rank_within_multiple)
        self._keep_min_abs_score = float(keep_min_abs_score) if keep_min_abs_score is not None else None
        self._cooldown_days = int(reentry_cooldown_days) if reentry_cooldown_days is not None else None

    def apply(
        self,
        *,
        as_of: date,
        portfolio: PortfolioSnapshot,
        raw_scores: pd.Series,
        selected_scores: Dict[str, float],
        state_store: PositionStateStore,
        is_rebalance: bool,
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        held = {s: float(sh) for s, sh in (portfolio.positions or {}).items() if abs(float(sh)) >= 1e-12}
        if not is_rebalance:
            kept: Dict[str, float] = {}
            for sym, sh in held.items():
                st = state_store.get(sym)
                kept[str(sym)] = float(st.last_score) if st and st.last_score is not None else float(1.0 if sh > 0 else -1.0)
            return kept, {}

        # Determine target sizes by side based on selected set.
        n_long = sum(1 for s in selected_scores.values() if float(s) > 0)
        n_short = sum(1 for s in selected_scores.values() if float(s) < 0)

        keep_long_rank = int(max(0, round(self._multiple * max(1, n_long))))
        keep_short_rank = int(max(0, round(self._multiple * max(1, n_short))))

        raw = raw_scores.copy()
        raw = pd.to_numeric(raw, errors="coerce").dropna()

        # Build rank sets.
        keep_set: set[str] = set()

        if keep_long_rank > 0:
            longs = raw[raw > 0].sort_values(ascending=False)
            if self._keep_min_abs_score is not None:
                longs = longs[longs.abs() >= self._keep_min_abs_score]
            keep_set |= set(str(s) for s in longs.head(keep_long_rank).index.tolist())

        if keep_short_rank > 0:
            shorts = raw[raw < 0].sort_values(ascending=True)  # more negative first
            if self._keep_min_abs_score is not None:
                shorts = shorts[shorts.abs() >= self._keep_min_abs_score]
            keep_set |= set(str(s) for s in shorts.head(keep_short_rank).index.tolist())

        merged: Dict[str, float] = {}
        merged.update({str(k): float(v) for k, v in selected_scores.items()})

        for sym, sh in held.items():
            sym_s = str(sym)
            if sym_s in merged:
                continue
            if sym_s not in keep_set:
                continue
            st = state_store.get(sym_s)
            if st and st.last_score is not None:
                merged[sym_s] = float(st.last_score)
            else:
                merged[sym_s] = float(1.0 if sh > 0 else -1.0)

        # Apply re-entry cooldown by filtering merged candidates.
        if self._cooldown_days is not None and self._cooldown_days > 0:
            idx = _bar_index(portfolio)
            if idx is not None:
                filtered: Dict[str, float] = {}
                for sym, score in merged.items():
                    st = state_store.get(sym)
                    if st and st.cooldown_until_bar_index is not None and idx < int(st.cooldown_until_bar_index):
                        continue
                    filtered[sym] = float(score)
                merged = filtered

        return merged, {}


class GracePeriodPolicy:
    def __init__(self, *, min_hold_days: int = 3, exit_only_on_risk: bool = True) -> None:
        self._min_hold_days = int(min_hold_days)
        self._exit_only_on_risk = bool(exit_only_on_risk)

    def apply(
        self,
        *,
        as_of: date,
        portfolio: PortfolioSnapshot,
        raw_scores: pd.Series,
        selected_scores: Dict[str, float],
        state_store: PositionStateStore,
        is_rebalance: bool,
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        held = {s: float(sh) for s, sh in (portfolio.positions or {}).items() if abs(float(sh)) >= 1e-12}
        if not is_rebalance:
            kept: Dict[str, float] = {}
            for sym, sh in held.items():
                st = state_store.get(sym)
                kept[str(sym)] = float(st.last_score) if st and st.last_score is not None else float(1.0 if sh > 0 else -1.0)
            return kept, {}

        merged: Dict[str, float] = dict({str(k): float(v) for k, v in selected_scores.items()})

        idx = _bar_index(portfolio)
        for sym, sh in held.items():
            sym_s = str(sym)
            if sym_s in merged:
                continue
            st = state_store.get(sym_s)
            if st is None:
                continue

            if self._exit_only_on_risk:
                merged[sym_s] = float(st.last_score) if st.last_score is not None else float(1.0 if sh > 0 else -1.0)
                continue

            # If selection would drop this symbol, keep it until it satisfies the minimum hold.
            entry_bar_index = None
            if portfolio.position_states and sym_s in portfolio.position_states:
                entry_bar_index = portfolio.position_states[sym_s].entry_bar_index
            if idx is not None and entry_bar_index is not None:
                if idx - int(entry_bar_index) < self._min_hold_days:
                    merged[sym_s] = float(st.last_score) if st.last_score is not None else float(1.0 if sh > 0 else -1.0)
            else:
                merged[sym_s] = float(st.last_score) if st.last_score is not None else float(1.0 if sh > 0 else -1.0)

        return merged, {}


class DecayPolicy:
    def __init__(self, *, dropped_scale: float = 0.5, decay_days: int = 5) -> None:
        self._dropped_scale = float(dropped_scale)
        self._decay_days = int(decay_days)

    def apply(
        self,
        *,
        as_of: date,
        portfolio: PortfolioSnapshot,
        raw_scores: pd.Series,
        selected_scores: Dict[str, float],
        state_store: PositionStateStore,
        is_rebalance: bool,
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        held = {s: float(sh) for s, sh in (portfolio.positions or {}).items() if abs(float(sh)) >= 1e-12}
        merged: Dict[str, float] = {}
        scales: Dict[str, float] = {}
        merged.update({str(k): float(v) for k, v in selected_scores.items()})

        idx = _bar_index(portfolio)
        for sym, sh in held.items():
            sym_s = str(sym)
            st = state_store.get(sym_s)
            if st is None:
                continue

            if sym_s in selected_scores:
                st.dropped_since_bar_index = None
                continue

            # Track dropped positions.
            if idx is not None and st.dropped_since_bar_index is None:
                st.dropped_since_bar_index = idx

            if idx is not None and st.dropped_since_bar_index is not None and self._decay_days > 0:
                dropped_for = idx - int(st.dropped_since_bar_index)
                if dropped_for >= self._decay_days:
                    # After the decay window, treat as removed (do not emit a score).
                    continue
                frac = max(0.0, min(1.0, dropped_for / float(self._decay_days)))
                scale = 1.0 - frac * (1.0 - self._dropped_scale)
                scales[sym_s] = max(0.0, min(1.0, float(scale)))
            else:
                scales[sym_s] = max(0.0, min(1.0, self._dropped_scale))

            merged[sym_s] = float(st.last_score) if st.last_score is not None else float(1.0 if sh > 0 else -1.0)

        return merged, scales


class HybridPolicy:
    def __init__(self, *, base: HoldingPolicy, overlay: HoldingPolicy) -> None:
        self._base = base
        self._overlay = overlay

    def apply(
        self,
        *,
        as_of: date,
        portfolio: PortfolioSnapshot,
        raw_scores: pd.Series,
        selected_scores: Dict[str, float],
        state_store: PositionStateStore,
        is_rebalance: bool,
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        base_scores, base_scales = self._base.apply(
            as_of=as_of,
            portfolio=portfolio,
            raw_scores=raw_scores,
            selected_scores=selected_scores,
            state_store=state_store,
            is_rebalance=is_rebalance,
        )
        overlay_scores, overlay_scales = self._overlay.apply(
            as_of=as_of,
            portfolio=portfolio,
            raw_scores=raw_scores,
            selected_scores=base_scores,
            state_store=state_store,
            is_rebalance=is_rebalance,
        )
        scales = dict(base_scales)
        scales.update(overlay_scales)
        return overlay_scores, scales


def build_holding_policy(cfg: dict) -> HoldingPolicy:
    if not isinstance(cfg, dict):
        raise ValueError("holding_policy must be an object.")
    ptype = str(cfg.get("type") or "").strip()
    if ptype == "replace_all":
        inner = dict(cfg.get("replace_all") or {})
        return ReplaceAllPolicy(
            exit_if_not_selected=bool(inner.get("exit_if_not_selected", True)),
            refresh_held_scores=bool(inner.get("refresh_held_scores", False)),
            refresh_mode=str(inner.get("refresh_mode") or "abs_signed"),  # type: ignore[arg-type]
        )
    if ptype == "sticky":
        inner = dict(cfg.get("sticky") or {})
        return StickyPolicy(
            keep_if_rank_within_multiple=float(inner.get("keep_if_rank_within_multiple", 2.0)),
            keep_min_abs_score=float(inner["keep_min_abs_score"]) if "keep_min_abs_score" in inner and inner["keep_min_abs_score"] is not None else None,
            reentry_cooldown_days=int(inner["reentry_cooldown_days"]) if "reentry_cooldown_days" in inner and inner["reentry_cooldown_days"] is not None else None,
        )
    if ptype == "grace_period":
        inner = dict(cfg.get("grace_period") or {})
        return GracePeriodPolicy(
            min_hold_days=int(inner.get("min_hold_days", 3)),
            exit_only_on_risk=bool(inner.get("exit_only_on_risk", True)),
        )
    if ptype == "decay":
        inner = dict(cfg.get("decay") or {})
        return DecayPolicy(
            dropped_scale=float(inner.get("dropped_scale", 0.5)),
            decay_days=int(inner.get("decay_days", 5)),
        )
    if ptype == "hybrid":
        inner = dict(cfg.get("hybrid") or {})
        base_name = str(inner.get("base") or "").strip()
        overlay_name = str(inner.get("overlay") or "").strip()
        # Minimal implementation: only allow composing named simple policies without nested config.
        base = build_holding_policy({"type": base_name, base_name: {}})
        overlay = build_holding_policy({"type": overlay_name, overlay_name: {}})
        return HybridPolicy(base=base, overlay=overlay)
    raise ValueError(f"Unknown holding_policy.type: {ptype!r}")
