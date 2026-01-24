from __future__ import annotations

import json
import math
import calendar
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from filelock import FileLock

from backtest.config import BacktestConfig
from backtest.composite_strategy import CompositeDecision
from backtest.constraints import ConstraintHit
from backtest.models import BacktestSummary, TradeFill, PortfolioSnapshot
from backtest.portfolio import Portfolio
from backtest.risk_model import RiskModel


def _safe_float(value: Any) -> Optional[float]:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _annualize_return(total_return: float, periods: int, *, periods_per_year: int = 252) -> float:
    if periods <= 0:
        return 0.0
    return (1.0 + total_return) ** (periods_per_year / periods) - 1.0


def _annualized_volatility(daily_returns: pd.Series, *, periods_per_year: int = 252) -> float:
    std = float(daily_returns.std(ddof=0))
    if std <= 0:
        return 0.0
    return std * math.sqrt(periods_per_year)


def _sharpe(daily_returns: pd.Series, *, periods_per_year: int = 252) -> Optional[float]:
    std = float(daily_returns.std(ddof=0))
    if std <= 0:
        return None
    mean = float(daily_returns.mean())
    return (mean / std) * math.sqrt(periods_per_year)


def _rolling_max_drawdown(values: "pd.Series | list[float]") -> float:
    peak = None
    worst = 0.0
    for raw in values:
        v = _safe_float(raw)
        if v is None:
            continue
        if peak is None or v > peak:
            peak = v
        if peak and peak > 0:
            dd = v / peak - 1.0
            if dd < worst:
                worst = dd
    return float(worst)


@dataclass
class Reporter:
    config: BacktestConfig
    run_id: str
    output_dir: Path
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    _trades: List[Dict[str, Any]] = field(default_factory=list)
    _days: List[Dict[str, Any]] = field(default_factory=list)
    _positions: List[Dict[str, Any]] = field(default_factory=list)
    _constraint_hits: List[Dict[str, Any]] = field(default_factory=list)
    _composite_leg_weights: List[Dict[str, Any]] = field(default_factory=list)
    _composite_blended_pre: List[Dict[str, Any]] = field(default_factory=list)
    _composite_blended_post: List[Dict[str, Any]] = field(default_factory=list)

    @staticmethod
    def create(config: BacktestConfig, *, run_id: str, output_dir: Optional[Path] = None) -> "Reporter":
        base = Path(output_dir) if output_dir else Path(config.output.local_dir)
        run_dir = base / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        return Reporter(config=config, run_id=run_id, output_dir=run_dir)

    def record_trades(self, fills: List[TradeFill]) -> None:
        for fill in fills:
            self._trades.append(
                {
                    "execution_date": fill.execution_date.isoformat(),
                    "symbol": fill.symbol,
                    "quantity": fill.quantity,
                    "price": fill.price,
                    "notional": fill.notional,
                    "commission": fill.commission,
                    "slippage_cost": fill.slippage_cost,
                    "cash_after": fill.cash_after,
                }
            )

    def record_day(
        self,
        as_of: date,
        *,
        portfolio: Portfolio,
        equity: float,
        daily_return: float,
        cumulative_return: float,
        drawdown: float,
        gross_exposure: float,
        net_exposure: float,
        turnover: float,
        commission: float,
        slippage_cost: float,
        n_trades: int = 0,
    ) -> None:
        self._days.append(
            {
                "date": as_of.isoformat(),
                "portfolio_value": float(equity),
                "cash": float(portfolio.cash),
                "daily_return": float(daily_return),
                "cumulative_return": float(cumulative_return),
                "drawdown": float(drawdown),
                "gross_exposure": float(gross_exposure),
                "net_exposure": float(net_exposure),
                "turnover": float(turnover),
                "commission": float(commission),
                "slippage_cost": float(slippage_cost),
                "n_trades": int(n_trades),
            }
        )

    def record_positions_snapshot(
        self,
        as_of: date,
        *,
        portfolio: Portfolio,
        equity: float,
        close_prices: Dict[str, float],
    ) -> None:
        universe = list(self.config.universe.symbols)
        for symbol in universe:
            shares = float(portfolio.shares(symbol))
            close_px = close_prices.get(symbol)
            position_value = None if close_px is None else shares * float(close_px)
            weight = (position_value / float(equity)) if (position_value is not None and equity) else 0.0
            side = "long" if shares > 0 else "short" if shares < 0 else "flat"
            self._positions.append(
                {
                    "date": as_of.isoformat(),
                    "symbol": str(symbol),
                    "shares": shares,
                    "close": float(close_px) if close_px is not None else None,
                    "position_value": float(position_value) if position_value is not None else None,
                    "weight": float(weight),
                    "side": side,
                }
            )

    def record_constraint_hits(self, hits: List[ConstraintHit]) -> None:
        for hit in hits:
            self._constraint_hits.append(
                {
                    "as_of": hit.as_of,
                    "constraint": hit.constraint,
                    "symbol": hit.symbol,
                    "before": hit.before,
                    "after": hit.after,
                    "details": hit.details,
                }
            )

    def record_composite_weights(self, as_of: date, *, composite: CompositeDecision, final_weights: Dict[str, float]) -> None:
        for leg in composite.leg_results:
            leg_name = str(leg.name)
            for symbol, weight in (leg.target_weights or {}).items():
                self._composite_leg_weights.append(
                    {
                        "date": as_of.isoformat(),
                        "leg": leg_name,
                        "alpha": float(leg.alpha),
                        "symbol": str(symbol),
                        "weight": float(weight),
                    }
                )

        for symbol, weight in (composite.blended_weights_pre_constraints or {}).items():
            self._composite_blended_pre.append(
                {
                    "date": as_of.isoformat(),
                    "symbol": str(symbol),
                    "weight": float(weight),
                }
            )

        for symbol, weight in (final_weights or {}).items():
            self._composite_blended_post.append(
                {
                    "date": as_of.isoformat(),
                    "symbol": str(symbol),
                    "weight": float(weight),
                }
            )

    def write_artifacts(self) -> BacktestSummary:
        if self.config.output.save_resolved_config_json:
            resolved = self.config.to_dict()
            resolved["run_id"] = self.run_id
            resolved["submitted_at"] = self.submitted_at.isoformat()
            (self.output_dir / "config.resolved.json").write_text(
                json.dumps(resolved, indent=2, sort_keys=True),
                encoding="utf-8",
            )

        self.config.to_yaml(self.output_dir / "config.yaml")

        if self.config.output.save_config_parquet:
            # Store full config as JSON string to handle schema evolution
            resolved_for_parquet = self.config.to_dict()
            resolved_for_parquet["run_id"] = self.run_id
            resolved_for_parquet["submitted_at"] = self.submitted_at.isoformat()
            
            config_row = {
                "run_id": self.run_id,
                "submitted_at": self.submitted_at,
                "config_json": json.dumps(resolved_for_parquet, sort_keys=True)
            }
            pd.DataFrame([config_row]).to_parquet(self.output_dir / "config.parquet", index=False)

        trades_df = pd.DataFrame(self._trades)
        if self.config.output.save_trades:
            trades_df.to_csv(self.output_dir / "trades.csv", index=False)

        if self.config.output.save_trades_parquet and not trades_df.empty:
            trades_df.to_parquet(self.output_dir / "trades.parquet", index=False)



        daily_df = pd.DataFrame(self._days)
        if self.config.output.save_daily_metrics:
            daily_df.to_csv(self.output_dir / "daily_metrics.csv", index=False)

        if daily_df.empty:
            raise ValueError("No daily metrics were recorded; cannot summarize.")

        self._write_periodic_returns(daily_df)

        if self.config.output.save_metrics_parquet:
            daily_df.to_parquet(self.output_dir / "metrics_timeseries.parquet", index=False)
            rolling_df = self._compute_rolling_metrics(daily_df)
            rolling_df.to_parquet(self.output_dir / "metrics_rolling.parquet", index=False)

        if self.config.output.save_positions_snapshot:
            positions_df = pd.DataFrame(self._positions)
            positions_df.to_parquet(self.output_dir / "daily_positions.parquet", index=False)

        if self.config.output.save_constraint_hits:
            (self.output_dir / "constraint_hits.json").write_text(
                json.dumps(self._constraint_hits, indent=2, sort_keys=True),
                encoding="utf-8",
            )

        self._write_composite_artifacts()

        initial_cash = float(self.config.initial_cash)
        final_equity = float(daily_df["portfolio_value"].iloc[-1])
        total_return = final_equity / initial_cash - 1.0

        daily_returns = pd.to_numeric(daily_df["daily_return"], errors="coerce").fillna(0.0)
        realized_periods = max(0, len(daily_returns) - 1)
        annualized_return = _annualize_return(total_return, realized_periods)
        annualized_vol = _annualized_volatility(daily_returns.iloc[1:] if len(daily_returns) > 1 else daily_returns)
        sharpe_ratio = _sharpe(daily_returns.iloc[1:] if len(daily_returns) > 1 else daily_returns)

        max_drawdown = float(pd.to_numeric(daily_df["drawdown"], errors="coerce").min())

        summary = BacktestSummary(
            run_id=self.run_id,
            run_name=self.config.run_name,
            start_date=self.config.start_date.isoformat(),
            end_date=self.config.end_date.isoformat(),
            initial_cash=initial_cash,
            final_equity=final_equity,
            total_return=float(total_return),
            annualized_return=float(annualized_return),
            annualized_volatility=float(annualized_vol),
            sharpe_ratio=_safe_float(sharpe_ratio),
            max_drawdown=float(max_drawdown),
            trades=int(len(trades_df)),
        )

        (self.output_dir / "summary.json").write_text(
            json.dumps(summary.__dict__, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        if self.config.output.save_metrics_json:
            (self.output_dir / "metrics.json").write_text(
                json.dumps(summary.__dict__, indent=2, sort_keys=True),
                encoding="utf-8",
            )

        if self.config.output.save_summary_parquet:
            pd.DataFrame([summary.__dict__]).to_parquet(self.output_dir / "summary.parquet", index=False)

        self._update_run_index(summary)
        if self.config.output.save_run_index_parquet:
            self._update_run_index_parquet(summary)

        if self.config.output.adls_dir:
            # Optional artifact upload for CLI/local runs.
            from api.service.adls_uploader import upload_run_artifacts
            from api.service.security import parse_container_and_path
            from core.blob_storage import BlobStorageClient

            upload_run_artifacts(run_id=self.run_id, run_dir=self.output_dir, adls_dir=str(self.config.output.adls_dir))

            # Best-effort: upload run index parquet for cross-run search.
            index_path = self.output_dir.parent / "runs" / "_index" / "runs.parquet"
            if self.config.output.save_run_index_parquet and index_path.exists():
                container, prefix = parse_container_and_path(str(self.config.output.adls_dir))
                client = BlobStorageClient(container_name=container, ensure_container_exists=True)
                remote_path = f"{prefix.rstrip('/')}/_index/runs.parquet".lstrip("/")
                client.upload_file(str(index_path), remote_path)

        return summary

    def _write_composite_artifacts(self) -> None:
        if not self._composite_leg_weights and not self._composite_blended_pre and not self._composite_blended_post:
            return

        def _safe_name(value: str) -> str:
            cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value).strip())
            return cleaned or "leg"

        if self._composite_leg_weights:
            legs_df = pd.DataFrame(self._composite_leg_weights)
            if not legs_df.empty:
                legs_df["leg"] = legs_df["leg"].astype(str)
                for leg_name in sorted(set(legs_df["leg"].tolist())):
                    out_dir = self.output_dir / "legs" / _safe_name(leg_name)
                    out_dir.mkdir(parents=True, exist_ok=True)
                    legs_df[legs_df["leg"] == leg_name].to_csv(out_dir / "weights.csv", index=False)

        blend_dir = self.output_dir / "blend"
        blend_dir.mkdir(parents=True, exist_ok=True)
        if self._composite_blended_pre:
            pd.DataFrame(self._composite_blended_pre).to_csv(blend_dir / "blended_pre_constraints.csv", index=False)
        if self._composite_blended_post:
            pd.DataFrame(self._composite_blended_post).to_csv(blend_dir / "blended_post_constraints.csv", index=False)

    def _compute_rolling_metrics(self, daily_df: pd.DataFrame) -> pd.DataFrame:
        if daily_df is None or daily_df.empty:
            return pd.DataFrame()

        working = daily_df.copy()
        working["date"] = pd.to_datetime(working["date"], errors="coerce")
        working = working.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        if working.empty:
            return pd.DataFrame()

        working["daily_return"] = pd.to_numeric(working.get("daily_return"), errors="coerce").fillna(0.0)
        working["portfolio_value"] = pd.to_numeric(working.get("portfolio_value"), errors="coerce")
        working["turnover"] = pd.to_numeric(working.get("turnover"), errors="coerce").fillna(0.0)
        working["commission"] = pd.to_numeric(working.get("commission"), errors="coerce").fillna(0.0)
        working["slippage_cost"] = pd.to_numeric(working.get("slippage_cost"), errors="coerce").fillna(0.0)
        working["n_trades"] = pd.to_numeric(working.get("n_trades"), errors="coerce").fillna(0.0)
        working["gross_exposure"] = pd.to_numeric(working.get("gross_exposure"), errors="coerce").fillna(0.0)
        working["net_exposure"] = pd.to_numeric(working.get("net_exposure"), errors="coerce").fillna(0.0)

        # Long-form rolling metrics: one row per (date, window_days).
        windows = [21, 63, 126, 252]
        dr = working["daily_return"].astype(float)
        pv = working["portfolio_value"].astype(float)

        rows: List[pd.DataFrame] = []
        for window in windows:
            w = int(window)
            if w <= 1:
                continue

            compounded = (1.0 + dr).rolling(w).apply(lambda x: float(np.prod(x)), raw=True) - 1.0
            vol = dr.rolling(w).std(ddof=0) * math.sqrt(252)
            mean = dr.rolling(w).mean() * 252
            sharpe = mean / (vol.replace(0.0, pd.NA))

            max_dd = pv.rolling(w).apply(_rolling_max_drawdown, raw=False)

            out = pd.DataFrame(
                {
                    "date": working["date"].dt.date.astype(str),
                    "window_days": w,
                    "rolling_return": compounded.astype(float),
                    "rolling_volatility": vol.astype(float),
                    "rolling_sharpe": pd.to_numeric(sharpe, errors="coerce"),
                    "rolling_max_drawdown": pd.to_numeric(max_dd, errors="coerce"),
                    "turnover_sum": working["turnover"].rolling(w).sum().astype(float),
                    "commission_sum": working["commission"].rolling(w).sum().astype(float),
                    "slippage_cost_sum": working["slippage_cost"].rolling(w).sum().astype(float),
                    "n_trades_sum": working["n_trades"].rolling(w).sum().astype(float),
                    "gross_exposure_avg": working["gross_exposure"].rolling(w).mean().astype(float),
                    "net_exposure_avg": working["net_exposure"].rolling(w).mean().astype(float),
                }
            )
            rows.append(out)

        if not rows:
            return pd.DataFrame()

        combined = pd.concat(rows, ignore_index=True)
        return combined.sort_values(["date", "window_days"]).reset_index(drop=True)

    def _write_periodic_returns(self, daily_df: pd.DataFrame) -> None:
        if "date" not in daily_df.columns or "portfolio_value" not in daily_df.columns:
            return

        working = daily_df[["date", "portfolio_value"]].copy()
        working["date"] = pd.to_datetime(working["date"], errors="coerce")
        working["portfolio_value"] = pd.to_numeric(working["portfolio_value"], errors="coerce")
        working = working.dropna(subset=["date", "portfolio_value"]).sort_values("date")
        if working.empty:
            return

        working["year"] = working["date"].dt.year
        working["month"] = working["date"].dt.month

        month_start = working.groupby(["year", "month"])["portfolio_value"].first()
        month_end = working.groupby(["year", "month"])["portfolio_value"].last()
        monthly_return = (month_end / month_start - 1.0).reset_index(name="return")

        pivot = monthly_return.pivot(index="year", columns="month", values="return").reindex(columns=range(1, 13))
        pivot.columns = [calendar.month_abbr[m] for m in pivot.columns]

        year_start = working.groupby("year")["portfolio_value"].first()
        year_end = working.groupby("year")["portfolio_value"].last()
        yearly_return = (year_end / year_start - 1.0).to_dict()

        pivot.insert(0, "Year", pivot.index)
        pivot["Yearly"] = pivot.index.map(yearly_return)
        pivot.reset_index(drop=True, inplace=True)

        pivot.to_csv(self.output_dir / "monthly_returns.csv", index=False)
        pivot.to_csv(self.output_dir / "returns_monthly.csv", index=False)

        # Quarterly returns (pivot)
        working["quarter"] = ((working["date"].dt.month - 1) // 3 + 1).astype(int)
        q_start = working.groupby(["year", "quarter"])["portfolio_value"].first()
        q_end = working.groupby(["year", "quarter"])["portfolio_value"].last()
        q_ret = (q_end / q_start - 1.0).reset_index(name="return")
        q_pivot = q_ret.pivot(index="year", columns="quarter", values="return").reindex(columns=range(1, 5))
        q_pivot.columns = [f"Q{q}" for q in q_pivot.columns]
        q_pivot.insert(0, "Year", q_pivot.index)
        q_pivot["Yearly"] = q_pivot["Year"].map(yearly_return)
        q_pivot.reset_index(drop=True, inplace=True)
        q_pivot.to_csv(self.output_dir / "returns_quarterly.csv", index=False)

        # Yearly returns (long form)
        y_df = pd.DataFrame({"year": list(yearly_return.keys()), "return": list(yearly_return.values())}).sort_values(
            "year"
        )
        y_df.to_csv(self.output_dir / "returns_yearly.csv", index=False)

    def _update_run_index(self, summary: BacktestSummary) -> None:
        index_path = self.output_dir.parent / "run_index.csv"
        lock = FileLock(str(index_path) + ".lock")
        row = {
            "run_id": summary.run_id,
            "run_name": summary.run_name or "",
            "submitted_at": self.submitted_at.isoformat(),
            "strategy": self.config.strategy.class_name,
            "start_date": summary.start_date,
            "end_date": summary.end_date,
            "total_return": summary.total_return,
            "sharpe_ratio": summary.sharpe_ratio if summary.sharpe_ratio is not None else "",
            "max_drawdown": summary.max_drawdown,
            "final_equity": summary.final_equity,
            "trades": summary.trades,
        }

        with lock:
            if index_path.exists():
                df = pd.read_csv(index_path)
                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            else:
                df = pd.DataFrame([row])
            df.to_csv(index_path, index=False)

    def _update_run_index_parquet(self, summary: BacktestSummary) -> None:
        index_dir = self.output_dir.parent / "runs" / "_index"
        index_dir.mkdir(parents=True, exist_ok=True)
        index_path = index_dir / "runs.parquet"
        lock = FileLock(str(index_path) + ".lock")
        row = {
            "run_id": summary.run_id,
            "run_name": summary.run_name or "",
            "submitted_at": self.submitted_at.isoformat(),
            "strategy": self.config.strategy.class_name,
            "sizing": self.config.sizing.class_name,
            "start_date": summary.start_date,
            "end_date": summary.end_date,
            "total_return": summary.total_return,
            "sharpe_ratio": summary.sharpe_ratio if summary.sharpe_ratio is not None else None,
            "max_drawdown": summary.max_drawdown,
            "final_equity": summary.final_equity,
            "trades": summary.trades,
            "output_dir": str(self.output_dir),
        }

        with lock:
            if index_path.exists():
                df = pd.read_parquet(index_path)
                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            else:
                df = pd.DataFrame([row])
            df.to_parquet(index_path, index=False)
