from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from filelock import FileLock

from asset_allocation.backtest.config import BacktestConfig
from asset_allocation.backtest.models import BacktestSummary, TradeFill
from asset_allocation.backtest.portfolio import Portfolio


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


@dataclass
class Reporter:
    config: BacktestConfig
    run_id: str
    output_dir: Path
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    _trades: List[Dict[str, Any]] = field(default_factory=list)
    _days: List[Dict[str, Any]] = field(default_factory=list)

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
            }
        )

    def write_artifacts(self) -> BacktestSummary:
        self.config.to_yaml(self.output_dir / "config.yaml")

        trades_df = pd.DataFrame(self._trades)
        if self.config.output.save_trades:
            trades_df.to_csv(self.output_dir / "trades.csv", index=False)

        daily_df = pd.DataFrame(self._days)
        if self.config.output.save_daily_metrics:
            daily_df.to_csv(self.output_dir / "daily_metrics.csv", index=False)

        if daily_df.empty:
            raise ValueError("No daily metrics were recorded; cannot summarize.")

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

        self._update_run_index(summary)
        return summary

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

