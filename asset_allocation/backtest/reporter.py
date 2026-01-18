from __future__ import annotations

import json
import math
import calendar
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from filelock import FileLock

from asset_allocation.backtest.config import BacktestConfig
from asset_allocation.backtest.constraints import ConstraintHit
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
    _positions: List[Dict[str, Any]] = field(default_factory=list)
    _constraint_hits: List[Dict[str, Any]] = field(default_factory=list)

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

        trades_df = pd.DataFrame(self._trades)
        if self.config.output.save_trades:
            trades_df.to_csv(self.output_dir / "trades.csv", index=False)

        daily_df = pd.DataFrame(self._days)
        if self.config.output.save_daily_metrics:
            daily_df.to_csv(self.output_dir / "daily_metrics.csv", index=False)

        if daily_df.empty:
            raise ValueError("No daily metrics were recorded; cannot summarize.")

        self._write_periodic_returns(daily_df)

        if self.config.output.save_metrics_parquet:
            daily_df.to_parquet(self.output_dir / "metrics_timeseries.parquet", index=False)

        if self.config.output.save_positions_snapshot:
            positions_df = pd.DataFrame(self._positions)
            positions_df.to_parquet(self.output_dir / "daily_positions.parquet", index=False)

        if self.config.output.save_constraint_hits:
            (self.output_dir / "constraint_hits.json").write_text(
                json.dumps(self._constraint_hits, indent=2, sort_keys=True),
                encoding="utf-8",
            )

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

        self._update_run_index(summary)
        if self.config.output.save_run_index_parquet:
            self._update_run_index_parquet(summary)

        if self.config.output.adls_dir:
            # Optional artifact upload for CLI/local runs.
            from asset_allocation.backtest.service.adls_uploader import upload_run_artifacts
            from asset_allocation.backtest.service.security import parse_container_and_path
            from scripts.common.blob_storage import BlobStorageClient

            upload_run_artifacts(run_id=self.run_id, run_dir=self.output_dir, adls_dir=str(self.config.output.adls_dir))

            # Best-effort: upload run index parquet for cross-run search.
            index_path = self.output_dir.parent / "runs" / "_index" / "runs.parquet"
            if self.config.output.save_run_index_parquet and index_path.exists():
                container, prefix = parse_container_and_path(str(self.config.output.adls_dir))
                client = BlobStorageClient(container_name=container, ensure_container_exists=True)
                remote_path = f"{prefix.rstrip('/')}/_index/runs.parquet".lstrip("/")
                client.upload_file(str(index_path), remote_path)

        return summary

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
