from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable

import numpy as np
import pandas as pd

from core.backtest_artifacts import (
    list_artifacts,
    read_json_artifact,
    read_parquet_artifact,
    write_json_artifact,
    write_manifest,
    write_parquet_artifact,
    write_text_artifact,
)
from core.backtest_repository import BacktestRepository
from core.postgres import connect
from core.ranking_engine import service as ranking_service
from core.ranking_engine.contracts import RankingSchemaConfig
from core.ranking_repository import RankingRepository
from core.strategy_engine import StrategyConfig, UniverseDefinition
from core.strategy_engine.exit_rules import ExitRuleEvaluator
from core.strategy_engine.position_state import PositionState, PriceBar
from core.strategy_engine import universe as universe_service
from core.strategy_repository import StrategyRepository
from core.universe_repository import UniverseRepository

logger = logging.getLogger(__name__)

_PRICE_TABLE = "market_data"
_PRICE_COLUMNS = {"open", "high", "low", "close", "volume"}


@dataclass(frozen=True)
class ResolvedBacktestDefinition:
    strategy_name: str
    strategy_version: int | None
    strategy_config: StrategyConfig
    strategy_config_raw: dict[str, Any]
    strategy_universe: UniverseDefinition
    ranking_schema_name: str
    ranking_schema_version: int | None
    ranking_schema: RankingSchemaConfig
    ranking_universe_name: str | None
    ranking_universe_version: int | None
    ranking_universe: UniverseDefinition


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _session_bounds(ts: datetime) -> tuple[datetime, datetime]:
    start = datetime.combine(ts.date(), time.min, tzinfo=timezone.utc)
    end = datetime.combine(ts.date(), time.max, tzinfo=timezone.utc)
    return start, end


def _normalize_timestamp_value(value: Any, *, kind: str) -> datetime:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Unable to normalize timestamp value: {value!r}")
    if kind == "slower":
        parsed = parsed.normalize()
    return parsed.to_pydatetime()


def _bounds_for_spec(spec: universe_service.UniverseTableSpec, start_ts: datetime, end_ts: datetime) -> tuple[Any, Any]:
    if spec.as_of_kind == "intraday":
        return start_ts, end_ts
    return start_ts.date(), end_ts.date()


def _load_run_schedule(
    dsn: str,
    *,
    table_name: str,
    table_spec: universe_service.UniverseTableSpec,
    start_ts: datetime,
    end_ts: datetime,
    bar_size: str | None,
) -> list[datetime]:
    start_bound, end_bound = _bounds_for_spec(table_spec, start_ts, end_ts)
    sql = f"""
        SELECT DISTINCT {universe_service._quote_identifier(table_spec.as_of_column)} AS as_of_value
        FROM "gold".{universe_service._quote_identifier(table_name)}
        WHERE {universe_service._quote_identifier(table_spec.as_of_column)} >= %s
          AND {universe_service._quote_identifier(table_spec.as_of_column)} <= %s
    """
    params: list[Any] = [start_bound, end_bound]
    if bar_size and "bar_size" in table_spec.columns:
        sql += f" AND {universe_service._quote_identifier('bar_size')} = %s"
        params.append(bar_size)
    sql += " ORDER BY as_of_value"

    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [_normalize_timestamp_value(row[0], kind=table_spec.as_of_kind) for row in rows if row and row[0] is not None]


def _load_exact_coverage(
    dsn: str,
    *,
    table_name: str,
    table_spec: universe_service.UniverseTableSpec,
    start_ts: datetime,
    end_ts: datetime,
    bar_size: str | None,
) -> set[datetime]:
    return set(
        _load_run_schedule(
            dsn,
            table_name=table_name,
            table_spec=table_spec,
            start_ts=start_ts,
            end_ts=end_ts,
            bar_size=bar_size,
        )
    )


def _value_series(
    raw: pd.Series,
    *,
    column_spec: universe_service.UniverseColumnSpec,
) -> pd.Series:
    if column_spec.value_kind == "number":
        return pd.to_numeric(raw, errors="coerce")
    if column_spec.value_kind == "boolean":
        return raw.astype("boolean")
    if column_spec.value_kind in {"date", "datetime"}:
        return pd.to_datetime(raw, utc=True, errors="coerce")
    return raw.astype("string")


def _prepare_loaded_frame(
    frame: pd.DataFrame,
    *,
    table_name: str,
    table_spec: universe_service.UniverseTableSpec,
    selected_columns: Iterable[str],
) -> pd.DataFrame:
    normalized_columns = list(selected_columns)
    if frame.empty:
        return pd.DataFrame(columns=["as_of", "symbol", *[f"{table_name}__{name}" for name in normalized_columns]])
    out = frame.copy()
    out["symbol"] = out["symbol"].astype("string").str.strip().str.upper()
    out["as_of"] = pd.to_datetime(out["as_of"], utc=True, errors="coerce")
    for column_name in normalized_columns:
        out[f"{table_name}__{column_name}"] = _value_series(out[column_name], column_spec=table_spec.columns[column_name])
    return out[["as_of", "symbol", *[f"{table_name}__{name}" for name in normalized_columns]]]


def _load_intraday_session_frames(
    dsn: str,
    *,
    table_specs: dict[str, universe_service.UniverseTableSpec],
    required_columns: dict[str, set[str]],
    session_start: datetime,
    session_end: datetime,
    bar_size: str | None,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    with connect(dsn) as conn:
        for table_name, columns in required_columns.items():
            spec = table_specs[table_name]
            if spec.as_of_kind != "intraday":
                continue
            selected_columns = sorted(columns)
            select_parts = [
                f"{universe_service._quote_identifier(spec.as_of_column)} AS as_of",
                f'{universe_service._quote_identifier("symbol")} AS symbol',
            ]
            select_parts.extend(universe_service._quote_identifier(column) for column in selected_columns)
            sql = f"""
                SELECT {", ".join(select_parts)}
                FROM "gold".{universe_service._quote_identifier(table_name)}
                WHERE {universe_service._quote_identifier(spec.as_of_column)} >= %s
                  AND {universe_service._quote_identifier(spec.as_of_column)} <= %s
            """
            params: list[Any] = [session_start, session_end]
            if bar_size and "bar_size" in spec.columns:
                sql += f" AND {universe_service._quote_identifier('bar_size')} = %s"
                params.append(bar_size)
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                columns_in_result = [desc.name for desc in cur.description]
            frame = pd.DataFrame(rows, columns=columns_in_result)
            frames[table_name] = _prepare_loaded_frame(
                frame,
                table_name=table_name,
                table_spec=spec,
                selected_columns=selected_columns,
            )
    return frames


def _load_slow_frames(
    dsn: str,
    *,
    table_specs: dict[str, universe_service.UniverseTableSpec],
    required_columns: dict[str, set[str]],
    as_of_ts: datetime,
    bar_size: str | None,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    with connect(dsn) as conn:
        for table_name, columns in required_columns.items():
            spec = table_specs[table_name]
            if spec.as_of_kind == "intraday":
                continue
            selected_columns = sorted(columns)
            select_parts = [
                f"{universe_service._quote_identifier(spec.as_of_column)} AS as_of",
                f'{universe_service._quote_identifier("symbol")} AS symbol',
            ]
            select_parts.extend(universe_service._quote_identifier(column) for column in selected_columns)
            sql = f"""
                SELECT DISTINCT ON ({universe_service._quote_identifier('symbol')})
                    {", ".join(select_parts)}
                FROM "gold".{universe_service._quote_identifier(table_name)}
                WHERE {universe_service._quote_identifier(spec.as_of_column)} <= %s
            """
            params: list[Any] = [as_of_ts.date()]
            if bar_size and "bar_size" in spec.columns:
                sql += f" AND {universe_service._quote_identifier('bar_size')} = %s"
                params.append(bar_size)
            sql += f"""
                ORDER BY
                    {universe_service._quote_identifier('symbol')},
                    {universe_service._quote_identifier(spec.as_of_column)} DESC NULLS LAST
            """
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                columns_in_result = [desc.name for desc in cur.description]
            frame = pd.DataFrame(rows, columns=columns_in_result)
            frames[table_name] = _prepare_loaded_frame(
                frame,
                table_name=table_name,
                table_spec=spec,
                selected_columns=selected_columns,
            )
    return frames


def _snapshot_for_timestamp(
    ts: datetime,
    *,
    intraday_frames: dict[str, pd.DataFrame],
    slow_frames: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for frame in intraday_frames.values():
        if frame.empty:
            continue
        exact = frame[frame["as_of"] == pd.Timestamp(ts)]
        if exact.empty:
            continue
        frames.append(exact.drop(columns=["as_of"], errors="ignore"))
    for frame in slow_frames.values():
        if frame.empty:
            continue
        frames.append(frame.drop(columns=["as_of"], errors="ignore"))

    merged: pd.DataFrame | None = None
    for frame in frames:
        frame = frame.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
        if merged is None:
            merged = frame.copy()
        else:
            merged = merged.merge(frame, on="symbol", how="outer")
    if merged is None:
        return pd.DataFrame(columns=["date", "symbol"])
    merged["date"] = pd.Timestamp(ts)
    merged = merged.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    return merged


def _resolve_strategy_universe(
    dsn: str,
    *,
    strategy_config: StrategyConfig,
    fallback_universe: UniverseDefinition,
) -> UniverseDefinition:
    if strategy_config.universe is not None:
        return strategy_config.universe
    if strategy_config.universeConfigName:
        record = UniverseRepository(dsn).get_universe_config(strategy_config.universeConfigName)
        if record:
            return UniverseDefinition.model_validate(record.get("config") or {})
    return fallback_universe


def resolve_backtest_definition(
    dsn: str,
    *,
    strategy_name: str,
    strategy_version: int | None = None,
) -> ResolvedBacktestDefinition:
    strategy_repo = StrategyRepository(dsn)
    ranking_repo = RankingRepository(dsn)
    universe_repo = UniverseRepository(dsn)

    strategy_revision = strategy_repo.get_strategy_revision(strategy_name, strategy_version)
    if strategy_revision:
        strategy_config_raw = dict(strategy_revision.get("config") or {})
    else:
        strategy_record = strategy_repo.get_strategy(strategy_name)
        if not strategy_record:
            raise ValueError(f"Strategy '{strategy_name}' not found.")
        strategy_config_raw = dict(strategy_record.get("config") or {})

    strategy_config = StrategyConfig.model_validate(strategy_config_raw)
    ranking_schema_name = str(
        (strategy_revision or {}).get("ranking_schema_name") or strategy_config.rankingSchemaName or ""
    ).strip()
    if not ranking_schema_name:
        raise ValueError(f"Strategy '{strategy_name}' does not reference a ranking schema.")

    ranking_schema_version = (
        int(strategy_revision["ranking_schema_version"])
        if strategy_revision and strategy_revision.get("ranking_schema_version") is not None
        else None
    )
    ranking_record = ranking_repo.get_ranking_schema_revision(ranking_schema_name, ranking_schema_version)
    if not ranking_record:
        raise ValueError(f"Ranking schema '{ranking_schema_name}' not found.")
    ranking_schema = RankingSchemaConfig.model_validate(ranking_record.get("config") or {})

    ranking_universe_name = str(
        (strategy_revision or {}).get("universe_name")
        or ranking_record.get("config", {}).get("universeConfigName")
        or ranking_schema.universeConfigName
        or ""
    ).strip() or None
    if not ranking_universe_name:
        raise ValueError(f"Ranking schema '{ranking_schema_name}' does not reference a universe config.")
    ranking_universe_version = (
        int(strategy_revision["universe_version"])
        if strategy_revision and strategy_revision.get("universe_version") is not None
        else None
    )
    universe_record = universe_repo.get_universe_config_revision(ranking_universe_name, ranking_universe_version)
    if not universe_record:
        raise ValueError(f"Universe config '{ranking_universe_name}' not found.")
    ranking_universe = UniverseDefinition.model_validate(universe_record.get("config") or {})
    strategy_universe = _resolve_strategy_universe(
        dsn,
        strategy_config=strategy_config,
        fallback_universe=ranking_universe,
    )
    return ResolvedBacktestDefinition(
        strategy_name=strategy_name,
        strategy_version=(int(strategy_revision["version"]) if strategy_revision else None),
        strategy_config=strategy_config,
        strategy_config_raw=strategy_config_raw,
        strategy_universe=strategy_universe,
        ranking_schema_name=ranking_schema_name,
        ranking_schema_version=int(ranking_record["version"]),
        ranking_schema=ranking_schema,
        ranking_universe_name=ranking_universe_name,
        ranking_universe_version=int(universe_record["version"]),
        ranking_universe=ranking_universe,
    )


def _required_columns(definition: ResolvedBacktestDefinition) -> dict[str, set[str]]:
    required = ranking_service._collect_required_columns(
        definition.strategy_universe,
        definition.ranking_universe,
        definition.ranking_schema,
    )
    required.setdefault(_PRICE_TABLE, set()).update(_PRICE_COLUMNS)
    for rule in definition.strategy_config.exits:
        if rule.atrColumn:
            required[_PRICE_TABLE].add(str(rule.atrColumn))
    return required


def validate_backtest_submission(
    dsn: str,
    *,
    definition: ResolvedBacktestDefinition,
    start_ts: datetime,
    end_ts: datetime,
    bar_size: str | None,
) -> list[datetime]:
    table_specs = universe_service._load_gold_table_specs(dsn)
    required = _required_columns(definition)
    missing_tables = [name for name in required if name not in table_specs]
    if missing_tables:
        raise ValueError(f"Missing required gold tables: {sorted(missing_tables)}")

    price_spec = table_specs[_PRICE_TABLE]
    intraday_tables = sorted(name for name, spec in table_specs.items() if name in required and spec.as_of_kind == "intraday")
    schedule_source_name = _PRICE_TABLE if price_spec.as_of_kind == "intraday" else (intraday_tables[0] if intraday_tables else _PRICE_TABLE)
    schedule_source = table_specs[schedule_source_name]
    schedule = _load_run_schedule(
        dsn,
        table_name=schedule_source_name,
        table_spec=schedule_source,
        start_ts=start_ts,
        end_ts=end_ts,
        bar_size=bar_size,
    )
    if len(schedule) < 2:
        raise ValueError("Backtest window must resolve to at least two bars.")
    if intraday_tables and price_spec.as_of_kind != "intraday":
        raise ValueError(
            "Execution price table 'market_data' is not intraday while intraday feature tables are required."
        )

    schedule_set = set(schedule)
    for table_name in intraday_tables:
        coverage = _load_exact_coverage(
            dsn,
            table_name=table_name,
            table_spec=table_specs[table_name],
            start_ts=start_ts,
            end_ts=end_ts,
            bar_size=bar_size,
        )
        missing = sorted(schedule_set.difference(coverage))
        if missing:
            sample = ", ".join(item.isoformat() for item in missing[:5])
            raise ValueError(
                f"Intraday feature coverage gap for gold.{table_name}; missing {len(missing)} rebalance bars, sample={sample}"
            )
    return schedule


def _score_snapshot(
    snapshot: pd.DataFrame,
    *,
    definition: ResolvedBacktestDefinition,
    rebalance_ts: datetime,
) -> pd.DataFrame:
    if snapshot.empty:
        return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])
    filtered = snapshot[
        ranking_service._evaluate_universe_mask(snapshot, definition.strategy_universe.root)
        & ranking_service._evaluate_universe_mask(snapshot, definition.ranking_universe.root)
    ].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])

    group_scores: list[tuple[str, float, pd.Series]] = []
    required_masks: list[pd.Series] = []
    for group in definition.ranking_schema.groups:
        group_series, group_required_masks = ranking_service._score_group(filtered, group)
        group_scores.append((group.name, group.weight, group_series))
        required_masks.extend(group_required_masks)

    if required_masks:
        keep_mask = pd.concat(required_masks, axis=1).all(axis=1)
        filtered = filtered[keep_mask].copy()
        group_scores = [(name, weight, series.loc[filtered.index]) for name, weight, series in group_scores]
        if filtered.empty:
            return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])

    weighted_total = pd.Series(0.0, index=filtered.index)
    total_weight = 0.0
    for _name, weight, series in group_scores:
        weighted_total = weighted_total.add(series * weight, fill_value=0.0)
        total_weight += weight
    if total_weight <= 0:
        raise ValueError("Ranking schema produced zero total group weight.")
    filtered["score"] = weighted_total / total_weight
    filtered["score"] = ranking_service._apply_transforms(
        filtered["score"],
        filtered["date"],
        definition.ranking_schema.overallTransforms,
    )
    filtered = filtered.dropna(subset=["score"]).copy()
    if filtered.empty:
        return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])

    filtered = filtered.sort_values(["score", "symbol"], ascending=[False, True]).reset_index(drop=True)
    filtered["ordinal"] = np.arange(1, len(filtered) + 1)
    top_n = min(definition.strategy_config.topN, len(filtered))
    filtered["selected"] = filtered["ordinal"] <= top_n
    target_weight = 1.0 / top_n if top_n > 0 else 0.0
    filtered["target_weight"] = np.where(filtered["selected"], target_weight, 0.0)
    filtered["rebalance_ts"] = pd.Timestamp(rebalance_ts)
    return filtered[["rebalance_ts", "symbol", "score", "ordinal", "selected", "target_weight"]]


def _market_row(snapshot: pd.DataFrame, symbol: str) -> pd.Series | None:
    matches = snapshot[snapshot["symbol"] == symbol]
    if matches.empty:
        return None
    return matches.iloc[0]


def _price_bar(ts: datetime, row: pd.Series) -> PriceBar:
    features = {
        column.removeprefix(f"{_PRICE_TABLE}__"): row[column]
        for column in row.index
        if str(column).startswith(f"{_PRICE_TABLE}__")
    }
    return PriceBar(
        date=ts,
        open=_maybe_float(features.get("open")),
        high=_maybe_float(features.get("high")),
        low=_maybe_float(features.get("low")),
        close=_maybe_float(features.get("close")),
        features=features,
    )


def _maybe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return float(int(value))
    try:
        return float(value)
    except Exception:
        return None


def _costs_from_raw_config(raw: dict[str, Any]) -> tuple[float, float]:
    costs = raw.get("costs") if isinstance(raw, dict) else None
    if not isinstance(costs, dict):
        return 0.0, 0.0
    commission_bps = float(costs.get("commissionBps") or costs.get("commission_bps") or 0.0)
    slippage_bps = float(costs.get("slippageBps") or costs.get("slippage_bps") or 0.0)
    return commission_bps, slippage_bps


def _execute_trade(
    *,
    trades: list[dict[str, Any]],
    ts: datetime,
    symbol: str,
    quantity_delta: float,
    price: float,
    cash: float,
    commission_bps: float,
    slippage_bps: float,
) -> tuple[float, float, float]:
    if math.isclose(quantity_delta, 0.0, abs_tol=1e-12):
        return cash, 0.0, 0.0
    notional = float(quantity_delta * price)
    abs_notional = abs(notional)
    commission = abs_notional * commission_bps / 10000.0
    slippage = abs_notional * slippage_bps / 10000.0
    cash_after = cash - notional - commission - slippage
    trades.append(
        {
            "execution_date": ts.isoformat(),
            "symbol": symbol,
            "quantity": float(quantity_delta),
            "price": float(price),
            "notional": float(notional),
            "commission": float(commission),
            "slippage_cost": float(slippage),
            "cash_after": float(cash_after),
        }
    )
    return cash_after, commission, slippage


def _compute_summary(timeseries: pd.DataFrame, trades: pd.DataFrame, *, run_id: str, run_name: str | None) -> dict[str, Any]:
    if timeseries.empty:
        return {
            "run_id": run_id,
            "run_name": run_name,
            "total_return": 0.0,
            "annualized_return": 0.0,
            "annualized_volatility": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "trades": int(len(trades)),
            "initial_cash": 0.0,
            "final_equity": 0.0,
        }
    initial_cash = float(timeseries["portfolio_value"].iloc[0])
    final_equity = float(timeseries["portfolio_value"].iloc[-1])
    total_return = (final_equity / initial_cash - 1.0) if initial_cash else 0.0
    returns = pd.to_numeric(timeseries["daily_return"], errors="coerce").fillna(0.0)
    periods = max(len(returns), 1)
    annualization = 252.0
    annualized_return = (1.0 + total_return) ** (annualization / periods) - 1.0 if periods > 0 else 0.0
    annualized_volatility = float(returns.std(ddof=0) * math.sqrt(annualization)) if len(returns) > 1 else 0.0
    sharpe_ratio = annualized_return / annualized_volatility if annualized_volatility > 0 else 0.0
    max_drawdown = float(pd.to_numeric(timeseries["drawdown"], errors="coerce").min() or 0.0)
    return {
        "run_id": run_id,
        "run_name": run_name,
        "start_date": str(timeseries["date"].iloc[0]),
        "end_date": str(timeseries["date"].iloc[-1]),
        "total_return": float(total_return),
        "annualized_return": float(annualized_return),
        "annualized_volatility": float(annualized_volatility),
        "sharpe_ratio": float(sharpe_ratio),
        "max_drawdown": float(max_drawdown),
        "trades": int(len(trades)),
        "initial_cash": float(initial_cash),
        "final_equity": float(final_equity),
    }


def _compute_rolling_metrics(timeseries: pd.DataFrame, *, window_bars: int = 63) -> pd.DataFrame:
    if timeseries.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "window_days",
                "rolling_return",
                "rolling_volatility",
                "rolling_sharpe",
                "rolling_max_drawdown",
                "turnover_sum",
                "commission_sum",
                "slippage_cost_sum",
                "n_trades_sum",
                "gross_exposure_avg",
                "net_exposure_avg",
            ]
        )
    frame = timeseries.copy()
    returns = pd.to_numeric(frame["daily_return"], errors="coerce").fillna(0.0)
    frame["rolling_return"] = (1.0 + returns).rolling(window_bars).apply(lambda values: float(np.prod(values) - 1.0), raw=True)
    frame["rolling_volatility"] = returns.rolling(window_bars).std(ddof=0) * math.sqrt(252.0)
    frame["rolling_sharpe"] = np.where(
        frame["rolling_volatility"].fillna(0.0) > 0,
        frame["rolling_return"] * (252.0 / max(window_bars, 1)) / frame["rolling_volatility"],
        0.0,
    )
    frame["rolling_max_drawdown"] = frame["drawdown"].rolling(window_bars).min()
    frame["turnover_sum"] = pd.to_numeric(frame["turnover"], errors="coerce").fillna(0.0).rolling(window_bars).sum()
    frame["commission_sum"] = pd.to_numeric(frame["commission"], errors="coerce").fillna(0.0).rolling(window_bars).sum()
    frame["slippage_cost_sum"] = pd.to_numeric(frame["slippage_cost"], errors="coerce").fillna(0.0).rolling(window_bars).sum()
    frame["n_trades_sum"] = pd.to_numeric(frame["trade_count"], errors="coerce").fillna(0.0).rolling(window_bars).sum()
    frame["gross_exposure_avg"] = pd.to_numeric(frame["gross_exposure"], errors="coerce").fillna(0.0).rolling(window_bars).mean()
    frame["net_exposure_avg"] = pd.to_numeric(frame["net_exposure"], errors="coerce").fillna(0.0).rolling(window_bars).mean()
    frame["window_days"] = window_bars
    return frame[
        [
            "date",
            "window_days",
            "rolling_return",
            "rolling_volatility",
            "rolling_sharpe",
            "rolling_max_drawdown",
            "turnover_sum",
            "commission_sum",
            "slippage_cost_sum",
            "n_trades_sum",
            "gross_exposure_avg",
            "net_exposure_avg",
        ]
    ].copy()


def execute_backtest_run(
    dsn: str,
    *,
    run_id: str,
    execution_name: str | None = None,
) -> dict[str, Any]:
    repo = BacktestRepository(dsn)
    run = repo.get_run(run_id)
    if not run:
        raise ValueError(f"Run '{run_id}' not found.")
    if run["status"] == "queued":
        repo.start_run(run_id, execution_name=execution_name)
        run = repo.get_run(run_id)
    if not run:
        raise ValueError(f"Run '{run_id}' not found after start.")

    start_ts = _ensure_utc(run["start_ts"])
    end_ts = _ensure_utc(run["end_ts"])
    definition = resolve_backtest_definition(
        dsn,
        strategy_name=str(run["strategy_name"] or ""),
        strategy_version=run.get("strategy_version"),
    )
    schedule = validate_backtest_submission(
        dsn,
        definition=definition,
        start_ts=start_ts,
        end_ts=end_ts,
        bar_size=str(run.get("bar_size") or "").strip() or None,
    )

    table_specs = universe_service._load_gold_table_specs(dsn)
    required_columns = _required_columns(definition)
    grouped_schedule: dict[date, list[datetime]] = defaultdict(list)
    for ts in schedule:
        grouped_schedule[ts.date()].append(ts)

    evaluator = ExitRuleEvaluator()
    commission_bps, slippage_bps = _costs_from_raw_config(definition.strategy_config_raw)
    cash = float(definition.strategy_config_raw.get("initialCash") or 100000.0)
    positions: dict[str, PositionState] = {}
    pending_target_weights: dict[str, float] = {}
    selection_trace_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    timeseries_rows: list[dict[str, Any]] = []
    log_lines = [f"run_id={run_id} strategy={definition.strategy_name} bars={len(schedule)}"]
    previous_equity = cash
    previous_close_by_symbol: dict[str, float] = {}
    first_signal_computed = False

    for session_date, session_schedule in grouped_schedule.items():
        session_start, session_end = _session_bounds(session_schedule[0])
        intraday_frames = _load_intraday_session_frames(
            dsn,
            table_specs=table_specs,
            required_columns=required_columns,
            session_start=session_start,
            session_end=session_end,
            bar_size=str(run.get("bar_size") or "").strip() or None,
        )
        slow_frames = _load_slow_frames(
            dsn,
            table_specs=table_specs,
            required_columns=required_columns,
            as_of_ts=session_schedule[-1],
            bar_size=str(run.get("bar_size") or "").strip() or None,
        )
        for index, current_ts in enumerate(session_schedule):
            snapshot = _snapshot_for_timestamp(current_ts, intraday_frames=intraday_frames, slow_frames=slow_frames)
            repo.update_heartbeat(run_id)
            if not first_signal_computed:
                initial_ranking = _score_snapshot(snapshot, definition=definition, rebalance_ts=current_ts)
                selection_trace_rows.extend(initial_ranking.to_dict("records"))
                pending_target_weights = {
                    str(row["symbol"]): float(row["target_weight"])
                    for row in initial_ranking.to_dict("records")
                    if bool(row["selected"])
                }
                first_signal_computed = True
                continue

            total_commission = 0.0
            total_slippage = 0.0
            trade_count = 0
            market_equity_open = cash

            for symbol, position in list(positions.items()):
                row = _market_row(snapshot, symbol)
                if row is None:
                    market_equity_open += position.quantity * previous_close_by_symbol.get(symbol, position.entry_price)
                    continue
                open_price = _maybe_float(row.get(f"{_PRICE_TABLE}__open")) or _maybe_float(row.get(f"{_PRICE_TABLE}__close")) or position.entry_price
                market_equity_open += position.quantity * open_price

            target_qty_by_symbol: dict[str, float] = {}
            if pending_target_weights:
                for symbol, target_weight in pending_target_weights.items():
                    row = _market_row(snapshot, symbol)
                    if row is None:
                        continue
                    open_price = _maybe_float(row.get(f"{_PRICE_TABLE}__open")) or _maybe_float(row.get(f"{_PRICE_TABLE}__close"))
                    if open_price is None or open_price <= 0:
                        continue
                    target_qty_by_symbol[symbol] = (market_equity_open * target_weight) / open_price

            all_symbols = sorted(set(positions.keys()) | set(target_qty_by_symbol.keys()))
            for symbol in all_symbols:
                row = _market_row(snapshot, symbol)
                if row is None:
                    continue
                open_price = _maybe_float(row.get(f"{_PRICE_TABLE}__open")) or _maybe_float(row.get(f"{_PRICE_TABLE}__close"))
                if open_price is None or open_price <= 0:
                    continue
                current_qty = positions[symbol].quantity if symbol in positions else 0.0
                target_qty = target_qty_by_symbol.get(symbol, 0.0)
                delta_qty = target_qty - current_qty
                if math.isclose(delta_qty, 0.0, abs_tol=1e-9):
                    continue
                cash, commission, slippage = _execute_trade(
                    trades=trade_rows,
                    ts=current_ts,
                    symbol=symbol,
                    quantity_delta=delta_qty,
                    price=open_price,
                    cash=cash,
                    commission_bps=commission_bps,
                    slippage_bps=slippage_bps,
                )
                total_commission += commission
                total_slippage += slippage
                trade_count += 1
                if target_qty <= 1e-9:
                    positions.pop(symbol, None)
                    previous_close_by_symbol.pop(symbol, None)
                else:
                    positions[symbol] = PositionState(
                        symbol=symbol,
                        entry_date=current_ts,
                        entry_price=open_price,
                        quantity=float(target_qty),
                    )

            pending_target_weights = {}

            for symbol, position in list(positions.items()):
                row = _market_row(snapshot, symbol)
                if row is None:
                    continue
                bar = _price_bar(current_ts, row)
                evaluation = evaluator.evaluate_bar(definition.strategy_config, position, bar)
                positions[symbol] = evaluation.position_state
                if evaluation.decision is None:
                    previous_close_by_symbol[symbol] = bar.close or previous_close_by_symbol.get(symbol, position.entry_price)
                    continue
                cash, commission, slippage = _execute_trade(
                    trades=trade_rows,
                    ts=current_ts,
                    symbol=symbol,
                    quantity_delta=-position.quantity,
                    price=float(evaluation.decision.exit_price),
                    cash=cash,
                    commission_bps=commission_bps,
                    slippage_bps=slippage_bps,
                )
                total_commission += commission
                total_slippage += slippage
                trade_count += 1
                positions.pop(symbol, None)
                previous_close_by_symbol.pop(symbol, None)

            close_equity = cash
            gross_exposure = 0.0
            for symbol, position in positions.items():
                row = _market_row(snapshot, symbol)
                close_price = None
                if row is not None:
                    close_price = _maybe_float(row.get(f"{_PRICE_TABLE}__close")) or _maybe_float(row.get(f"{_PRICE_TABLE}__open"))
                if close_price is None:
                    close_price = previous_close_by_symbol.get(symbol, position.entry_price)
                previous_close_by_symbol[symbol] = float(close_price)
                position_value = float(position.quantity * close_price)
                close_equity += position_value
                gross_exposure += abs(position_value)

            period_return = (close_equity / previous_equity - 1.0) if previous_equity else 0.0
            running_peak = max([close_equity, *(row["portfolio_value"] for row in timeseries_rows)] if timeseries_rows else [close_equity])
            drawdown = (close_equity / running_peak - 1.0) if running_peak else 0.0
            timeseries_rows.append(
                {
                    "date": current_ts.isoformat(),
                    "portfolio_value": float(close_equity),
                    "drawdown": float(drawdown),
                    "daily_return": float(period_return),
                    "cumulative_return": float(close_equity / timeseries_rows[0]["portfolio_value"] - 1.0) if timeseries_rows else 0.0,
                    "cash": float(cash),
                    "gross_exposure": float(gross_exposure / close_equity) if close_equity else 0.0,
                    "net_exposure": float(gross_exposure / close_equity) if close_equity else 0.0,
                    "turnover": float(
                        sum(abs(trade["notional"]) for trade in trade_rows[-trade_count:]) / previous_equity
                    ) if previous_equity and trade_count else 0.0,
                    "commission": float(total_commission),
                    "slippage_cost": float(total_slippage),
                    "trade_count": int(trade_count),
                }
            )
            previous_equity = close_equity

            if index < len(session_schedule) - 1:
                ranking = _score_snapshot(snapshot, definition=definition, rebalance_ts=current_ts)
                selection_trace_rows.extend(ranking.to_dict("records"))
                pending_target_weights = {
                    str(row["symbol"]): float(row["target_weight"])
                    for row in ranking.to_dict("records")
                    if bool(row["selected"])
                }

    timeseries = pd.DataFrame(timeseries_rows)
    trades = pd.DataFrame(trade_rows)
    selection_trace = pd.DataFrame(selection_trace_rows)
    rolling_metrics = _compute_rolling_metrics(timeseries)
    summary = _compute_summary(
        timeseries,
        trades,
        run_id=run_id,
        run_name=run.get("run_name"),
    )

    write_json_artifact(run_id, "effective_config.json", {
        "strategy": definition.strategy_config_raw,
        "pins": {
            "strategyName": definition.strategy_name,
            "strategyVersion": definition.strategy_version,
            "rankingSchemaName": definition.ranking_schema_name,
            "rankingSchemaVersion": definition.ranking_schema_version,
            "universeName": definition.ranking_universe_name,
            "universeVersion": definition.ranking_universe_version,
        },
        "run": {
            "startTs": start_ts.isoformat(),
            "endTs": end_ts.isoformat(),
            "barSize": run.get("bar_size"),
        },
    })
    write_json_artifact(run_id, "summary.json", summary)
    write_parquet_artifact(run_id, "timeseries.parquet", timeseries)
    write_parquet_artifact(run_id, "rolling_metrics.parquet", rolling_metrics)
    write_parquet_artifact(run_id, "trades.parquet", trades)
    write_parquet_artifact(run_id, "selection_trace.parquet", selection_trace)
    write_text_artifact(run_id, "worker.log", "\n".join(log_lines))
    manifest_path = write_manifest(run_id)
    repo.complete_run(run_id, summary=summary, artifact_manifest_path=manifest_path)
    return {
        "summary": summary,
        "artifacts": list_artifacts(run_id),
    }


def load_summary(run_id: str, *, repo: BacktestRepository) -> dict[str, Any]:
    run = repo.get_run(run_id)
    if not run:
        raise ValueError(f"Run '{run_id}' not found.")
    summary = run.get("summary_json") or {}
    if isinstance(summary, dict) and summary:
        return summary
    artifact_summary = read_json_artifact(run_id, "summary.json")
    if artifact_summary is None:
        raise FileNotFoundError(f"Summary artifact missing for run '{run_id}'.")
    return artifact_summary


def load_timeseries(run_id: str) -> pd.DataFrame:
    return read_parquet_artifact(run_id, "timeseries.parquet")


def load_trades(run_id: str) -> pd.DataFrame:
    return read_parquet_artifact(run_id, "trades.parquet")


def load_rolling_metrics(run_id: str, *, window_days: int = 63) -> pd.DataFrame:
    artifact = read_parquet_artifact(run_id, "rolling_metrics.parquet")
    if not artifact.empty and "window_days" in artifact.columns:
        filtered = artifact[pd.to_numeric(artifact["window_days"], errors="coerce") == int(window_days)]
        if not filtered.empty:
            return filtered.reset_index(drop=True)
    return _compute_rolling_metrics(load_timeseries(run_id), window_bars=max(2, int(window_days)))

