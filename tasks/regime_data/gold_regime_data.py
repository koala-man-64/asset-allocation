from __future__ import annotations

import os
from datetime import datetime, timezone
from math import sqrt
from typing import Any

import pandas as pd

from core import core as mdc
from core.postgres import connect, copy_rows
from core.regime import build_regime_outputs, compute_curve_state, compute_trend_state
from core.regime_repository import RegimeRepository
from tasks.common import domain_artifacts
from tasks.common.job_trigger import ensure_api_awake_from_env
from tasks.common.system_health_markers import write_system_health_marker
from tasks.common.watermarks import save_last_success, save_watermarks

JOB_NAME = "gold-regime-job"
WATERMARK_KEY = "gold_regime_features"
_INPUTS_COLUMNS = (
    "as_of_date",
    "spy_close",
    "return_1d",
    "return_20d",
    "rvol_10d_ann",
    "vix_spot_close",
    "vix3m_close",
    "vix_slope",
    "trend_state",
    "curve_state",
    "vix_gt_32_streak",
    "inputs_complete_flag",
    "computed_at",
)
_HISTORY_COLUMNS = (
    "as_of_date",
    "effective_from_date",
    "model_name",
    "model_version",
    "regime_code",
    "regime_status",
    "matched_rule_id",
    "halt_flag",
    "halt_reason",
    "spy_return_20d",
    "rvol_10d_ann",
    "vix_spot_close",
    "vix3m_close",
    "vix_slope",
    "trend_state",
    "curve_state",
    "vix_gt_32_streak",
    "computed_at",
)
_TRANSITIONS_COLUMNS = (
    "model_name",
    "model_version",
    "effective_from_date",
    "prior_regime_code",
    "new_regime_code",
    "trigger_rule_id",
    "computed_at",
)


def _require_postgres_dsn() -> str:
    dsn = str(os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise ValueError("POSTGRES_DSN is required for gold regime job.")
    return dsn


def _coerce_cell(value: Any) -> Any:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.to_pydatetime()
        return value.tz_convert("UTC").to_pydatetime()
    return value


def _frame_rows(frame: pd.DataFrame, columns: tuple[str, ...]) -> list[tuple[Any, ...]]:
    if frame.empty:
        return []
    return [
        tuple(_coerce_cell(row.get(column)) for column in columns)
        for row in frame.loc[:, list(columns)].to_dict("records")
    ]


def _load_market_series(dsn: str) -> pd.DataFrame:
    sql = """
        SELECT symbol, date, close, return_1d, return_20d
        FROM gold.market_data
        WHERE symbol IN ('SPY', '^VIX', '^VIX3M')
        ORDER BY date ASC, symbol ASC
    """
    with connect(dsn) as conn:
        frame = pd.read_sql_query(sql, conn)
    if frame.empty:
        raise ValueError("gold.market_data does not contain SPY, ^VIX, and ^VIX3M history.")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
    frame = frame.dropna(subset=["date"]).reset_index(drop=True)
    return frame


def _build_inputs_daily(market_series: pd.DataFrame, *, computed_at: datetime) -> pd.DataFrame:
    spy = (
        market_series[market_series["symbol"] == "SPY"][["date", "close", "return_1d", "return_20d"]]
        .rename(columns={"date": "as_of_date", "close": "spy_close"})
        .copy()
    )
    vix = (
        market_series[market_series["symbol"] == "^VIX"][["date", "close"]]
        .rename(columns={"date": "as_of_date", "close": "vix_spot_close"})
        .copy()
    )
    vix3m = (
        market_series[market_series["symbol"] == "^VIX3M"][["date", "close"]]
        .rename(columns={"date": "as_of_date", "close": "vix3m_close"})
        .copy()
    )

    inputs = spy.merge(vix, on="as_of_date", how="outer").merge(vix3m, on="as_of_date", how="outer")
    inputs = inputs.sort_values("as_of_date").reset_index(drop=True)
    inputs["vix_slope"] = inputs["vix3m_close"] - inputs["vix_spot_close"]
    inputs["rvol_10d_ann"] = inputs["return_1d"].rolling(window=10, min_periods=10).std(ddof=1) * sqrt(252.0) * 100.0

    streak = 0
    streak_values: list[int] = []
    for raw_value in inputs["vix_spot_close"].tolist():
        value = float(raw_value) if raw_value is not None and not pd.isna(raw_value) else None
        if value is not None and value > 32.0:
            streak += 1
        else:
            streak = 0
        streak_values.append(streak)
    inputs["vix_gt_32_streak"] = streak_values
    inputs["trend_state"] = inputs["return_20d"].map(lambda value: compute_trend_state(value))
    inputs["curve_state"] = inputs["vix_slope"].map(lambda value: compute_curve_state(value))
    inputs["inputs_complete_flag"] = inputs[
        [
            "spy_close",
            "return_1d",
            "return_20d",
            "rvol_10d_ann",
            "vix_spot_close",
            "vix3m_close",
            "vix_slope",
        ]
    ].notna().all(axis=1)
    inputs["computed_at"] = pd.Timestamp(computed_at)
    return inputs[list(_INPUTS_COLUMNS)].copy()


def _replace_postgres_tables(
    dsn: str,
    *,
    inputs: pd.DataFrame,
    history: pd.DataFrame,
    latest: pd.DataFrame,
    transitions: pd.DataFrame,
    active_models: list[tuple[str, int]],
) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE gold.regime_inputs_daily")
            input_rows = _frame_rows(inputs, _INPUTS_COLUMNS)
            if input_rows:
                copy_rows(
                    cur,
                    table="gold.regime_inputs_daily",
                    columns=_INPUTS_COLUMNS,
                    rows=input_rows,
                )

            if active_models:
                cur.executemany(
                    "DELETE FROM gold.regime_history WHERE model_name = %s AND model_version = %s",
                    active_models,
                )
                cur.executemany(
                    "DELETE FROM gold.regime_latest WHERE model_name = %s AND model_version = %s",
                    active_models,
                )
                cur.executemany(
                    "DELETE FROM gold.regime_transitions WHERE model_name = %s AND model_version = %s",
                    active_models,
                )

            history_rows = _frame_rows(history, _HISTORY_COLUMNS)
            if history_rows:
                copy_rows(cur, table="gold.regime_history", columns=_HISTORY_COLUMNS, rows=history_rows)

            latest_rows = _frame_rows(latest, _HISTORY_COLUMNS)
            if latest_rows:
                copy_rows(cur, table="gold.regime_latest", columns=_HISTORY_COLUMNS, rows=latest_rows)

            transition_rows = _frame_rows(transitions, _TRANSITIONS_COLUMNS)
            if transition_rows:
                copy_rows(
                    cur,
                    table="gold.regime_transitions",
                    columns=_TRANSITIONS_COLUMNS,
                    rows=transition_rows,
                )


def _write_storage_outputs(
    *,
    gold_container: str,
    inputs: pd.DataFrame,
    history: pd.DataFrame,
    latest: pd.DataFrame,
    transitions: pd.DataFrame,
) -> None:
    client = mdc.get_storage_client(gold_container)
    if client is None:
        raise ValueError(f"Storage client unavailable for container '{gold_container}'.")
    client.write_parquet("regime/inputs.parquet", inputs)
    client.write_parquet("regime/history.parquet", history)
    client.write_parquet("regime/latest.parquet", latest)
    client.write_parquet("regime/transitions.parquet", transitions)

    history_dates = pd.to_datetime(history["as_of_date"], errors="coerce").dropna() if not history.empty else pd.Series(dtype="datetime64[ns]")
    date_range = None
    if not history_dates.empty:
        date_range = {
            "min": history_dates.min().isoformat(),
            "max": history_dates.max().isoformat(),
            "column": "as_of_date",
            "source": "artifact",
        }
    artifact_path = domain_artifacts.domain_artifact_path(layer="gold", domain="regime")
    payload = {
        "version": 1,
        "scope": "domain",
        "layer": "gold",
        "domain": "regime",
        "rootPath": "regime",
        "artifactPath": artifact_path,
        "updatedAt": computed_at_iso(),
        "computedAt": computed_at_iso(),
        "producerJobName": JOB_NAME,
        "symbolCount": 0,
        "columnCount": len(sorted(set(inputs.columns) | set(history.columns) | set(latest.columns) | set(transitions.columns))),
        "columns": sorted(set(inputs.columns) | set(history.columns) | set(latest.columns) | set(transitions.columns)),
        "dateRange": date_range,
        "totalRows": int(len(history)),
        "fileCount": 4,
        "warnings": [],
    }
    mdc.save_json_content(payload, artifact_path, client=client)


def computed_at_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    mdc.log_environment_diagnostics()
    dsn = _require_postgres_dsn()
    gold_container = str(os.environ.get("AZURE_CONTAINER_GOLD") or "").strip()
    if not gold_container:
        raise ValueError("AZURE_CONTAINER_GOLD is required for gold regime job.")

    computed_at = datetime.now(timezone.utc)
    repo = RegimeRepository(dsn)
    active_revisions = repo.list_active_regime_model_revisions()
    if not active_revisions:
        raise ValueError("No active regime model revisions found.")

    market_series = _load_market_series(dsn)
    inputs = _build_inputs_daily(market_series, computed_at=computed_at)

    history_frames: list[pd.DataFrame] = []
    latest_frames: list[pd.DataFrame] = []
    transition_frames: list[pd.DataFrame] = []
    active_models: list[tuple[str, int]] = []

    for revision in active_revisions:
        model_name = str(revision["name"])
        model_version = int(revision["version"])
        history, latest, transitions = build_regime_outputs(
            inputs[["as_of_date", "return_1d", "return_20d", "rvol_10d_ann", "vix_spot_close", "vix3m_close", "vix_slope", "vix_gt_32_streak", "inputs_complete_flag"]].copy(),
            model_name=model_name,
            model_version=model_version,
            config=revision.get("config") or {},
            computed_at=computed_at,
        )
        history_frames.append(history)
        latest_frames.append(latest)
        transition_frames.append(transitions)
        active_models.append((model_name, model_version))

    history = pd.concat(history_frames, ignore_index=True) if history_frames else pd.DataFrame(columns=_HISTORY_COLUMNS)
    latest = pd.concat(latest_frames, ignore_index=True) if latest_frames else pd.DataFrame(columns=_HISTORY_COLUMNS)
    transitions = (
        pd.concat(transition_frames, ignore_index=True)
        if transition_frames
        else pd.DataFrame(columns=_TRANSITIONS_COLUMNS)
    )

    _replace_postgres_tables(
        dsn,
        inputs=inputs,
        history=history,
        latest=latest,
        transitions=transitions,
        active_models=active_models,
    )
    _write_storage_outputs(
        gold_container=gold_container,
        inputs=inputs,
        history=history,
        latest=latest,
        transitions=transitions,
    )

    save_watermarks(
        WATERMARK_KEY,
        {
            "as_of_date": str(inputs["as_of_date"].max()) if not inputs.empty else None,
            "history_rows": int(len(history)),
            "active_models": [
                {"model_name": model_name, "model_version": model_version}
                for model_name, model_version in active_models
            ],
        },
    )
    save_last_success(
        WATERMARK_KEY,
        when=computed_at,
        metadata={
            "as_of_date": str(inputs["as_of_date"].max()) if not inputs.empty else None,
            "history_rows": int(len(history)),
            "latest_rows": int(len(latest)),
            "transition_rows": int(len(transitions)),
        },
    )
    mdc.write_line(
        "Gold regime complete: "
        f"inputs_rows={len(inputs)} history_rows={len(history)} latest_rows={len(latest)} "
        f"transition_rows={len(transitions)} active_models={len(active_models)}"
    )
    return 0


if __name__ == "__main__":
    ensure_api_awake_from_env(required=True)
    exit_code = main()
    if exit_code == 0:
        write_system_health_marker(layer="gold", domain="regime", job_name=JOB_NAME)
    raise SystemExit(exit_code)
