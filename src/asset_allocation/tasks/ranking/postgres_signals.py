from __future__ import annotations

import os
from typing import Optional

import pandas as pd

from asset_allocation.core.core import write_line
from asset_allocation.core.postgres import PostgresError, connect, copy_rows, require_columns


RANKING_SIGNAL_COLUMNS = [
    "date",
    "year_month",
    "symbol",
    "strategy",
    "rank",
    "rank_percentile",
    "n_symbols",
    "score",
]

COMPOSITE_SIGNAL_COLUMNS = [
    "date",
    "year_month",
    "symbol",
    "composite_percentile",
    "composite_rank",
    "strategies_present",
    "strategies_hit",
]


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _prepare_ranking_signals(signals: pd.DataFrame, year_month: str) -> pd.DataFrame:
    if signals is None or signals.empty:
        return pd.DataFrame(columns=RANKING_SIGNAL_COLUMNS)

    required = ["date", "symbol", "strategy", "rank", "rank_percentile", "n_symbols"]
    require_columns(signals, required, "Ranking signals")

    out = signals.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out = out.dropna(subset=["date"])

    out["year_month"] = year_month
    out["symbol"] = out["symbol"].astype(str)
    out["strategy"] = out["strategy"].astype(str)
    out["rank"] = pd.to_numeric(out["rank"], errors="coerce")
    out["rank_percentile"] = pd.to_numeric(out["rank_percentile"], errors="coerce").astype(float)
    out["n_symbols"] = pd.to_numeric(out["n_symbols"], errors="coerce")

    if "score" not in out.columns:
        out["score"] = None
    else:
        out["score"] = pd.to_numeric(out["score"], errors="coerce").where(lambda s: s.notna(), None)

    out = out.dropna(subset=["rank", "rank_percentile", "n_symbols", "symbol", "strategy"])
    out["rank"] = out["rank"].astype(int)
    out["n_symbols"] = out["n_symbols"].astype(int)
    return out[RANKING_SIGNAL_COLUMNS].reset_index(drop=True)


def _prepare_composite_signals(composite: pd.DataFrame, year_month: str) -> pd.DataFrame:
    if composite is None or composite.empty:
        return pd.DataFrame(columns=COMPOSITE_SIGNAL_COLUMNS)

    required = [
        "date",
        "symbol",
        "composite_percentile",
        "composite_rank",
        "strategies_present",
        "strategies_hit",
    ]
    require_columns(composite, required, "Composite signals")

    out = composite.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out = out.dropna(subset=["date"])

    out["year_month"] = year_month
    out["symbol"] = out["symbol"].astype(str)
    out["composite_percentile"] = pd.to_numeric(out["composite_percentile"], errors="coerce").astype(float)
    out["composite_rank"] = pd.to_numeric(out["composite_rank"], errors="coerce")
    out["strategies_present"] = pd.to_numeric(out["strategies_present"], errors="coerce")
    out["strategies_hit"] = pd.to_numeric(out["strategies_hit"], errors="coerce")

    out = out.dropna(
        subset=[
            "symbol",
            "composite_percentile",
            "composite_rank",
            "strategies_present",
            "strategies_hit",
        ]
    )
    out["composite_rank"] = out["composite_rank"].astype(int)
    out["strategies_present"] = out["strategies_present"].astype(int)
    out["strategies_hit"] = out["strategies_hit"].astype(int)
    return out[COMPOSITE_SIGNAL_COLUMNS].reset_index(drop=True)


def write_signals_for_year_month(
    *,
    dsn: str,
    year_month: str,
    signals: pd.DataFrame,
    composite: pd.DataFrame,
) -> None:
    verify_counts = _env_bool("POSTGRES_SIGNALS_VERIFY_COUNTS", default=False)

    signals_out = _prepare_ranking_signals(signals, year_month)
    composite_out = _prepare_composite_signals(composite, year_month)

    if not dsn:
        raise ValueError("dsn is required")

    write_line(
        f"Replicating signals to Postgres for {year_month}: "
        f"signals_rows={len(signals_out)} composite_rows={len(composite_out)}"
    )

    try:
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM ranking.ranking_signal WHERE year_month = %s", (year_month,))
                cur.execute("DELETE FROM ranking.composite_signal_daily WHERE year_month = %s", (year_month,))

                copy_rows(
                    cur,
                    table="ranking.ranking_signal",
                    columns=RANKING_SIGNAL_COLUMNS,
                    rows=signals_out.itertuples(index=False, name=None),
                )
                copy_rows(
                    cur,
                    table="ranking.composite_signal_daily",
                    columns=COMPOSITE_SIGNAL_COLUMNS,
                    rows=composite_out.itertuples(index=False, name=None),
                )

                if verify_counts:
                    cur.execute(
                        "SELECT COUNT(*) FROM ranking.ranking_signal WHERE year_month = %s",
                        (year_month,),
                    )
                    stored_signals = int(cur.fetchone()[0])
                    cur.execute(
                        "SELECT COUNT(*) FROM ranking.composite_signal_daily WHERE year_month = %s",
                        (year_month,),
                    )
                    stored_composite = int(cur.fetchone()[0])

                    if stored_signals != len(signals_out) or stored_composite != len(composite_out):
                        raise PostgresError(
                            f"Postgres row-count mismatch for {year_month}: "
                            f"signals expected={len(signals_out)} actual={stored_signals}; "
                            f"composite expected={len(composite_out)} actual={stored_composite}"
                        )

                cur.execute(
                    """
                    INSERT INTO ranking.signal_sync_state(year_month, synced_at, signals_rows, composite_rows, status, error)
                    VALUES (%s, now(), %s, %s, %s, %s)
                    ON CONFLICT (year_month) DO UPDATE
                    SET synced_at = EXCLUDED.synced_at,
                        signals_rows = EXCLUDED.signals_rows,
                        composite_rows = EXCLUDED.composite_rows,
                        status = EXCLUDED.status,
                        error = EXCLUDED.error
                    """,
                    (year_month, len(signals_out), len(composite_out), "success", None),
                )
    except Exception as exc:
        write_line(f"Postgres replication failed for {year_month}: {exc}")
        raise
