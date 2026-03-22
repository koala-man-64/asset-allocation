from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Optional, Sequence

import pandas as pd

from core.postgres import PostgresError, connect, copy_rows, get_dsn
from tasks.common.finance_contracts import VALUATION_FINANCE_COLUMNS


_MARKET_COLUMNS: tuple[str, ...] = (
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "return_1d",
    "return_5d",
    "return_20d",
    "return_60d",
    "vol_20d",
    "vol_60d",
    "rolling_max_252d",
    "drawdown_1y",
    "true_range",
    "atr_14d",
    "gap_atr",
    "sma_20d",
    "sma_50d",
    "sma_200d",
    "sma_20_gt_sma_50",
    "sma_50_gt_sma_200",
    "trend_50_200",
    "above_sma_50",
    "sma_20_crosses_above_sma_50",
    "sma_20_crosses_below_sma_50",
    "sma_50_crosses_above_sma_200",
    "sma_50_crosses_below_sma_200",
    "bb_width_20d",
    "range_close",
    "range_20",
    "compression_score",
    "volume_z_20d",
    "volume_pct_rank_252d",
    "range",
    "body",
    "is_bull",
    "is_bear",
    "upper_shadow",
    "lower_shadow",
    "body_to_range",
    "upper_to_range",
    "lower_to_range",
    "pat_doji",
    "pat_spinning_top",
    "pat_bullish_marubozu",
    "pat_bearish_marubozu",
    "pat_star_gap_up",
    "pat_star_gap_down",
    "pat_star",
    "pat_hammer",
    "pat_hanging_man",
    "pat_inverted_hammer",
    "pat_shooting_star",
    "pat_dragonfly_doji",
    "pat_gravestone_doji",
    "pat_bullish_spinning_top",
    "pat_bearish_spinning_top",
    "pat_bullish_engulfing",
    "pat_bearish_engulfing",
    "pat_bullish_harami",
    "pat_bearish_harami",
    "pat_piercing_line",
    "pat_dark_cloud_line",
    "pat_tweezer_bottom",
    "pat_tweezer_top",
    "pat_bullish_kicker",
    "pat_bearish_kicker",
    "pat_morning_star",
    "pat_morning_doji_star",
    "pat_evening_star",
    "pat_evening_doji_star",
    "pat_bullish_abandoned_baby",
    "pat_bearish_abandoned_baby",
    "pat_three_white_soldiers",
    "pat_three_black_crows",
    "pat_bullish_three_line_strike",
    "pat_bearish_three_line_strike",
    "pat_three_inside_up",
    "pat_three_outside_up",
    "pat_three_inside_down",
    "pat_three_outside_down",
    "ha_open",
    "ha_high",
    "ha_low",
    "ha_close",
    "ichimoku_tenkan_sen_9",
    "ichimoku_kijun_sen_26",
    "ichimoku_senkou_span_a",
    "ichimoku_senkou_span_b",
    "ichimoku_senkou_span_a_26",
    "ichimoku_senkou_span_b_26",
    "ichimoku_chikou_span_26",
    "donchian_high_20d",
    "donchian_low_20d",
    "dist_donchian_high_20d_atr",
    "dist_donchian_low_20d_atr",
    "above_donchian_high_20d",
    "below_donchian_low_20d",
    "crosses_above_donchian_high_20d",
    "crosses_below_donchian_low_20d",
    "donchian_high_55d",
    "donchian_low_55d",
    "dist_donchian_high_55d_atr",
    "dist_donchian_low_55d_atr",
    "above_donchian_high_55d",
    "below_donchian_low_55d",
    "crosses_above_donchian_high_55d",
    "crosses_below_donchian_low_55d",
    "sr_support_1_mid",
    "sr_support_1_low",
    "sr_support_1_high",
    "sr_support_1_touches",
    "sr_support_1_strength",
    "sr_support_1_dist_atr",
    "sr_resistance_1_mid",
    "sr_resistance_1_low",
    "sr_resistance_1_high",
    "sr_resistance_1_touches",
    "sr_resistance_1_strength",
    "sr_resistance_1_dist_atr",
    "sr_in_support_1_zone",
    "sr_in_resistance_1_zone",
    "sr_breaks_above_resistance_1",
    "sr_breaks_below_support_1",
    "sr_zone_position",
    "fib_swing_direction",
    "fib_anchor_low",
    "fib_anchor_high",
    "fib_level_236",
    "fib_level_382",
    "fib_level_500",
    "fib_level_618",
    "fib_level_786",
    "fib_nearest_level",
    "fib_nearest_dist_atr",
    "fib_in_value_zone",
)
_MARKET_INTEGER_COLUMNS = frozenset(
    {
        "sma_20_gt_sma_50",
        "sma_50_gt_sma_200",
        "above_sma_50",
        "sma_20_crosses_above_sma_50",
        "sma_20_crosses_below_sma_50",
        "sma_50_crosses_above_sma_200",
        "sma_50_crosses_below_sma_200",
        "is_bull",
        "is_bear",
        "pat_doji",
        "pat_spinning_top",
        "pat_bullish_marubozu",
        "pat_bearish_marubozu",
        "pat_star_gap_up",
        "pat_star_gap_down",
        "pat_star",
        "pat_hammer",
        "pat_hanging_man",
        "pat_inverted_hammer",
        "pat_shooting_star",
        "pat_dragonfly_doji",
        "pat_gravestone_doji",
        "pat_bullish_spinning_top",
        "pat_bearish_spinning_top",
        "pat_bullish_engulfing",
        "pat_bearish_engulfing",
        "pat_bullish_harami",
        "pat_bearish_harami",
        "pat_piercing_line",
        "pat_dark_cloud_line",
        "pat_tweezer_bottom",
        "pat_tweezer_top",
        "pat_bullish_kicker",
        "pat_bearish_kicker",
        "pat_morning_star",
        "pat_morning_doji_star",
        "pat_evening_star",
        "pat_evening_doji_star",
        "pat_bullish_abandoned_baby",
        "pat_bearish_abandoned_baby",
        "pat_three_white_soldiers",
        "pat_three_black_crows",
        "pat_bullish_three_line_strike",
        "pat_bearish_three_line_strike",
        "pat_three_inside_up",
        "pat_three_outside_up",
        "pat_three_inside_down",
        "pat_three_outside_down",
        "above_donchian_high_20d",
        "below_donchian_low_20d",
        "crosses_above_donchian_high_20d",
        "crosses_below_donchian_low_20d",
        "above_donchian_high_55d",
        "below_donchian_low_55d",
        "crosses_above_donchian_high_55d",
        "crosses_below_donchian_low_55d",
        "sr_support_1_touches",
        "sr_resistance_1_touches",
        "sr_in_support_1_zone",
        "sr_in_resistance_1_zone",
        "sr_breaks_above_resistance_1",
        "sr_breaks_below_support_1",
        "fib_swing_direction",
        "fib_in_value_zone",
    }
)
_MARKET_BIGINT_COLUMNS = frozenset({"volume"})

_FINANCE_COLUMNS: tuple[str, ...] = (
    "date",
    "symbol",
    *VALUATION_FINANCE_COLUMNS,
    "piotroski_roa_pos",
    "piotroski_cfo_pos",
    "piotroski_delta_roa_pos",
    "piotroski_accruals_pos",
    "piotroski_leverage_decrease",
    "piotroski_liquidity_increase",
    "piotroski_no_new_shares",
    "piotroski_gross_margin_increase",
    "piotroski_asset_turnover_increase",
    "piotroski_f_score",
)
_FINANCE_INTEGER_COLUMNS = frozenset(
    {
        "piotroski_roa_pos",
        "piotroski_cfo_pos",
        "piotroski_delta_roa_pos",
        "piotroski_accruals_pos",
        "piotroski_leverage_decrease",
        "piotroski_liquidity_increase",
        "piotroski_no_new_shares",
        "piotroski_gross_margin_increase",
        "piotroski_asset_turnover_increase",
        "piotroski_f_score",
    }
)

_EARNINGS_COLUMNS: tuple[str, ...] = (
    "date",
    "symbol",
    "reported_eps",
    "eps_estimate",
    "surprise",
    "surprise_pct",
    "surprise_mean_4q",
    "surprise_std_8q",
    "beat_rate_8q",
    "is_earnings_day",
    "last_earnings_date",
    "days_since_earnings",
    "next_earnings_date",
    "days_until_next_earnings",
    "next_earnings_estimate",
    "next_earnings_time_of_day",
    "next_earnings_fiscal_date_ending",
    "has_upcoming_earnings",
    "is_scheduled_earnings_day",
)
_EARNINGS_INTEGER_COLUMNS = frozenset(
    {
        "is_earnings_day",
        "days_since_earnings",
        "days_until_next_earnings",
        "has_upcoming_earnings",
        "is_scheduled_earnings_day",
    }
)
_EARNINGS_TEXT_COLUMNS = frozenset({"next_earnings_time_of_day"})

_PRICE_TARGET_COLUMNS: tuple[str, ...] = (
    "obs_date",
    "symbol",
    "tp_mean_est",
    "tp_std_dev_est",
    "tp_high_est",
    "tp_low_est",
    "tp_cnt_est",
    "tp_cnt_est_rev_up",
    "tp_cnt_est_rev_down",
    "disp_abs",
    "disp_norm",
    "disp_std_norm",
    "rev_net",
    "rev_ratio",
    "rev_intensity",
    "disp_norm_change_30d",
    "tp_mean_change_30d",
    "disp_z",
    "tp_mean_slope_90d",
)
_PRICE_TARGET_INTEGER_COLUMNS = frozenset(
    {
        "tp_cnt_est",
        "tp_cnt_est_rev_up",
        "tp_cnt_est_rev_down",
        "rev_net",
    }
)


@dataclass(frozen=True)
class GoldSyncConfig:
    domain: str
    table: str
    date_column: str
    date_columns: tuple[str, ...]
    columns: tuple[str, ...]
    integer_columns: frozenset[str]
    bigint_columns: frozenset[str] = frozenset()
    text_columns: frozenset[str] = frozenset()


@dataclass(frozen=True)
class GoldSyncResult:
    status: str
    domain: str
    bucket: str
    row_count: int
    symbol_count: int
    scope_symbol_count: int
    source_commit: Optional[float]
    min_key: Optional[date]
    max_key: Optional[date]
    error: Optional[str] = None


def sync_state_cache_entry(result: GoldSyncResult) -> dict[str, Any]:
    return {
        "source_commit": result.source_commit,
        "status": "success" if result.status == "ok" else result.status,
        "row_count": result.row_count,
        "symbol_count": result.symbol_count,
        "min_observation_date": result.min_key,
        "max_observation_date": result.max_key,
        "error": result.error,
    }


_DOMAIN_CONFIGS: dict[str, GoldSyncConfig] = {
    "market": GoldSyncConfig(
        domain="market",
        table="gold.market_data",
        date_column="date",
        date_columns=("date",),
        columns=_MARKET_COLUMNS,
        integer_columns=_MARKET_INTEGER_COLUMNS,
        bigint_columns=_MARKET_BIGINT_COLUMNS,
    ),
    "finance": GoldSyncConfig(
        domain="finance",
        table="gold.finance_data",
        date_column="date",
        date_columns=("date",),
        columns=_FINANCE_COLUMNS,
        integer_columns=_FINANCE_INTEGER_COLUMNS,
    ),
    "earnings": GoldSyncConfig(
        domain="earnings",
        table="gold.earnings_data",
        date_column="date",
        date_columns=("date", "last_earnings_date", "next_earnings_date", "next_earnings_fiscal_date_ending"),
        columns=_EARNINGS_COLUMNS,
        integer_columns=_EARNINGS_INTEGER_COLUMNS,
        text_columns=_EARNINGS_TEXT_COLUMNS,
    ),
    "price-target": GoldSyncConfig(
        domain="price-target",
        table="gold.price_target_data",
        date_column="obs_date",
        date_columns=("obs_date",),
        columns=_PRICE_TARGET_COLUMNS,
        integer_columns=_PRICE_TARGET_INTEGER_COLUMNS,
    ),
}


def resolve_postgres_dsn() -> Optional[str]:
    return get_dsn("POSTGRES_DSN")


def get_sync_config(domain: str) -> GoldSyncConfig:
    normalized = str(domain or "").strip().lower().replace("_", "-")
    if normalized == "targets":
        normalized = "price-target"
    config = _DOMAIN_CONFIGS.get(normalized)
    if config is None:
        raise ValueError(f"Unsupported Postgres gold sync domain={domain!r}")
    return config


def load_domain_sync_state(dsn: Optional[str], *, domain: str) -> dict[str, dict[str, Any]]:
    if not dsn:
        return {}

    config = get_sync_config(domain)
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT bucket, source_commit, status, row_count, symbol_count, synced_at, error
                FROM core.gold_sync_state
                WHERE domain = %s
                """,
                (config.domain,),
            )
            rows = cur.fetchall()

    out: dict[str, dict[str, Any]] = {}
    for bucket, source_commit, status, row_count, symbol_count, synced_at, error in rows:
        out[str(bucket or "").strip().upper()] = {
            "source_commit": source_commit,
            "status": status,
            "row_count": row_count,
            "symbol_count": symbol_count,
            "synced_at": synced_at,
            "error": error,
        }
    return out


def bucket_sync_is_current(
    sync_state: Mapping[str, Mapping[str, Any]],
    *,
    bucket: str,
    source_commit: Optional[float],
) -> bool:
    if source_commit is None:
        return False

    state = sync_state.get(str(bucket or "").strip().upper())
    if not state:
        return False
    if str(state.get("status") or "").strip().lower() != "success":
        return False

    prior_commit = state.get("source_commit")
    if prior_commit is None:
        return False

    try:
        return float(prior_commit) >= float(source_commit)
    except (TypeError, ValueError):
        return False


def sync_gold_bucket(
    *,
    domain: str,
    bucket: str,
    frame: pd.DataFrame,
    scope_symbols: Sequence[str],
    source_commit: Optional[float],
    dsn: Optional[str] = None,
) -> GoldSyncResult:
    resolved_dsn = dsn or resolve_postgres_dsn()
    config = get_sync_config(domain)
    prepared = _prepare_frame(frame, config=config)
    current_symbols = _normalize_symbols(prepared.get("symbol", pd.Series(dtype="object")).tolist())
    normalized_scope_symbols = sorted(set(_normalize_symbols(scope_symbols)).union(current_symbols))
    min_key = prepared[config.date_column].min() if not prepared.empty else None
    max_key = prepared[config.date_column].max() if not prepared.empty else None

    result = GoldSyncResult(
        status="ok",
        domain=config.domain,
        bucket=str(bucket or "").strip().upper(),
        row_count=int(len(prepared)),
        symbol_count=len(current_symbols),
        scope_symbol_count=len(normalized_scope_symbols),
        source_commit=source_commit,
        min_key=min_key,
        max_key=max_key,
    )

    if not resolved_dsn:
        return GoldSyncResult(**{**result.__dict__, "status": "skipped_no_dsn"})

    try:
        with connect(resolved_dsn) as conn:
            with conn.cursor() as cur:
                if normalized_scope_symbols:
                    cur.execute(
                        f'DELETE FROM {config.table} WHERE "symbol" = ANY(%s)',
                        (normalized_scope_symbols,),
                    )
                if not prepared.empty:
                    copy_rows(
                        cur,
                        table=config.table,
                        columns=_quote_columns(config.columns),
                        rows=_copy_rows(prepared),
                    )
                _upsert_sync_state(
                    cur,
                    domain=config.domain,
                    bucket=result.bucket,
                    source_commit=source_commit,
                    status="success",
                    row_count=result.row_count,
                    symbol_count=result.symbol_count,
                    min_key=min_key,
                    max_key=max_key,
                    error=None,
                )
        return result
    except Exception as exc:
        _record_failed_sync_state(
            resolved_dsn,
            domain=config.domain,
            bucket=result.bucket,
            source_commit=source_commit,
            row_count=result.row_count,
            symbol_count=result.symbol_count,
            min_key=min_key,
            max_key=max_key,
            error=str(exc),
        )
        raise PostgresError(
            f"Gold Postgres sync failed for domain={config.domain} bucket={result.bucket}: {exc}"
        ) from exc


def _prepare_frame(frame: pd.DataFrame, *, config: GoldSyncConfig) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(config.columns))

    work = frame.copy()
    missing_columns = [column for column in config.columns if column not in work.columns]
    if missing_columns:
        missing_frame = pd.DataFrame(index=work.index, columns=missing_columns)
        work = pd.concat([work, missing_frame], axis=1)
    work = work[list(config.columns)].copy()

    for column in config.date_columns:
        if column not in work.columns:
            continue
        work[column] = pd.to_datetime(work[column], errors="coerce").dt.date

    symbols = work["symbol"].astype("string").str.strip().str.upper()
    work["symbol"] = symbols
    work = work[symbols.notna() & (symbols != "")].copy()

    for column in config.columns:
        if column == "symbol" or column in config.date_columns:
            continue
        if column in config.text_columns:
            values = work[column].astype("string").str.strip()
            work[column] = values.where(values.notna() & (values != ""), pd.NA)
            continue
        if column in config.integer_columns or column in config.bigint_columns:
            work[column] = pd.to_numeric(work[column], errors="coerce").round().astype("Int64")
        else:
            work[column] = pd.to_numeric(work[column], errors="coerce")

    work = work.dropna(subset=[config.date_column, "symbol"]).copy()
    work = work.drop_duplicates(subset=["symbol", config.date_column], keep="last").reset_index(drop=True)
    return work


def _normalize_symbols(values: Sequence[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = str(value or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _copy_rows(df: pd.DataFrame):
    prepared = df.astype(object).where(pd.notnull(df), None)
    return prepared.itertuples(index=False, name=None)


def _quote_columns(columns: Sequence[str]) -> list[str]:
    return [_quote_identifier(column) for column in columns]


def _quote_identifier(identifier: str) -> str:
    escaped = str(identifier or "").replace('"', '""')
    return f'"{escaped}"'


def _upsert_sync_state(
    cur: Any,
    *,
    domain: str,
    bucket: str,
    source_commit: Optional[float],
    status: str,
    row_count: int,
    symbol_count: int,
    min_key: Optional[date],
    max_key: Optional[date],
    error: Optional[str],
) -> None:
    cur.execute(
        """
        INSERT INTO core.gold_sync_state (
            domain,
            bucket,
            source_commit,
            status,
            row_count,
            symbol_count,
            min_observation_date,
            max_observation_date,
            synced_at,
            error
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (domain, bucket) DO UPDATE
        SET source_commit = EXCLUDED.source_commit,
            status = EXCLUDED.status,
            row_count = EXCLUDED.row_count,
            symbol_count = EXCLUDED.symbol_count,
            min_observation_date = EXCLUDED.min_observation_date,
            max_observation_date = EXCLUDED.max_observation_date,
            synced_at = NOW(),
            error = EXCLUDED.error
        """,
        (
            domain,
            bucket,
            source_commit,
            status,
            row_count,
            symbol_count,
            min_key,
            max_key,
            error,
        ),
    )


def _record_failed_sync_state(
    dsn: str,
    *,
    domain: str,
    bucket: str,
    source_commit: Optional[float],
    row_count: int,
    symbol_count: int,
    min_key: Optional[date],
    max_key: Optional[date],
    error: str,
) -> None:
    try:
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                _upsert_sync_state(
                    cur,
                    domain=domain,
                    bucket=bucket,
                    source_commit=source_commit,
                    status="failed",
                    row_count=row_count,
                    symbol_count=symbol_count,
                    min_key=min_key,
                    max_key=max_key,
                    error=error,
                )
    except Exception:
        return
