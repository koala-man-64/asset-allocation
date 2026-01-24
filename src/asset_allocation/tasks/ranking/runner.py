"""
Main Runner for the Ranking Framework.
Orchestrates data loading, strategy execution, and result saving.
"""
import os
from datetime import date, datetime
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from asset_allocation.core import config as cfg
from asset_allocation.core.blob_storage import BlobStorageClient
from asset_allocation.core.core import write_error, write_line
from asset_allocation.core import core as mdc
from asset_allocation.core.delta_core import load_delta
from asset_allocation.core.data_contract import CANONICAL_RANKINGS_PATH
from asset_allocation.tasks.ranking.core import save_rankings
from asset_allocation.tasks.ranking.signals import DEFAULT_TOP_N, materialize_signals_for_year_month
from asset_allocation.tasks.ranking.strategies import (
    AbstractStrategy,
    BrokenGrowthImprovingInternalsStrategy,
    MomentumStrategy,
    ValueStrategy,
)


DeltaSource = Dict[str, str]
WhitelistSource = Tuple[str, Optional[str]]

# Delta tables keyed by container + path env override.
DELTA_SOURCES: List[DeltaSource] = [
    {
        "name": "finance",
        "container": cfg.AZURE_CONTAINER_FINANCE,
        "path_env": "RANKING_FINANCE_DELTA_PATH",
        "per_symbol": True,
        "whitelist_prefix": "finance_data",
    },
    {
        "name": "price_targets",
        "container": cfg.AZURE_CONTAINER_TARGETS,
        "path_env": "RANKING_PRICE_DELTA_PATH",
        "per_symbol": True,
        "whitelist_prefix": "price_target_data",
    },
    {
        "name": "earnings",
        "container": cfg.AZURE_CONTAINER_EARNINGS,
        "path_env": "RANKING_EARNINGS_DELTA_PATH",
        "per_symbol": True,
        "whitelist_prefix": "earnings_data",
    },
]
SOURCE_LOOKUP = {source["name"]: source for source in DELTA_SOURCES}


def _build_blob_client(container_name: str, label: str) -> Optional[BlobStorageClient]:
    # Keep container creation out of the ranking path; it should exist already.
    if not container_name:
        raise ValueError(f"Missing required container configuration for {label}.")
    try:
        return BlobStorageClient(container_name=container_name, ensure_container_exists=False)
    except Exception as exc:
        write_line(f"Failed to initialize blob client for {container_name}: {exc}")
        return None


def _normalize_symbol_column(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize symbol casing and type for consistent joins downstream.
    if df is None or df.empty:
        return df
    if "symbol" not in df.columns and "Symbol" in df.columns:
        df = df.rename(columns={"Symbol": "symbol"})
    if "symbol" not in df.columns and "ticker" in df.columns:
        df = df.rename(columns={"ticker": "symbol"})
    if "date" not in df.columns and "obs_date" in df.columns:
        df = df.rename(columns={"obs_date": "date"})
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str)
    return df


def _collapse_latest_by_symbol(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    # Keep only the most recent row per symbol when a date column exists.
    if df is None or df.empty:
        return None
    df = _normalize_symbol_column(df).copy()
    if "symbol" not in df.columns:
        write_line("Warning: Delta table missing 'symbol'. Skipping.")
        return None

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.sort_values("date")
    df = df.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)
    return df


def _collect_whitelist_tickers(container_name: Optional[str], context_prefix: str) -> List[str]:
    # Each data pipeline maintains its own whitelist in its container.
    if not container_name:
        raise ValueError(f"Missing required container for {context_prefix} whitelist.")

    client = _build_blob_client(container_name, label=f"{context_prefix} container")
    if client is None:
        return []

    from asset_allocation.core.pipeline import ListManager

    # ListManager reads <context>_whitelist.csv in the target container.
    list_manager = ListManager(client, context_prefix)
    list_manager.load()
    return sorted(list_manager.whitelist)


def _get_whitelist_intersection(containers: List[WhitelistSource]) -> Set[str]:
    # Intersection of whitelists; empty if any source has no whitelist.
    intersection: Optional[Set[str]] = None
    for context_prefix, container_name in containers:
        current = set(_collect_whitelist_tickers(container_name, context_prefix))
        if intersection is None:
            intersection = current
        else:
            intersection &= current
        if not intersection:
            return set()
    return intersection or set()


def _get_market_feature_tickers(
    client: BlobStorageClient, whitelist: Optional[set[str]]
) -> List[str]:
    # Resolve available market features, then apply whitelist if present.
    write_line(
        "Listing market feature blobs from "
        f"{cfg.AZURE_CONTAINER_MARKET}/market/<ticker>..."
    )
    try:
        blobs = client.list_files(name_starts_with="market/")
    except Exception as exc:
        write_line(f"Warning: Failed to list market feature blobs: {exc}")
        return sorted(whitelist) if whitelist else []

    # Market feature deltas are stored as market/<ticker>/...
    available = set()
    for name in blobs:
        parts = name.split("/")
        # Expected: market/ticker/part-files...
        if len(parts) >= 2 and parts[0] == "market":
            available.add(parts[1])

    if whitelist:
        return sorted(available.intersection(whitelist))
    return sorted(available)


def _load_market_data(whitelist: Optional[Set[str]]) -> pd.DataFrame:
    from asset_allocation.core.pipeline import DataPaths

    by_date_path = os.environ.get("RANKING_MARKET_BY_DATE_DELTA_PATH", "").strip().lstrip("/")
    if by_date_path:
        by_date = load_delta(cfg.AZURE_CONTAINER_MARKET, by_date_path)
        if by_date is not None and not by_date.empty:
            by_date = _normalize_symbol_column(by_date)
            if whitelist:
                by_date = by_date[by_date["symbol"].isin(whitelist)]
            if by_date.empty:
                return pd.DataFrame()
            write_line(
                f"Loaded market features from by-date table {cfg.AZURE_CONTAINER_MARKET}/{by_date_path} "
                f"(rows={len(by_date)})"
            )
            return by_date.reset_index(drop=True)

    # Fallback: load per-ticker market features from the market container.
    client = _build_blob_client(cfg.AZURE_CONTAINER_MARKET, label="market container")

    tickers = _get_market_feature_tickers(client, whitelist)
    if not tickers:
        write_line("Warning: No market feature tickers found.")
        return pd.DataFrame()

    write_line(
        f"Loading market features for {len(tickers)} ticker(s) from "
        f"{cfg.AZURE_CONTAINER_MARKET}/gold/<ticker>..."
    )

    frames = []
    for ticker in tickers:
        # Each ticker's market features live under gold/<ticker>.
        path = DataPaths.get_gold_features_path(ticker)
        df = load_delta(cfg.AZURE_CONTAINER_MARKET, path)
        if df is None or df.empty:
            continue
        frames.append(_normalize_symbol_column(df))

    if not frames:
        write_line("Warning: Market feature delta tables not found or empty.")
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def _get_delta_path(source: Dict[str, str]) -> str:
    # Allow container-specific path overrides via env vars.
    path = os.environ.get(source["path_env"])
    if not path:
        raise ValueError(f"Missing required environment variable: {source['path_env']}")
    return path


def _load_delta_source(source: DeltaSource, whitelist: Optional[Set[str]]) -> Optional[pd.DataFrame]:
    # Load a single delta table and reduce to latest-per-symbol.
    container = source["container"]
    if not container:
        raise ValueError(f"Missing required container for {source['name']} source.")

    path = _get_delta_path(source)
    if source.get("per_symbol"):
        if not whitelist:
            write_line(f"No whitelist provided for '{source['name']}' source. Skipping.")
            return None

        write_line(
            f"Loading delta source '{source['name']}' for {len(whitelist)} ticker(s) from "
            f"{container}/{path}/<symbol>..."
        )
        frames = []
        for ticker in sorted(whitelist):
            ticker_path = f"{path.rstrip('/')}/{ticker}"
            df = load_delta(container, ticker_path)
            if df is None or df.empty:
                continue
            frames.append(_normalize_symbol_column(df))

        if not frames:
            write_line(
                f"Delta source '{source['name']}' is unavailable or empty ({container}/{path}/<symbol>)."
            )
            return None

        df = pd.concat(frames, ignore_index=True)
    else:
        write_line(f"Loading delta source '{source['name']}' from {container}/{path}")
        df = load_delta(container, path)
        if df is None or df.empty:
            write_line(f"Delta source '{source['name']}' is unavailable or empty ({container}/{path}).")
            return None

    collapsed = _collapse_latest_by_symbol(df)
    if collapsed is None or collapsed.empty:
        return None
    if whitelist:
        collapsed = collapsed[collapsed["symbol"].isin(whitelist)]
        if collapsed.empty:
            write_line(f"Delta source '{source['name']}' filtered out by whitelist.")
            return None
    return collapsed


def _merge_source(
    base: pd.DataFrame, extra: Optional[pd.DataFrame], source_name: str
) -> pd.DataFrame:
    # Keep base rows even when a source is missing.
    if extra is None or extra.empty:
        return base

    merged = base.merge(extra, on="symbol", how="left", suffixes=("", f"_{source_name}"))
    return merged


def _get_whitelist_sources_for_strategy(strategy: AbstractStrategy) -> List[WhitelistSource]:
    sources: List[WhitelistSource] = [("market_data", cfg.AZURE_CONTAINER_MARKET)]
    for source_name in strategy.sources_used:
        source = SOURCE_LOOKUP.get(source_name)
        if not source:
            raise ValueError(f"Missing required source definition for '{source_name}'.")
        container = source.get("container")
        whitelist_prefix = source.get("whitelist_prefix")
        if not container or not whitelist_prefix:
            raise ValueError(f"Missing required container/whitelist_prefix for source '{source_name}'.")
        sources.append((whitelist_prefix, container))
    return sources


def _format_value(value: Optional[str]) -> str:
    return value if value else "<unset>"


def _log_strategy_configuration(strategy: AbstractStrategy, ranking_container: str) -> None:
    source_specs = []
    whitelist_specs = [f"market_data@{_format_value(cfg.AZURE_CONTAINER_MARKET)}"]

    for source_name in strategy.sources_used:
        source = SOURCE_LOOKUP.get(source_name, {})
        container = source.get("container")
        base_path = os.environ.get(source.get("path_env", ""))
        suffix = "/<symbol>" if source.get("per_symbol") else ""
        source_specs.append(f"{source_name}={_format_value(container)}/{_format_value(base_path)}{suffix}")

        whitelist_prefix = source.get("whitelist_prefix")
        if whitelist_prefix:
            whitelist_specs.append(f"{whitelist_prefix}@{_format_value(container)}")

    containers_line = (
        f"market={_format_value(cfg.AZURE_CONTAINER_MARKET)}, "
        f"ranking_out={_format_value(ranking_container)}"
    )
    if strategy.sources_used:
        containers_line = f"{containers_line}, sources={', '.join(source_specs)}"

    write_line(f"Strategy={strategy.name} sources_used={strategy.sources_used}")
    write_line(f"Strategy={strategy.name} required_columns={strategy.required_columns}")
    write_line(f"Strategy={strategy.name} containers={containers_line}")
    write_line(f"Strategy={strategy.name} whitelist_intersection={whitelist_specs}")


def assemble_strategy_data(strategy: AbstractStrategy) -> pd.DataFrame:
    # Build whitelist from the containers the strategy depends on.
    whitelist_sources = _get_whitelist_sources_for_strategy(strategy)
    whitelist = _get_whitelist_intersection(whitelist_sources)
    if not whitelist:
        write_error(f"No whitelist entries available for {strategy.name}.")
        return pd.DataFrame()

    # Market features are the base for ranking inputs.
    base = _load_market_data(whitelist)
    if base.empty:
        return pd.DataFrame()

    for source_name in strategy.sources_used:
        source = SOURCE_LOOKUP.get(source_name)
        if not source:
            write_line(f"Warning: Unknown source '{source_name}' for {strategy.name}.")
            continue
        # Merge each auxiliary data set onto the market features.
        extra = _load_delta_source(source, whitelist)
        if extra is None:
            write_error(f"Required source '{source_name}' missing for {strategy.name}.")
            return pd.DataFrame()
        base = _merge_source(base, extra, source["name"])

    # Apply whitelist post-merge for safety.
    base = base[base["symbol"].isin(whitelist)]
    if base.empty:
        write_line(f"No data available after whitelist for {strategy.name}.")
        return pd.DataFrame()

    return base.reset_index(drop=True)


def _load_existing_ranking_dates(strategy_name: str, container: str) -> Set[date]:
    filtered = load_delta(
        container,
        CANONICAL_RANKINGS_PATH,
        columns=["date"],
        filters=[("strategy", "=", strategy_name)],
    )
    if filtered is None:
        rankings = load_delta(container, CANONICAL_RANKINGS_PATH, columns=["strategy", "date"])
        if rankings is None or rankings.empty:
            return set()
        if "strategy" not in rankings.columns or "date" not in rankings.columns:
            raise ValueError("Ranking table is missing required columns: strategy/date")
        filtered = rankings[rankings["strategy"] == strategy_name].copy()
        if filtered.empty:
            return set()

    if "date" not in filtered.columns:
        raise ValueError("Ranking table is missing required column: date")

    dates = pd.to_datetime(filtered["date"], errors="coerce")
    dates = dates.dropna()
    return set(dates.dt.date.tolist())

def _instantiate_strategies() -> List[AbstractStrategy]:
    # Pull thresholds from env to keep config consistent with job definitions.
    drawdown_threshold = float(os.environ["RANKING_BROKEN_DRAWDOWN_THRESHOLD"])
    margin_delta_threshold = float(os.environ["RANKING_MARGIN_DELTA_THRESHOLD"])

    return [
        MomentumStrategy(),
        ValueStrategy(),
        BrokenGrowthImprovingInternalsStrategy(
            drawdown_threshold=drawdown_threshold,
            margin_delta_threshold=margin_delta_threshold,
        ),
    ]


def main():
    # Log environment info early for troubleshooting.
    mdc.log_environment_diagnostics()
    write_line("Starting Ranking Runner...")

    strategies = _instantiate_strategies()
    write_line(f"Running {len(strategies)} ranking strategies.")

    if "AZURE_CONTAINER_RANKING" not in os.environ:
        raise ValueError("Missing required environment variable: AZURE_CONTAINER_RANKING")
    ranking_container = cfg.AZURE_CONTAINER_RANKING

    touched_year_months: Set[str] = set()

    for strategy in strategies:
        try:
            _log_strategy_configuration(strategy, ranking_container)
            data = assemble_strategy_data(strategy)
            if data.empty:
                write_error(f"No data available for {strategy.name}.")
                continue

            if "date" not in data.columns:
                write_error(f"{strategy.name} input missing required column: date")
                continue

            data["date"] = pd.to_datetime(data["date"], errors="coerce")
            data = data.dropna(subset=["date"])
            if data.empty:
                write_error(f"{strategy.name} has no valid dated rows.")
                continue

            required_missing = [col for col in strategy.required_columns if col not in data.columns]
            if required_missing:
                write_error(f"{strategy.name} missing required columns: {required_missing}")
                continue

            available_dates = set(data["date"].dt.date.tolist())
            existing_dates = _load_existing_ranking_dates(strategy.name, ranking_container)
            missing_dates = sorted(available_dates - existing_dates)

            if not missing_dates:
                write_line(f"{strategy.name}: no missing ranking dates.")
                continue

            write_line(
                f"{strategy.name}: computing rankings for {len(missing_dates)} missing date(s)."
            )

            total_dates = len(missing_dates)
            for idx, ranking_date in enumerate(missing_dates, start=1):
                percent_complete = (idx / total_dates) * 100
                progress = f"[{idx}/{total_dates} {percent_complete:.1f}%]"

                day_slice = data[data["date"].dt.date == ranking_date]
                if day_slice.empty:
                    write_error(
                        f"{strategy.name} {progress}: no input rows for {ranking_date}. Skipping date."
                    )
                    continue

                results = strategy.rank(day_slice, ranking_date)
                if results:
                    save_rankings(results, container=ranking_container)
                    touched_year_months.add(ranking_date.strftime("%Y-%m"))
                    write_line(
                        f"{strategy.name} {progress} saved {len(results)} rankings for {ranking_date}."
                    )
                else:
                    write_line(f"{strategy.name} {progress}: no results for {ranking_date}.")
        except Exception as exc:
            write_line(f"Error executing strategy {strategy.name}: {exc}")

    if touched_year_months:
        months = sorted(touched_year_months)
        write_line(f"Materializing ranking signals for {len(months)} month(s): {', '.join(months)}")
        for year_month in months:
            result = materialize_signals_for_year_month(
                container=ranking_container, year_month=year_month, top_n=DEFAULT_TOP_N
            )
            write_line(
                f"Signals materialized for {year_month}: rankings_rows={result.rankings_rows} "
                f"signals_rows={result.signals_rows} composite_rows={result.composite_rows}"
            )

    write_line("Ranking process completed.")


if __name__ == "__main__":
    main()
