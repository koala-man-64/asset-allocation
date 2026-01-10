"""
Main Runner for the Ranking Framework.
Orchestrates data loading, strategy execution, and result saving.
"""
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd

from scripts.common import config as cfg
from scripts.common.blob_storage import BlobStorageClient
from scripts.common.core import write_line
from scripts.common import core as mdc
from scripts.common.delta_core import load_delta
from scripts.ranking.core import save_rankings
from scripts.ranking.strategies import (
    AbstractStrategy,
    BrokenGrowthImprovingInternalsStrategy,
    MomentumStrategy,
    ValueStrategy,
)


# Ensure project root is in path for CLI/container execution.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


DeltaSource = Dict[str, str]

# Delta tables keyed by container + path env override.
DELTA_SOURCES: List[DeltaSource] = [
    {
        "name": "finance",
        "container": cfg.AZURE_CONTAINER_FINANCE,
        "path_env": "RANKING_FINANCE_DELTA_PATH",
        "default_path": "gold/finance_features",
    },
    {
        "name": "price_targets",
        "container": cfg.AZURE_CONTAINER_TARGETS,
        "path_env": "RANKING_PRICE_DELTA_PATH",
        "default_path": "gold/price_targets",
    },
]


def _build_blob_client(container_name: str, label: str = "container") -> Optional[BlobStorageClient]:
    # Keep container creation out of the ranking path; it should exist already.
    if not container_name:
        write_line(f"Error: {label} not configured for ranking job.")
        return None
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
        df = df.sort_values("date")
    df = df.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)
    return df


def _collect_whitelist_tickers(container_name: Optional[str], context_prefix: str) -> List[str]:
    # Each data pipeline maintains its own whitelist in its container.
    if not container_name:
        write_line(f"Warning: Missing container for {context_prefix} whitelist.")
        return []

    client = _build_blob_client(container_name, label=f"{context_prefix} container")
    if client is None:
        return []

    from scripts.common.pipeline import ListManager

    # ListManager reads <context>_whitelist.csv in the target container.
    list_manager = ListManager(client, context_prefix)
    list_manager.load()
    return sorted(list_manager.whitelist)


def _get_whitelist_tickers(containers: List[tuple[str, Optional[str]]]) -> List[str]:
    # Union all whitelists for sources used by this ranking run.
    tickers = set()
    for context_prefix, container_name in containers:
        tickers.update(_collect_whitelist_tickers(container_name, context_prefix))

    return sorted(tickers)


def _get_market_feature_tickers(
    client: BlobStorageClient, whitelist: Optional[set[str]]
) -> List[str]:
    # Resolve available market features, then apply whitelist if present.
    try:
        blobs = client.list_files(name_starts_with="gold/")
    except Exception as exc:
        write_line(f"Warning: Failed to list market feature blobs: {exc}")
        return sorted(whitelist) if whitelist else []

    # Market feature deltas are stored as gold/<ticker>/...
    available = set()
    for name in blobs:
        parts = name.split("/")
        if len(parts) >= 2 and parts[0] == "gold":
            available.add(parts[1])

    if whitelist:
        return sorted(available.intersection(whitelist))
    return sorted(available)


def _load_market_data(whitelist: Optional[set[str]]) -> pd.DataFrame:
    # Load per-ticker market features from the market container.
    client = _build_blob_client(cfg.AZURE_CONTAINER_MARKET, label="market container")
    if client is None:
        return pd.DataFrame()

    tickers = _get_market_feature_tickers(client, whitelist)
    if not tickers:
        write_line("Warning: No market feature tickers found.")
        return pd.DataFrame()

    from scripts.common.pipeline import DataPaths

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
    return os.environ.get(source["path_env"], source["default_path"])


def _load_delta_source(source: DeltaSource, whitelist: Optional[set[str]]) -> Optional[pd.DataFrame]:
    # Load a single delta table and reduce to latest-per-symbol.
    container = source["container"]
    if not container:
        write_line(f"Skipping {source['name']} source; container not configured.")
        return None

    path = _get_delta_path(source)
    df = load_delta(container, path)
    if df is None or df.empty:
        write_line(f"Delta source '{source['name']}' is unavailable or empty ({container}/{path}).")
        return None

    collapsed = _collapse_latest_by_symbol(df)
    if collapsed is None or collapsed.empty:
        return None

    if whitelist:
        # Apply whitelist consistently across all sources.
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


def assemble_ranking_data() -> pd.DataFrame:
    # Build whitelist from only the containers used in this run.
    whitelist_sources = [("market_data", cfg.AZURE_CONTAINER_MARKET)]
    for source in DELTA_SOURCES:
        whitelist_sources.append((f"{source['name']}_data", source["container"]))

    whitelist = set(_get_whitelist_tickers(whitelist_sources))
    if not whitelist:
        whitelist = None

    # Market features are the base for ranking inputs.
    base = _load_market_data(whitelist)
    if base.empty:
        return pd.DataFrame()

    for source in DELTA_SOURCES:
        # Merge each auxiliary data set onto the market features.
        extra = _load_delta_source(source, whitelist)
        base = _merge_source(base, extra, source["name"])

    return base.reset_index(drop=True)


def _instantiate_strategies() -> List[AbstractStrategy]:
    # Pull thresholds from env to keep config consistent with job definitions.
    drawdown_threshold = float(os.environ.get("RANKING_BROKEN_DRAWDOWN_THRESHOLD", "-0.3"))
    margin_delta_threshold = float(os.environ.get("RANKING_MARGIN_DELTA_THRESHOLD", "0.0"))

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

    # Load and assemble the full ranking dataset.
    data = assemble_ranking_data()
    if data.empty:
        write_line("No data available to rank.")
        return

    write_line(f"Ranking input data contains {len(data)} rows and {len(data.columns)} columns.")
    strategies = _instantiate_strategies()
    write_line(f"Running {len(strategies)} ranking strategies.")

    # Use UTC to keep ranking dates consistent across environments.
    today = datetime.now(timezone.utc).date()
    for strategy in strategies:
        try:
            # Each strategy handles its own required column checks.
            results = strategy.rank(data, today)
            if results:
                # Persist rankings per strategy.
                save_rankings(results)
                write_line(f"{strategy.name} produced {len(results)} rankings.")
            else:
                write_line(f"No results generated for strategy: {strategy.name}")
        except Exception as exc:
            write_line(f"Error executing strategy {strategy.name}: {exc}")

    write_line("Ranking process completed.")


if __name__ == "__main__":
    main()
