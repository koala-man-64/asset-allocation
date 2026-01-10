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
from scripts.common.core import load_parquet, write_line
from scripts.common.delta_core import load_delta
from scripts.ranking.core import save_rankings
from scripts.ranking.strategies import (
    AbstractStrategy,
    BrokenGrowthImprovingInternalsStrategy,
    MomentumStrategy,
    ValueStrategy,
)


# Ensure project root is in path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


DeltaSource = Dict[str, str]

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


def _build_blob_client(container_name: str) -> Optional[BlobStorageClient]:
    if not container_name:
        write_line("Error: Market container not configured for ranking job.")
        return None
    try:
        return BlobStorageClient(container_name=container_name, ensure_container_exists=False)
    except Exception as exc:
        write_line(f"Failed to initialize blob client for {container_name}: {exc}")
        return None


def _normalize_symbol_column(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "symbol" not in df.columns and "Symbol" in df.columns:
        df = df.rename(columns={"Symbol": "symbol"})
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str)
    return df


def _collapse_latest_by_symbol(df: pd.DataFrame) -> Optional[pd.DataFrame]:
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


def _load_market_data() -> pd.DataFrame:
    client = _build_blob_client(cfg.AZURE_CONTAINER_MARKET)
    if client is None:
        return pd.DataFrame()

    market_df = load_parquet("get_historical_data_output.parquet", client=client)
    if market_df is None or market_df.empty:
        write_line("Warning: Market parquet not found or empty.")
        return pd.DataFrame()

    return _normalize_symbol_column(market_df)


def _get_delta_path(source: Dict[str, str]) -> str:
    return os.environ.get(source["path_env"], source["default_path"])


def _load_delta_source(source: DeltaSource) -> Optional[pd.DataFrame]:
    container = source["container"]
    if not container:
        write_line(f"Skipping {source['name']} source; container not configured.")
        return None

    path = _get_delta_path(source)
    df = load_delta(container, path)
    if df is None or df.empty:
        write_line(f"Delta source '{source['name']}' is unavailable or empty ({container}/{path}).")
        return None

    return _collapse_latest_by_symbol(df)


def _merge_source(
    base: pd.DataFrame, extra: Optional[pd.DataFrame], source_name: str
) -> pd.DataFrame:
    if extra is None or extra.empty:
        return base

    merged = base.merge(extra, on="symbol", how="left", suffixes=("", f"_{source_name}"))
    return merged


def assemble_ranking_data() -> pd.DataFrame:
    base = _load_market_data()
    if base.empty:
        return pd.DataFrame()

    for source in DELTA_SOURCES:
        extra = _load_delta_source(source)
        base = _merge_source(base, extra, source["name"])

    return base.reset_index(drop=True)


def _instantiate_strategies() -> List[AbstractStrategy]:
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
    write_line("Starting Ranking Runner...")

    data = assemble_ranking_data()
    if data.empty:
        write_line("No data available to rank.")
        return

    strategies = _instantiate_strategies()
    write_line(f"Running {len(strategies)} ranking strategies.")

    today = datetime.now(timezone.utc).date()
    for strategy in strategies:
        try:
            results = strategy.rank(data, today)
            if results:
                save_rankings(results)
            else:
                write_line(f"No results generated for strategy: {strategy.name}")
        except Exception as exc:
            write_line(f"Error executing strategy {strategy.name}: {exc}")

    write_line("Ranking process completed.")


if __name__ == "__main__":
    main()
