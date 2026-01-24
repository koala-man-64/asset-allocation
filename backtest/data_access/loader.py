from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
from deltalake import DeltaTable

from backtest.config import BacktestConfig, DataConfig


_PLACEHOLDER_PATTERN = re.compile(r"{\s*symbol\s*}")


@dataclass(frozen=True)
class DeltaRef:
    container: str
    path: str


def _validate_symbol_for_path(symbol: str) -> str:
    text = str(symbol)
    if "/" in text or "\\" in text or ".." in text:
        raise ValueError(f"Invalid symbol for path templating: {symbol!r}")
    return text


def _find_column(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = list(df.columns)
    lower_map = {str(c).lower(): str(c) for c in cols}
    for candidate in candidates:
        if candidate in cols:
            return candidate
        mapped = lower_map.get(str(candidate).lower())
        if mapped:
            return mapped
    return None


def _coerce_datetime(series: pd.Series) -> pd.Series:
    value = pd.to_datetime(series, errors="coerce")
    if hasattr(value.dt, "tz_convert") and value.dt.tz is not None:
        value = value.dt.tz_convert(None)
    return value


def _filter_by_config(df: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    symbol_col = _find_column(out, ["symbol", "Symbol", "ticker", "Ticker"])
    if symbol_col:
        out[symbol_col] = out[symbol_col].astype(str)
        out = out[out[symbol_col].isin(set(config.universe.symbols))]

    date_col = _find_column(out, ["date", "Date", "obs_date"])
    if date_col:
        out[date_col] = _coerce_datetime(out[date_col])
        dates = out[date_col].dt.date
        out = out[(dates >= config.start_date) & (dates <= config.end_date)]

    return out.reset_index(drop=True)


def _read_local_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported local file type for {path} (expected .csv or .parquet).")


def _parse_delta_ref(value: str) -> DeltaRef:
    text = str(value).strip()
    if not text:
        raise ValueError("Delta path is empty.")

    if text.startswith("abfss://"):
        parsed = urlparse(text)
        # abfss://<container>@<account>.dfs.core.windows.net/<path>
        if not parsed.netloc or "@" not in parsed.netloc:
            raise ValueError(f"Invalid abfss URI: {text!r}")
        container = parsed.netloc.split("@", 1)[0]
        path = parsed.path.lstrip("/")
        if not container or not path:
            raise ValueError(f"Invalid abfss URI: {text!r}")
        return DeltaRef(container=container, path=path)

    # Accept simple container/path format.
    if "/" not in text:
        raise ValueError(
            "ADLS/Delta paths must be either abfss://... URIs or 'container/path' strings "
            f"(got {text!r})."
        )
    container, path = text.split("/", 1)
    container = container.strip()
    path = path.strip().lstrip("/")
    if not container or not path:
        raise ValueError(f"Invalid Delta reference: {text!r}")
    return DeltaRef(container=container, path=path)


def _read_delta_table(
    ref: DeltaRef,
    *,
    columns: Optional[list[str]] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> pd.DataFrame:
    from core import delta_core

    uri = delta_core.get_delta_table_uri(ref.container, ref.path)
    opts = delta_core.get_delta_storage_options(ref.container)

    dt = DeltaTable(uri, storage_options=opts)

    filters = None
    if start is not None and end is not None:
        schema_cols = [field.name for field in dt.schema().fields]
        date_col = None
        if "date" in schema_cols:
            date_col = "date"
        elif "Date" in schema_cols:
            date_col = "Date"
        if date_col:
            filters = [(date_col, ">=", start), (date_col, "<=", end)]

    df = dt.to_pandas(columns=columns, filters=filters)
    return df


def _load_prices_local(config: BacktestConfig, data: DataConfig) -> pd.DataFrame:
    if not data.price_path:
        raise ValueError("data.price_path is required when price_source=local.")

    path_text = str(data.price_path)
    if _PLACEHOLDER_PATTERN.search(path_text):
        frames = []
        for symbol in config.universe.symbols:
            resolved = Path(_PLACEHOLDER_PATTERN.sub(_validate_symbol_for_path(symbol), path_text))
            df = _read_local_table(resolved)
            if _find_column(df, ["symbol", "Symbol", "ticker", "Ticker"]) is None:
                df = df.copy()
                df["symbol"] = symbol
            frames.append(df)
        prices = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        prices = _read_local_table(Path(path_text))

    return _filter_by_config(prices, config)


def _load_signals_local(config: BacktestConfig, data: DataConfig) -> Optional[pd.DataFrame]:
    if not data.signal_path:
        return None
    signals = _read_local_table(Path(str(data.signal_path)))
    filtered = _filter_by_config(signals, config)
    return filtered if not filtered.empty else None


def _load_prices_delta(config: BacktestConfig, data: DataConfig) -> pd.DataFrame:
    if not data.price_path:
        raise ValueError("data.price_path is required when price_source=ADLS.")

    ref = _parse_delta_ref(data.price_path)
    start = datetime.combine(config.start_date, datetime.min.time())
    end = datetime.combine(config.end_date, datetime.max.time())

    if _PLACEHOLDER_PATTERN.search(ref.path):
        frames = []
        for symbol in config.universe.symbols:
            symbol_path = _PLACEHOLDER_PATTERN.sub(_validate_symbol_for_path(symbol), ref.path)
            df = _read_delta_table(DeltaRef(container=ref.container, path=symbol_path), start=start, end=end)
            if df is None or df.empty:
                continue
            if _find_column(df, ["symbol", "Symbol", "ticker", "Ticker"]) is None:
                df = df.copy()
                df["symbol"] = symbol
            frames.append(df)
        prices = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        prices = _read_delta_table(ref, start=start, end=end)

    return _filter_by_config(prices, config)


def _load_signals_delta(config: BacktestConfig, data: DataConfig) -> Optional[pd.DataFrame]:
    if not data.signal_path:
        return None

    ref = _parse_delta_ref(data.signal_path)
    start = datetime.combine(config.start_date, datetime.min.time())
    end = datetime.combine(config.end_date, datetime.max.time())
    signals = _read_delta_table(ref, start=start, end=end)
    filtered = _filter_by_config(signals, config)
    return filtered if not filtered.empty else None


def load_backtest_inputs(config: BacktestConfig) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Loads and filters inputs for a backtest run based on BacktestConfig.data.
    """
    if not config.data:
        raise ValueError("BacktestConfig.data is required to load inputs from paths.")

    data = config.data
    if data.price_source == "local":
        prices = _load_prices_local(config, data)
        signals = _load_signals_local(config, data)
    elif data.price_source == "ADLS":
        prices = _load_prices_delta(config, data)
        signals = _load_signals_delta(config, data)
    else:
        raise ValueError(f"Unsupported price_source: {data.price_source}")

    if prices.empty:
        raise ValueError("No price rows loaded after filtering; check universe/date range and data paths.")
    return prices, signals
