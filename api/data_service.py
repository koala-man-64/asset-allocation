from __future__ import annotations

import math
import json
from io import BytesIO
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core import config as cfg
from core import core as mdc
from core import delta_core
from core.pipeline import DataPaths

class DataService:
    """
    Service layer for accessing financial data from Delta Lake storage.
    Decouples API from direct pipeline script usage.
    """

    @staticmethod
    def _sanitize_json_value(value: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, (bool, np.bool_)):
            return bool(value)

        if isinstance(value, (float, np.floating)):
            numeric = float(value)
            if not math.isfinite(numeric):
                return None
            return numeric

        if isinstance(value, (int, np.integer)):
            return int(value)

        if isinstance(value, dict):
            return {str(k): DataService._sanitize_json_value(v) for k, v in value.items()}

        if isinstance(value, list):
            return [DataService._sanitize_json_value(v) for v in value]

        if isinstance(value, tuple):
            return [DataService._sanitize_json_value(v) for v in value]

        return value

    @staticmethod
    def _df_to_records_json_safe(df: pd.DataFrame, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if limit:
            df = df.head(int(limit))

        # Starlette's JSONResponse enforces RFC-compliant JSON and rejects NaN/Inf.
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.astype(object).where(pd.notnull(df), None)

        records: Any = df.to_dict(orient="records")
        sanitized = DataService._sanitize_json_value(records)
        return sanitized

    @staticmethod
    def _first_bronze_blob_path(
        client: Any,
        *,
        container: str,
        prefix: str,
        allowed_suffixes: tuple[str, ...],
    ) -> str:
        normalized = str(prefix or "").strip()
        if not normalized:
            raise ValueError("prefix is required to list Bronze blobs.")
        if not normalized.endswith("/"):
            normalized = normalized + "/"

        list_files = getattr(client, "list_files", None)
        if callable(list_files):
            names = [str(name) for name in list_files(name_starts_with=normalized)]
            candidates = [name for name in names if name.lower().endswith(allowed_suffixes)]
            if not candidates:
                raise FileNotFoundError(f"No Bronze blobs found under {container}/{normalized}")
            return sorted(candidates)[0]

        container_client = getattr(client, "container_client", None)
        if container_client is not None and hasattr(container_client, "list_blobs"):
            for blob in container_client.list_blobs(name_starts_with=normalized):
                name = getattr(blob, "name", None)
                if name and str(name).lower().endswith(allowed_suffixes):
                    return str(name)
            raise FileNotFoundError(f"No Bronze blobs found under {container}/{normalized}")

        raise FileNotFoundError("Storage client does not support listing Bronze blobs.")

    @staticmethod
    def _container_for_layer(layer: str) -> str:
        key = str(layer or "").strip().lower()
        if key == "silver":
            return cfg.AZURE_CONTAINER_SILVER
        if key == "gold":
            return cfg.AZURE_CONTAINER_GOLD
        if key == "bronze":
            return cfg.AZURE_CONTAINER_BRONZE
        raise ValueError(f"Unsupported layer: {layer!r}")
    
    @staticmethod
    def get_data(
        layer: str, 
        domain: str, 
        ticker: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Generic data retrieval for market, earnings, and price-target domains.

        Notes
        - Silver/Gold use Delta tables.
        - Bronze stores raw source files (CSV/JSON/Parquet) partitioned by ticker and does not
          materialize by-date tables.
        """
        resolved_layer = str(layer or "").strip().lower()
        resolved_domain = str(domain or "").strip().lower()
        container = DataService._container_for_layer(resolved_layer)

        if resolved_layer == "bronze":
            return DataService._get_bronze_data(container=container, domain=resolved_domain, ticker=ticker, limit=limit)
        
        # Determine Path based on Domain & Layer
        path = ""
        is_raw = resolved_layer == "silver"

        if resolved_domain == "market":
            if ticker:
                path = DataPaths.get_market_data_path(ticker) if is_raw else DataPaths.get_gold_features_path(ticker)
            else:
                path = DataPaths.get_market_data_by_date_path() if is_raw else DataPaths.get_gold_features_by_date_path()
        elif resolved_domain == "finance":
            if is_raw:
                # Silver finance probing defaults to the by-date dataset (all symbols/sub-domains).
                path = DataPaths.get_finance_by_date_path()
            else:
                # Gold supports per-ticker and by-date datasets.
                path = DataPaths.get_gold_finance_path(ticker) if ticker else DataPaths.get_gold_finance_by_date_path()
        elif resolved_domain == "earnings":
            if ticker:
                path = DataPaths.get_earnings_path(ticker) if is_raw else DataPaths.get_gold_earnings_path(ticker)
            else:
                path = DataPaths.get_earnings_by_date_path() if is_raw else DataPaths.get_gold_earnings_by_date_path()
        elif resolved_domain in {"price-target", "price_target"}:
            if ticker:
                path = DataPaths.get_price_target_path(ticker) if is_raw else DataPaths.get_gold_price_targets_path(ticker)
            else:
                path = DataPaths.get_price_targets_by_date_path() if is_raw else DataPaths.get_gold_price_targets_by_date_path()
        else:
             raise ValueError(f"Domain '{domain}' not supported on generic endpoint")
             
        return DataService._read_delta(container, path, limit=limit)

    @staticmethod
    def get_finance_data(
        layer: str,
        sub_domain: str,
        ticker: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Specialized retrieval for Finance data.
        """
        resolved_layer = str(layer or "").strip().lower()
        resolved_sub = str(sub_domain or "").strip().lower()
        container = DataService._container_for_layer(resolved_layer)

        if resolved_layer == "bronze":
            client = mdc.get_storage_client(container)
            if client is None:
                raise FileNotFoundError(
                    f"Storage client unavailable for container={container!r}. "
                    "Set Azure storage env vars to enable Bronze exploration."
                )

            folder_map = {
                "balance_sheet": ("Balance Sheet", "quarterly_balance-sheet"),
                "income_statement": ("Income Statement", "quarterly_financials"),
                "cash_flow": ("Cash Flow", "quarterly_cash-flow"),
                "valuation": ("Valuation", "quarterly_valuation_measures"),
            }
            if resolved_sub not in folder_map:
                raise ValueError(f"Unknown finance sub-domain: {sub_domain}")

            folder, suffix = folder_map[resolved_sub]
            symbol = str(ticker or "").strip().upper() if ticker else ""
            blob_path = (
                f"finance-data/{folder}/{symbol}_{suffix}.csv"
                if symbol
                else DataService._first_bronze_blob_path(
                    client,
                    container=container,
                    prefix=f"finance-data/{folder}",
                    allowed_suffixes=(f"_{suffix}.csv",),
                )
            )
            return DataService._read_bronze_raw(container, blob_path, kind="csv", limit=limit, client=client)
        
        if resolved_layer == "silver":
            if not ticker:
                raise ValueError("ticker is required for Silver finance data.")
            folder_map = {
                "balance_sheet": ("Balance Sheet", "quarterly_balance-sheet"),
                "income_statement": ("Income Statement", "quarterly_financials"),
                "cash_flow": ("Cash Flow", "quarterly_cash-flow"),
                "valuation": ("Valuation", "quarterly_valuation_measures")
            }
            if resolved_sub not in folder_map:
                raise ValueError(f"Unknown finance sub-domain: {sub_domain}")
            
            folder, suffix = folder_map[resolved_sub]
            path = DataPaths.get_finance_path(folder, ticker, suffix)
        else:
            # Gold logic
            if not ticker:
                raise ValueError("ticker is required for Gold finance data.")
            path = DataPaths.get_gold_finance_path(ticker)
                
        return DataService._read_delta(container, path, limit=limit)

    @staticmethod
    def _get_bronze_data(
        *,
        container: str,
        domain: str,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        client = mdc.get_storage_client(container)
        if client is None:
            raise FileNotFoundError(
                f"Storage client unavailable for container={container!r}. "
                "Set Azure storage env vars to enable Bronze exploration."
            )

        symbol = str(ticker or "").strip().upper() if ticker else ""

        if domain == "market":
            blob_path = (
                f"market-data/{symbol}.csv"
                if symbol
                else DataService._first_bronze_blob_path(
                    client,
                    container=container,
                    prefix="market-data",
                    allowed_suffixes=(".csv",),
                )
            )
            return DataService._read_bronze_raw(
                container,
                blob_path,
                kind="csv",
                limit=limit,
                client=client,
            )

        if domain == "earnings":
            prefix = getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data") or "earnings-data"
            blob_path = (
                f"{prefix}/{symbol}.json"
                if symbol
                else DataService._first_bronze_blob_path(
                    client,
                    container=container,
                    prefix=prefix,
                    allowed_suffixes=(".json",),
                )
            )
            return DataService._read_bronze_raw(
                container,
                blob_path,
                kind="json",
                limit=limit,
                client=client,
            )

        if domain == "finance":
            if symbol:
                symbol_prefixes = [
                    f"finance-data/balance_sheet/{symbol}_",
                    f"finance-data/income_statement/{symbol}_",
                    f"finance-data/cash_flow/{symbol}_",
                    f"finance-data/valuation/{symbol}_",
                ]
                blob_path = ""
                for prefix in symbol_prefixes:
                    try:
                        blob_path = DataService._first_bronze_blob_path(
                            client,
                            container=container,
                            prefix=prefix,
                            allowed_suffixes=(".csv",),
                        )
                        break
                    except FileNotFoundError:
                        continue
                if not blob_path:
                    raise FileNotFoundError(
                        f"No Bronze finance blobs found for symbol={symbol!r} under {container}/finance-data/"
                    )
            else:
                blob_path = DataService._first_bronze_blob_path(
                    client,
                    container=container,
                    prefix="finance-data",
                    allowed_suffixes=(".csv",),
                )

            return DataService._read_bronze_raw(
                container,
                blob_path,
                kind="csv",
                limit=limit,
                client=client,
            )

        if domain in {"price-target", "price_target"}:
            blob_path = (
                f"price-target-data/{symbol}.parquet"
                if symbol
                else DataService._first_bronze_blob_path(
                    client,
                    container=container,
                    prefix="price-target-data",
                    allowed_suffixes=(".parquet",),
                )
            )
            return DataService._read_bronze_raw(
                container,
                blob_path,
                kind="parquet",
                limit=limit,
                client=client,
            )

        raise ValueError(f"Domain '{domain}' not supported on Bronze explorer endpoint")

    @staticmethod
    def _read_bronze_raw(
        container: str,
        blob_path: str,
        *,
        kind: str,
        limit: Optional[int] = None,
        client: Any = None,
    ) -> List[Dict[str, Any]]:
        if client is None:
            client = mdc.get_storage_client(container)
        if client is None:
            raise FileNotFoundError(
                f"Storage client unavailable for container={container!r}. "
                "Set Azure storage env vars to enable Bronze exploration."
            )

        raw_bytes = mdc.read_raw_bytes(blob_path, client=client)
        if not raw_bytes:
            raise FileNotFoundError(f"Raw blob not found: {container}/{blob_path}")

        df: pd.DataFrame
        kind_key = str(kind or "").strip().lower()
        if kind_key == "csv":
            df = pd.read_csv(BytesIO(raw_bytes))
        elif kind_key == "json":
            payload = json.loads(raw_bytes.decode("utf-8"))
            if isinstance(payload, list):
                df = pd.DataFrame(payload)
            elif isinstance(payload, dict):
                df = pd.DataFrame([payload])
            else:
                raise ValueError(f"Unsupported JSON payload type: {type(payload).__name__}")
        elif kind_key == "parquet":
            df = pd.read_parquet(BytesIO(raw_bytes))
        else:
            raise ValueError(f"Unsupported bronze kind={kind!r}")

        return DataService._df_to_records_json_safe(df, limit=limit)

    @staticmethod
    def _read_delta(container: str, path: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        try:
            df = delta_core.load_delta(container, path)
            if df is None:
                raise FileNotFoundError(f"Delta table not found: {container}/{path}")

            return DataService._df_to_records_json_safe(df, limit=limit)
        except Exception as e:
            # Log error
            raise FileNotFoundError(f"Failed to read data at {path}: {str(e)}")

    @staticmethod
    def _extract_finance_domain_rows(
        layer: str,
        domain: str,
        ticker: Optional[str],
        sample_rows: int,
    ) -> List[Dict[str, Any]]:
        normalized_domain = str(domain or "").strip().lower()
        if normalized_domain.startswith("finance/"):
            _, _, remainder = normalized_domain.partition("/")
            sub_domain = remainder.strip()
            if not sub_domain:
                raise ValueError("finance domain requires a sub-domain, e.g. finance/balance_sheet")
            return DataService.get_finance_data(layer, sub_domain, ticker, limit=sample_rows)
        return DataService.get_data(layer, domain, ticker, limit=sample_rows)

    @staticmethod
    def _format_number_bucket_edge(value: float) -> float:
        if value == int(value):
            return float(int(value))
        return float(np.round(value, 6))

    @staticmethod
    def get_column_profile(
        layer: str,
        domain: str,
        column: str,
        *,
        ticker: Optional[str] = None,
        bins: int = 20,
        sample_rows: int = 10000,
        top_values: int = 20,
    ) -> Dict[str, Any]:
        normalized_layer = str(layer or "").strip().lower()
        normalized_domain = str(domain or "").strip().lower()
        normalized_column = str(column or "").strip()

        if not normalized_layer:
            raise ValueError("layer is required.")
        if normalized_layer not in {"bronze", "silver", "gold"}:
            raise ValueError("Layer must be 'bronze', 'silver', or 'gold'.")
        if not normalized_domain:
            raise ValueError("domain is required.")
        if not normalized_column:
            raise ValueError("column is required.")

        resolved_ticker = None if ticker is None else str(ticker).strip().upper() or None
        resolved_bins = max(3, min(int(bins), 200))
        resolved_sample_rows = max(10, min(int(sample_rows), 100000))
        resolved_top_values = max(1, min(int(top_values), 200))

        rows = DataService._extract_finance_domain_rows(
            normalized_layer,
            normalized_domain,
            resolved_ticker,
            sample_rows=resolved_sample_rows,
        )
        if not rows:
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "string",
                "totalRows": 0,
                "nonNullCount": 0,
                "nullCount": 0,
                "sampleRows": resolved_sample_rows,
                "bins": [],
                "uniqueCount": 0,
                "duplicateCount": 0,
                "topValues": [],
            }

        df = pd.DataFrame(rows)
        if normalized_column not in df.columns:
            raise ValueError(f"Column '{normalized_column}' not found in sampled data.")

        series = df[normalized_column]
        total_rows = int(len(df))
        series_non_null = series.dropna()
        non_null_count = int(len(series_non_null))
        null_count = total_rows - non_null_count

        if non_null_count == 0:
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "string",
                "totalRows": total_rows,
                "nonNullCount": 0,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": [],
                "uniqueCount": 0,
                "duplicateCount": 0,
                "topValues": [],
            }

        candidate_str = series_non_null.astype(str).str.strip()
        if candidate_str.empty:
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "string",
                "totalRows": total_rows,
                "nonNullCount": non_null_count,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": [],
                "uniqueCount": 0,
                "duplicateCount": 0,
                "topValues": [],
            }

        parsed_date = pd.to_datetime(series_non_null, errors="coerce", utc=False)
        date_count = int(parsed_date.notna().sum())
        date_ratio = date_count / non_null_count if non_null_count else 0.0

        if date_ratio >= 0.7:
            date_vals = parsed_date.dropna().dt.to_period("M").astype(str)
            value_counts = date_vals.value_counts().sort_index()
            buckets = []
            for key, count in value_counts.items():
                buckets.append({
                    "label": str(key),
                    "count": int(count),
                })
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "date",
                "totalRows": total_rows,
                "nonNullCount": non_null_count,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": buckets,
                "uniqueCount": int(date_vals.nunique()),
                "duplicateCount": int(non_null_count - date_vals.nunique()),
                "topValues": [],
            }

        numeric = pd.to_numeric(series_non_null, errors="coerce")
        numeric_count = int(numeric.notna().sum())
        numeric_ratio = numeric_count / non_null_count if non_null_count else 0.0

        if numeric_ratio >= 0.7 and numeric_count > 0:
            numeric_clean = numeric.replace([np.inf, -np.inf], np.nan).dropna()

            if numeric_clean.empty:
                kind = "string"
                return {
                    "layer": normalized_layer,
                    "domain": normalized_domain,
                    "column": normalized_column,
                    "kind": kind,
                    "totalRows": total_rows,
                    "nonNullCount": non_null_count,
                    "nullCount": null_count,
                    "sampleRows": resolved_sample_rows,
                    "bins": [],
                    "uniqueCount": 0,
                    "duplicateCount": 0,
                    "topValues": [],
                }

            if len(numeric_clean.unique()) == 1:
                value = DataService._format_number_bucket_edge(float(numeric_clean.iloc[0]))
                return {
                    "layer": normalized_layer,
                    "domain": normalized_domain,
                    "column": normalized_column,
                    "kind": "numeric",
                    "totalRows": total_rows,
                    "nonNullCount": non_null_count,
                    "nullCount": null_count,
                    "sampleRows": resolved_sample_rows,
                    "bins": [{"label": str(value), "count": int(len(numeric_clean)), "start": value, "end": value}],
                    "uniqueCount": 1,
                    "duplicateCount": int(non_null_count - 1),
                    "topValues": [],
                }

            try:
                bucketed = pd.cut(numeric_clean, bins=resolved_bins)
            except ValueError:
                unique_values = numeric_clean.drop_duplicates().sort_values()
                min_value = float(unique_values.min())
                max_value = float(unique_values.max())
                if min_value == max_value:
                    return {
                        "layer": normalized_layer,
                        "domain": normalized_domain,
                        "column": normalized_column,
                        "kind": "numeric",
                        "totalRows": total_rows,
                        "nonNullCount": non_null_count,
                        "nullCount": null_count,
                        "sampleRows": resolved_sample_rows,
                        "bins": [{"label": str(DataService._format_number_bucket_edge(min_value)), "count": int(len(numeric_clean)), "start": min_value, "end": max_value}],
                        "uniqueCount": int(unique_values.nunique()),
                        "duplicateCount": int(non_null_count - unique_values.nunique()),
                        "topValues": [],
                    }
                bucketed = pd.qcut(
                    numeric_clean,
                    q=min(20, int(numeric_clean.nunique())),
                    duplicates="drop"
                )
                buckets = bucketed.value_counts().sort_index()
            else:
                buckets = bucketed.value_counts().sort_index()

            payload = []
            for key, count in buckets.items():
                if isinstance(key, pd.Interval):
                    left = DataService._format_number_bucket_edge(float(key.left))
                    right = DataService._format_number_bucket_edge(float(key.right))
                    if isinstance(key.left, (int, float)) and isinstance(key.right, (int, float)) and key.left == key.right:
                        label = str(left)
                    else:
                        label = f"{left} to {right}"
                    payload.append(
                        {
                            "label": label,
                            "count": int(count),
                            "start": left,
                            "end": right,
                        }
                    )
                else:
                    value = DataService._format_number_bucket_edge(float(key))
                    payload.append({"label": str(value), "count": int(count), "start": value, "end": value})

            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "numeric",
                "totalRows": total_rows,
                "nonNullCount": non_null_count,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": payload,
                "uniqueCount": int(numeric_clean.nunique()),
                "duplicateCount": int(non_null_count - numeric_clean.nunique()),
                "topValues": [],
            }

        value_counts = candidate_str.value_counts()
        unique = int(value_counts.shape[0])
        top_n = value_counts.head(resolved_top_values)

        return {
            "layer": normalized_layer,
            "domain": normalized_domain,
            "column": normalized_column,
            "kind": "string",
            "totalRows": total_rows,
            "nonNullCount": non_null_count,
            "nullCount": null_count,
            "sampleRows": resolved_sample_rows,
            "bins": [],
            "uniqueCount": unique,
            "duplicateCount": int(non_null_count - unique),
            "topValues": [
                {"value": str(value), "count": int(count)} for value, count in top_n.items()
            ],
        }
