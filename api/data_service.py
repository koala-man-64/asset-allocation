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
