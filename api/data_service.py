from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from core import config as cfg
from core import delta_core
from core.pipeline import DataPaths

class DataService:
    """
    Service layer for accessing financial data from Delta Lake storage.
    Decouples API from direct pipeline script usage.
    """

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
        """
        resolved_layer = str(layer or "").strip().lower()
        resolved_domain = str(domain or "").strip().lower()
        container = DataService._container_for_layer(resolved_layer)
        
        # Determine Path based on Domain & Layer
        path = ""
        is_raw = resolved_layer in ("silver", "bronze")

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
        ticker: str,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Specialized retrieval for Finance data.
        """
        resolved_layer = str(layer or "").strip().lower()
        resolved_sub = str(sub_domain or "").strip().lower()
        container = DataService._container_for_layer(resolved_layer)
        
        if resolved_layer in ("silver", "bronze"):
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
            path = DataPaths.get_gold_finance_path(ticker)
                
        return DataService._read_delta(container, path, limit=limit)

    @staticmethod
    def _read_delta(container: str, path: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        try:
            df = delta_core.load_delta(container, path)
            if df is None:
                raise FileNotFoundError(f"Delta table not found: {container}/{path}")

            # Preserve nulls (do not coerce to 0); make JSON-safe.
            if limit:
                df = df.head(limit)
            df = df.where(pd.notnull(df), None)
            return df.to_dict(orient="records")
        except Exception as e:
            # Log error
            raise FileNotFoundError(f"Failed to read data at {path}: {str(e)}")
