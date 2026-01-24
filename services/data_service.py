from typing import Any, Dict, List, Optional
from core import pipeline
from deltalake import DeltaTable  # type: ignore

class DataService:
    """
    Service layer for accessing financial data from Delta Lake storage.
    Decouples API from direct pipeline script usage.
    """
    
    @staticmethod
    def get_data(
        layer: str, 
        domain: str, 
        ticker: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Generic data retrieval for market, earnings, and price-target domains.
        """
        container = pipeline.Containers.SILVER if layer == "silver" else pipeline.Containers.BRONZE
        
        # Determine Path based on Domain & Layer
        path = ""
        if domain == "market":
            if ticker:
                path = pipeline.DataPaths.get_market_data_path(ticker) if layer == "silver" else pipeline.DataPaths.get_gold_features_path(ticker)
            else:
                path = pipeline.DataPaths.get_market_data_by_date_path() if layer == "silver" else pipeline.DataPaths.get_gold_features_by_date_path()
        elif domain == "earnings":
            if ticker:
                path = pipeline.DataPaths.get_earnings_path(ticker) if layer == "silver" else pipeline.DataPaths.get_gold_earnings_path(ticker)
            else:
                path = pipeline.DataPaths.get_earnings_by_date_path() if layer == "silver" else pipeline.DataPaths.get_gold_earnings_by_date_path()
        elif domain == "price-target":
            if ticker:
                path = pipeline.DataPaths.get_price_target_path(ticker) if layer == "silver" else pipeline.DataPaths.get_gold_price_targets_path(ticker)
            else:
                path = pipeline.DataPaths.get_price_targets_by_date_path() if layer == "silver" else pipeline.DataPaths.get_gold_price_targets_by_date_path()
        else:
             raise ValueError(f"Domain '{domain}' not supported on generic endpoint")
             
        return DataService._read_delta(container, path)

    @staticmethod
    def get_finance_data(
        layer: str,
        sub_domain: str,
        ticker: str
    ) -> List[Dict[str, Any]]:
        """
        Specialized retrieval for Finance data.
        """
        container = pipeline.Containers.SILVER if layer == "silver" else pipeline.Containers.BRONZE # Gold container? pipeline.py variable? Assuming similar logic.
        # Actually pipeline.py defines containers often. 
        # For now assume same container logic as endpoint: resolve_container(layer, "finance") -> usually silver container.
        
        if layer == "silver":
            folder_map = {
                "balance_sheet": ("Balance Sheet", "quarterly_balance-sheet"),
                "income_statement": ("Income Statement", "quarterly_financials"),
                "cash_flow": ("Cash Flow", "quarterly_cash-flow"),
                "valuation": ("Valuation", "quarterly_valuation_measures")
            }
            if sub_domain not in folder_map:
                raise ValueError(f"Unknown finance sub-domain: {sub_domain}")
            
            folder, suffix = folder_map[sub_domain]
            path = pipeline.DataPaths.get_finance_path(folder, ticker, suffix)
        else:
            # Gold logic
            if sub_domain == "all":
                path = pipeline.DataPaths.get_gold_finance_path(ticker)
            else:
                path = pipeline.DataPaths.get_gold_finance_path(ticker)
                
        return DataService._read_delta(container, path)

    @staticmethod
    def _read_delta(container: str, path: str) -> List[Dict[str, Any]]:
        try:
            # Assuming shared access signature or current creds work
            dt = pipeline.get_delta_table(container, path)
            df = dt.to_pandas()
            # Handle NaN/NaT for JSON serializability if needed
            df = df.fillna(0) # Simple fallback, potentially risky for real data
            return df.to_dict(orient="records")
        except Exception as e:
            # Log error
            raise FileNotFoundError(f"Failed to read data at {path}: {str(e)}")
