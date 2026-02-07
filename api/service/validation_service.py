import logging
from typing import Any, Dict
from datetime import datetime, timezone

from api.data_service import DataService


logger = logging.getLogger("asset-allocation.api.service.validation")

class ValidationService:
    """
    Service to compute data quality validation statistics for Bronze, Silver, and Gold layers.
    """

    @staticmethod
    def get_validation_report(layer: str, domain: str) -> Dict[str, Any]:
        """
        Generates a validation report for a specific layer and domain.
        Returns a dictionary containing file counts, validation stats, and health checks.
        """
        layer_key = str(layer or "").strip().lower()
        domain_key = str(domain or "").strip().lower()

        logger.info(f"Generating validation report for {layer_key}/{domain_key}")

        # 1. basic metadata (re-using existing domain metadata logic if possible, 
        # but for now we'll fetch data to compute stats manually as per plan)
        try:
             # Fetch a sample of data to compute stats. 
             # In a real heavy-load scenario, we might want to use pre-computed stats from Delta Log
             # or a dedicated stats job. For this implementation, we process loaded data.
             # Limit to 1000 rows for performance safety during on-demand check.
            data = DataService.get_data(layer_key, domain_key, limit=1000)
        except Exception as e:
            logger.error(f"Failed to fetch data for validation: {e}")
            return {
                "layer": layer_key,
                "domain": domain_key,
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

        if not data:
            return {
                "layer": layer_key,
                "domain": domain_key,
                "status": "empty",
                "rowCount": 0,
                "columns": [],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

        # 2. Compute Column Stats
        # We need to derive columns from the first row or consistent schema
        columns_stats = []
        if len(data) > 0:
            keys = data[0].keys()
            for col in keys:
                values = [row.get(col) for row in data]
                
                # Count Not Null
                not_null_count = sum(1 for v in values if v is not None and v != "")
                
                # Count Unique
                try:
                    unique_count = len(set(v for v in values if v is not None))
                except TypeError:
                    # Handle unhashable types (lists, dicts) by converting to str
                    unique_count = len(set(str(v) for v in values if v is not None))

                columns_stats.append({
                    "name": col,
                    "type": type(values[0]).__name__ if values else "unknown",
                    "total": len(data),
                    "unique": unique_count,
                    "notNull": not_null_count,
                    "null": len(data) - not_null_count
                })

        # 3. Construct Final Report
        return {
            "layer": layer_key,
            "domain": domain_key,
            "status": "healthy",
            "rowCount": len(data),
            "columns": columns_stats,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sampleLimit": 1000 # indicating these stats are based on a sample
        }
