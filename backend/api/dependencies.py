import os
from typing import Generator, Optional
from scripts.common import delta_core
from scripts.common import config as cfg

# Re-export DeltaTable for use in endpoints
from deltalake import DeltaTable

def get_delta_table(container: str, path: str) -> DeltaTable:
    """
    Dependency to get a DeltaTable instance.
    """
    uri = delta_core.get_delta_table_uri(container, path)
    opts = delta_core.get_delta_storage_options(container)
    return DeltaTable(uri, storage_options=opts)

def resolve_container(layer: str) -> str:
    """
    Resolves the container name based on the layer.
    """
    layer = layer.lower()
    if layer == "silver":
        return cfg.AZURE_CONTAINER_SILVER
    elif layer == "gold":
        # Strategy/Ranking data is in Gold container per current setup? 
        # Or is Platinum a separate container? 
        # Checking config.py: AZURE_CONTAINER_RANKING exists.
        # But 'gold' layer usually implies AZURE_CONTAINER_COMMON (for Gold market/finance?) 
        # or specific containers?
        # Let's check config.py again. 
        # AZURE_CONTAINER_MARKET, FINANCE, EARNINGS, TARGETS are likely Gold containers?
        # Re-reading config.py:
        # AZURE_CONTAINER_MARKET = ...
        # AZURE_CONTAINER_BRONZE = ...
        # AZURE_CONTAINER_SILVER = ...
        # AZURE_CONTAINER_RANKING = ...
        # It seems 'Silver' is a single container. 
        # 'Gold' might be split or centralized.
        # Let's assume Gold data is in the domain-specific containers or Common?
        # Based on pipeline.py:
        # get_gold_features_path -> market/{ticker}
        # This implies Gold data might be in a 'Market' container?
        # Let's default to specific containers for Gold if possible, or raise if ambiguous.
        pass
    elif layer == "platinum":
        return cfg.AZURE_CONTAINER_RANKING
    
    raise ValueError(f"Unknown layer: {layer}")

def resolve_gold_container(domain: str) -> str:
    """
    Resolves Gold container by domain.
    """
    domain = domain.lower()
    if domain == "market":
        return cfg.AZURE_CONTAINER_MARKET
    elif domain == "finance":
        return cfg.AZURE_CONTAINER_FINANCE
    elif domain == "earnings":
        return cfg.AZURE_CONTAINER_EARNINGS
    elif domain == "price-target":
        return cfg.AZURE_CONTAINER_TARGETS
    else:
        # Fallback or specific
        return cfg.AZURE_CONTAINER_COMMON
