"""
Main Runner for the Ranking Framework.
Orchestrates data loading, strategy execution, and result saving.
"""
import os
import sys
import pandas as pd
from datetime import datetime, timezone

# Ensure project root is in path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.common import config as cfg
from scripts.common.core import write_line, load_parquet
from scripts.common.core import write_line, load_parquet

# Imports for data loading
from azure.storage.blob import BlobServiceClient

from scripts.ranking.core import save_rankings
from scripts.ranking.strategies import MomentumStrategy, ValueStrategy

def load_latest_data() -> pd.DataFrame:
    """
    Loads and merges the necessary data for ranking.
    For MVP, we will try to load the latest market data parquet.
    """
    write_line("Loading input data...")
    try:
        conn_str = cfg.AZURE_STORAGE_CONNECTION_STRING
        if not conn_str:
            write_line("Error: AZURE_STORAGE_CONNECTION_STRING not found.")
            return pd.DataFrame()
            
        service_client = BlobServiceClient.from_connection_string(conn_str)
        container_name = cfg.AZURE_CONTAINER_MARKET
        
        # Check if container name is set
        if not container_name:
             write_line("Error: AZURE_CONTAINER_MARKET not set.")
             return pd.DataFrame()

        market_client = service_client.get_container_client(container_name)
        
        # Load Market Data (MVP: using assumed output file from factor analysis or scraper)
        # NOTE: In a real system you might query bronze/silver delta tables directly.
        # For now, we reuse the 'get_historical_data_output.parquet' artifact if it serves as a master dataset.
        input_file = 'get_historical_data_output.parquet' 
        
        df = load_parquet(input_file, client=market_client)
        if df is None:
             write_line(f"Warning: {input_file} not found in {container_name}")
             return pd.DataFrame()
             
        return df

    except Exception as e:
        write_line(f"Error loading data: {e}")
        return pd.DataFrame()

def main():
    write_line("Starting Ranking Runner...")
    
    # 1. Load Data
    data = load_latest_data()
    if data.empty:
        write_line("No data available to rank.")
        return

    # 2. Define Strategies
    strategies = [
        MomentumStrategy(),
        ValueStrategy()
    ]
    
    # 3. Execution
    today = datetime.now(timezone.utc).date()
    
    for strategy in strategies:
        try:
            results = strategy.rank(data, today)
            if results:
                save_rankings(results)
            else:
                write_line(f"No results for strategy: {strategy.name}")
        except Exception as e:
            write_line(f"Error executing strategy {strategy.name}: {e}")

    write_line("Ranking process completed.")

if __name__ == "__main__":
    main()
