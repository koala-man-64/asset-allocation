
import os
import sys
import logging
from dotenv import load_dotenv

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load env vars
load_dotenv()

from backtest.config import BacktestConfig
from core.strategy_repository import StrategyRepository

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_db_strategy():
    strategy_name = "Test_Strategy_DB"
    
    # 1. Verify it exists in DB (using Repo directly)
    repo = StrategyRepository()
    config = repo.get_strategy_config(strategy_name)
    
    if not config:
        logger.error(f"Strategy '{strategy_name}' NOT found in DB. Upload failed?")
        sys.exit(1)
        
    logger.info(f"Confirmed '{strategy_name}' exists in DB.")
    
    # 2. Verify BacktestConfig loads it via 'from_dict' with name reference
    logger.info("Testing BacktestConfig hydration...")
    
    raw_config = {
        "run_name": "VERIFY_DB_LOAD",
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "universe": {
            "symbols": ["AAPL"]
        },
        "strategy": strategy_name, # REFERENCE BY NAME
        "formatting": "strict"
    }
    
    try:
        cfg = BacktestConfig.from_dict(raw_config)
        logger.info("BacktestConfig successfully loaded strategy from DB!")
        logger.info(f"Loaded Strategy Type: {cfg.strategy.class_name}")
        
        # Check a specific field to ensure content matches
        if not cfg.strategy.parameters.get("rebalance"):
             logger.error("Loaded strategy missing 'rebalance' parameter!")
             sys.exit(1)
             
    except Exception as e:
        logger.error(f"BacktestConfig hydration failed: {e}")
        sys.exit(1)

    logger.info("Verification PASSED.")

if __name__ == "__main__":
    verify_db_strategy()
