
import os
import sys
import logging
from dotenv import load_dotenv

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load env vars
load_dotenv()

from core.core import get_symbols

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_symbols():
    logger.info("Verifying get_symbols()...")
    df = get_symbols()
    
    if df is None:
        logger.error("get_symbols returned None!")
        sys.exit(1)
        
    if df.empty:
        logger.error("get_symbols returned empty DataFrame!")
        sys.exit(1)
        
    logger.info(f"Retrieved {len(df)} symbols.")
    logger.info(f"Columns: {df.columns.tolist()}")
    
    required_cols = ['Symbol', 'Name', 'Description', 'Sector', 'Industry', 'Country']
    missing = [c for c in required_cols if c not in df.columns]
    
    if missing:
        logger.error(f"Missing columns: {missing}")
        sys.exit(1)
        
    # Print sample
    print(df.head())
    
    logger.info("Verification PASSED.")

if __name__ == "__main__":
    verify_symbols()
