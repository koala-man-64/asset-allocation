
import os
import sys
import logging
from dotenv import load_dotenv

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load env vars
load_dotenv()

from core.postgres import connect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cleanup_symbols():
    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        logger.error("POSTGRES_DSN not set.")
        sys.exit(1)
        
    try:
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                # Count before
                cur.execute("""
                    SELECT count(*) FROM symbols 
                    WHERE symbol IS NULL 
                       OR trim(symbol) = '' 
                       OR lower(symbol) = 'nan'
                """)
                count = cur.fetchone()[0]
                logger.info(f"Found {count} invalid symbols to delete.")
                
                if count > 0:
                    # Delete
                    cur.execute("""
                        DELETE FROM symbols 
                        WHERE symbol IS NULL 
                           OR trim(symbol) = '' 
                           OR lower(symbol) = 'nan'
                    """)
                    logger.info(f"Deleted {cur.rowcount} rows.")
                    conn.commit()
                else:
                    logger.info("No cleanup needed.")
                    
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    cleanup_symbols()
