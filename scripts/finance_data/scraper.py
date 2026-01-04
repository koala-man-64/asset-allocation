
import os
import sys
import asyncio
import warnings

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.common import core as mdc
from scripts.finance_data import config as cfg
from scripts.finance_data import core as fin_lib

warnings.filterwarnings('ignore')

def _validate_environment() -> None:

    config_container = os.environ.get("AZURE_CONTAINER_COMMON")

    required = [
        "AZURE_CONTAINER_FINANCE",
        "DOWNLOADS_PATH",
        "PLAYWRIGHT_USER_DATA_DIR",
        "YAHOO_USERNAME",
        "YAHOO_PASSWORD",
        "NASDAQ_API_KEY", # Keeping for consistency with original file validation list
    ]
    missing = [name for name in required if not os.environ.get(name)]
    if not config_container:
        missing.append("AZURE_CONTAINER_COMMON")


    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

    if not (account_name or conn_str):
        missing.append("AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING")

    if missing:
        raise RuntimeError(
            "Missing required environment configuration: "
            + ", ".join(missing)
        )
    
    if not cfg.AZURE_CONTAINER_FINANCE:
        raise ValueError("Environment variable 'AZURE_CONTAINER_FINANCE' is strictly required for Finance Data Scraper.")

async def main_async():
    mdc.log_environment_diagnostics()
    _validate_environment()
    mdc.write_line(f"Processing Business Data Scraper {mdc.get_current_timestamp_str()}...")

    # Load Universe
    df_symbols = mdc.get_symbols()
    
    # Apply Debug Filter
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG MODE: Restricting execution to {len(cfg.DEBUG_SYMBOLS)} symbols: {cfg.DEBUG_SYMBOLS}")
        df_symbols = df_symbols[df_symbols['Symbol'].isin(cfg.DEBUG_SYMBOLS)]        

    # Run Core Logic
    await fin_lib.refresh_finance_data_async(df_symbols)

if __name__ == "__main__":
    job_name = 'finance-data-job'
    with mdc.JobLock(job_name):
        asyncio.run(main_async())
