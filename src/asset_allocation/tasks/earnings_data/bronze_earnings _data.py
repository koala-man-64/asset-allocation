
import os
import asyncio
import warnings
from pathlib import Path

from asset_allocation.core import core as mdc
from asset_allocation.tasks.earnings_data import config as cfg
from asset_allocation.tasks.earnings_data import core as earn_lib

warnings.filterwarnings('ignore')

def _validate_environment() -> None:
    required = [
        "AZURE_CONTAINER_EARNINGS",
        "DOWNLOADS_PATH", 
        "PLAYWRIGHT_USER_DATA_DIR",
        "YAHOO_USERNAME",
        "YAHOO_PASSWORD"
    ]
    missing = [name for name in required if not os.environ.get(name)]
    
    if not os.environ.get("AZURE_CONTAINER_COMMON"):
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

    if not cfg.AZURE_CONTAINER_BRONZE:
        raise ValueError("Environment variable 'AZURE_CONTAINER_BRONZE' is strictly required for Earnings Data Scraper.")

def main():
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    mdc.log_environment_diagnostics()
    _validate_environment()
    mdc.write_line("Fetching symbols...")
    df_symbols = mdc.get_symbols()
    


    try:
        asyncio.run(earn_lib.run_earnings_refresh(df_symbols))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Main loop error: {e}")

if __name__ == "__main__":
    job_name = 'bronze-earnings-job'
    with mdc.JobLock(job_name):
        main()
