
import sys
import os
import warnings
from pathlib import Path

# Adjust path to find 'scripts' when running directly
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from scripts.common import core as mdc
from scripts.price_target_data import config as cfg
from scripts.price_target_data import core as pt_lib

warnings.filterwarnings('ignore')


import asyncio

def _validate_environment() -> None:
    required = [
        "AZURE_CONTAINER_TARGETS",
        "DOWNLOADS_PATH", 
        "PLAYWRIGHT_USER_DATA_DIR",
        "YAHOO_USERNAME",
        "YAHOO_PASSWORD",
        "NASDAQ_API_KEY"
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
        raise ValueError("Environment variable 'AZURE_CONTAINER_BRONZE' is strictly required for Price Target Scraper.")

def main():
    mdc.log_environment_diagnostics()
    _validate_environment()

    if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
        pt_lib.run_interactive_mode()
    else:
        # Load symbols at scraper level
        df_symbols = mdc.get_symbols()
        asyncio.run(pt_lib.run_price_target_refresh(df_symbols))

if __name__ == "__main__":
    job_name = 'price-target-job'
    with mdc.JobLock(job_name):
        main()

