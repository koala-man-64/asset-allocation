
import sys
import os
import asyncio
import warnings
from pathlib import Path

# Adjust path to find 'scripts' when running directly
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from scripts.common import core as mdc
from scripts.earnings_data import config as cfg
from scripts.earnings_data import core as earn_lib

warnings.filterwarnings('ignore')

def main():
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if not cfg.AZURE_CONTAINER_EARNINGS:
        raise ValueError("Environment variable 'AZURE_CONTAINER_EARNINGS' is strictly required for Earnings Data Scraper.")

    # Load symbols
    mdc.log_environment_diagnostics()
    mdc.write_line("Fetching symbols...")
    df_symbols = mdc.get_symbols()
    


    try:
        asyncio.run(earn_lib.run_earnings_refresh(df_symbols))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Main loop error: {e}")

if __name__ == "__main__":
    job_name = 'earnings-data-job'
    with mdc.JobLock(job_name):
        main()
