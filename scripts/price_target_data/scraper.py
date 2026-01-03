
import sys
import os
import warnings
from pathlib import Path

# Adjust path to find 'scripts' when running directly
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from scripts.common import core as mdc
from scripts.common import config as cfg
from scripts.price_target_data import core as pt_lib

warnings.filterwarnings('ignore')


import asyncio
# ... imports ...

def main():
    if not cfg.AZURE_CONTAINER_TARGETS:
        raise ValueError("Environment variable 'AZURE_CONTAINER_TARGETS' is strictly required for Price Target Scraper.")

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

