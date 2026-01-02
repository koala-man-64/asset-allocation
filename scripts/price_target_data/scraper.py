
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

def main():
    if not cfg.AZURE_CONTAINER_TARGETS:
        raise ValueError("Environment variable 'AZURE_CONTAINER_TARGETS' is strictly required for Price Target Scraper.")

    if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
        pt_lib.run_interactive_mode()
    else:
        pt_lib.run_batch_processing()

if __name__ == "__main__":
    job_name = 'price-target-job'
    with mdc.JobLock(job_name):
        main()
