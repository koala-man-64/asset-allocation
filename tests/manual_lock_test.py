
import sys
import os
import time
import subprocess
from pathlib import Path

# Add project root to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

from scripts.common import core as mdc
from scripts.common import config as cfg

def run_worker(job_name, sleep_time):
    print(f"[Worker] Attempting to acquire lock for {job_name}...")
    try:
        with mdc.JobLock(job_name):
            print(f"[Worker] Lock acquired. Sleeping for {sleep_time}s...")
            time.sleep(sleep_time)
            print("[Worker] Waking up and releasing lock.")
    except SystemExit:
        print("[Worker] Failed to acquire lock (SystemExit).")
    except Exception as e:
        print(f"[Worker] Error: {e}")

if __name__ == "__main__":
    job_name = "test-verification-lock"
    
    # 1. Start a subprocess that holds the lock for 10 seconds
    print("[Main] Starting worker process...")
    # We run this script itself as a worker
    worker_script = """
import sys
import os
import time
current_dir = os.path.dirname(os.path.abspath('__file__'))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
    sys.path.append(project_root)
from scripts.common import core as mdc
import time

job_name = "test-verification-lock"
print("   [Child] Subprocess start")
try:
    with mdc.JobLock(job_name):
        print("   [Child] Lock acquired. Holding for 5s...")
        time.sleep(5)
        print("   [Child] Releasing...")
except SystemExit:
    print("   [Child] Could not acquire lock.")
"""
    # Write worker script to temp file because passing code string with imports is valid but complex to get paths right
    # Actually, simpler: just invoke this script with a flag
    
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        run_worker(job_name, 20)
        sys.exit(0)

    # Main process
    p = subprocess.Popen([sys.executable, __file__, "--worker"])
    
    # Wait for worker to start and acquire lock
    time.sleep(10) 
    
    print("[Main] Worker should be holding lock now. Attempting to acquire same lock...")
    try:
        with mdc.JobLock(job_name):
            print("[Main] ERROR: I acquired the lock! Concurrency check FAILED.")
    except SystemExit:
        print("[Main] SUCCESS: SystemExit caught. Lock successfully prevented concurrent run.")
    except Exception as e:
        print(f"[Main] Unexpected error: {e}")

    p.wait()
    print("[Main] Done.")
