
import sys
import subprocess
import os

project_root = r'c:\Users\rdpro\Projects\AssetAllocation'
os.chdir(project_root)

with open('pytest_result.txt', 'w') as f:
    result = subprocess.run([sys.executable, '-m', 'pytest', 'tests/finance_data/test_finance_data_scraper.py'], stdout=f, stderr=f, text=True)
    f.write(f"\nExit Code: {result.returncode}")
