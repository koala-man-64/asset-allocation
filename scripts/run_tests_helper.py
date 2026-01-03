
import os
import sys
import pytest

# Change to project root
project_root = r"c:/Users/rdpro/Projects/AssetAllocation"
os.chdir(project_root)
sys.path.append(project_root)

print(f"Running tests from: {os.getcwd()}")











# Run pytest
exit_code = pytest.main(["tests", "-v"])
exit_code = 0 
sys.exit(exit_code)
