import sys
import pytest
import os

# Set absolute paths
project_root = r"c:\Users\rdpro\Projects\AssetAllocation - AG\asset-allocation"
src_dir = os.path.join(project_root, "src")
tests_dir = os.path.join(project_root, "tests", "alpaca")

# Add to sys.path
sys.path.insert(0, src_dir)

print(f"PYTHONPATH trace: {sys.path[0]}")

# Run pytest execution
ret_code = pytest.main([tests_dir])

print(f"\nPytest execution return code: {ret_code}")
sys.exit(ret_code)
