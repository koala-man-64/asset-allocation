import sys
import pytest
import os

# Set absolute paths
project_root = r"c:\Users\rdpro\Projects\AssetAllocation - AG\asset-allocation"
src_dir = os.path.join(project_root, "src")
tests_dir = os.path.join(project_root, "tests")

# Add to sys.path
sys.path.insert(0, src_dir)

print(f"PYTHONPATH trace: {sys.path[0]}")
print(f"Current working directory: {os.getcwd()}")

# Run pytest collection
ret_code = pytest.main(["--collect-only", tests_dir])

print(f"\nPytest discovery return code: {ret_code}")
if ret_code == 0:
    print("SUCCESS: Test suite discovered successfully.")
else:
    print("FAILURE: Test suite discovery failed.")

sys.exit(ret_code)
