import shutil
import os
import sys

src = r"c:\Users\rdpro\Projects\AssetAllocation\scripts\market_analysis"
dst = r"c:\Users\rdpro\Projects\AssetAllocation\scripts\market_data"

print(f"Checking source: {src}")
if not os.path.exists(src):
    print("Source does not exist!")
else:
    print("Source exists.")

print(f"Checking destination: {dst}")
if os.path.exists(dst):
    print("Destination already exists.")
else:
    print("Destination does not exist. Attempting to create/copy...")

try:
    if not os.path.exists(dst):
        shutil.copytree(src, dst)
        print("Copytree successful.")
    else:
        print("Skipping copytree as destination exists.")
except Exception as e:
    print(f"Error during copytree: {e}")

# Verify content
if os.path.exists(dst):
    print("Listing destination content:")
    print(os.listdir(dst))
else:
    print("Destination still missing after attempt.")
