import os
import shutil
from pathlib import Path

BASE_DIR = Path(r"c:\Users\rdpro\Projects\AssetAllocation")
DATA_DIR_OLD = BASE_DIR / "Data"
DATA_DIR_NEW = BASE_DIR / "data"

print(f"Starting refactor in {BASE_DIR}")

# 1. Rename Data -> data
if DATA_DIR_OLD.exists():
    # check if it is already lowercase 'data' on disk (by listing parent)
    # Windows is tricky. Let's do the rename dance.
    print(f"Found Data directory: {DATA_DIR_OLD}")
    TEMP = BASE_DIR / "data_tmp_refactor"
    try:
        os.rename(DATA_DIR_OLD, TEMP)
        os.rename(TEMP, DATA_DIR_NEW)
        print("Renamed Data -> data successfully")
    except Exception as e:
        print(f"Error renaming Data: {e}")
else:
    if DATA_DIR_NEW.exists():
         print("data directory already exists (lowercase or just matches path)")
         # confirm actual casing? hard on windows python.
    else:
        print("Data directory not found!")

# 2. Rename Price Targets
PRICE_TARGETS_OLD = DATA_DIR_NEW / "Price Targets"
PRICE_TARGETS_NEW = DATA_DIR_NEW / "price_targets"

if PRICE_TARGETS_OLD.exists():
    try:
        os.rename(PRICE_TARGETS_OLD, PRICE_TARGETS_NEW)
        print("Renamed Price Targets -> price_targets")
    except Exception as e:
        print(f"Error renaming price targets: {e}")
else:
    print(f"Price Targets not found at {PRICE_TARGETS_OLD}")

# 3. Create deploy dir
DEPLOY_DIR = BASE_DIR / "deploy"
DEPLOY_DIR.mkdir(exist_ok=True)
print("Ensured deploy/ exists")

# 4. Move yamls
for filename in ["job_finance_data.yaml", "job_update.yaml"]:
    src = BASE_DIR / filename
    dst = DEPLOY_DIR / filename
    if src.exists():
        try:
            shutil.move(src, dst)
            print(f"Moved {filename} to deploy/")
        except Exception as e:
             print(f"Error moving {filename}: {e}")
    else:
        # Check if already in deploy
        if dst.exists():
             print(f"{filename} already in deploy/")
        else:
             print(f"{filename} not found in root")
