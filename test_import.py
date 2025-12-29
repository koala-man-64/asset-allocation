
import sys
import os

# Ensure the current directory is in pythonpath
sys.path.append(os.getcwd())

try:
    print("Attempting imports...")
    import asset_allocation
    print(f"Loaded package: {asset_allocation}")
    
    from scripts.common import config
    print(f"Loaded config: {config.BASE_DIR}")
    
    from asset_allocation.core import analysis
    print("Loaded analysis")
    
    from asset_allocation.core import processing
    print("Loaded processing")
    
    from asset_allocation.data import storage
    print("Loaded storage")
    
    from asset_allocation.ui import cli
    print("Loaded UI")
    
    print("SUCCESS: All modules imported.")
except Exception as e:
    print(f"FAILURE: {e}")
    sys.exit(1)
