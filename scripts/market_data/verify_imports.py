import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

print("Importing config...")
try:
    from scripts.market_data import config
    print("Config imported.")
except ImportError as e:
    print(f"Config import failed: {e}")


print("Importing ta_lib...")
try:
    from scripts.market_data import ta_lib
    print("ta_lib imported.")
except ImportError as e:
    print(f"ta_lib import failed: {e}")

print("Importing core...")
try:
    from scripts.market_data import core
    print("core imported.")
except ImportError as e:
    print(f"core import failed: {e}")

print("Importing playwright_lib...")
try:
    from scripts.common import playwright_lib
    print("playwright_lib imported.")
except ImportError as e:
    print(f"playwright_lib import failed: {e}")

print("Imports verification complete.")
