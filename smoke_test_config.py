
import sys
import os

# Add project root to sys.path
sys.path.append(os.getcwd())

print("Testing imports...")

try:
    from scripts.market_data import config as market_cfg
    print("Market Data Config: OK")
except Exception as e:
    print(f"Market Data Config: FAIL - {e}")

try:
    from scripts.finance_data import config as finance_cfg
    print("Finance Data Config: OK")
except Exception as e:
    print(f"Finance Data Config: FAIL - {e}")

try:
    from scripts.earnings_data import config as earnings_cfg
    print("Earnings Data Config: OK")
except Exception as e:
    print(f"Earnings Data Config: FAIL - {e}")

try:
    from scripts.price_target_data import config as target_cfg
    print("Price Target Config: OK")
except Exception as e:
    print(f"Price Target Config: FAIL - {e}")
