
import sys
import os

# Add project root to sys.path
sys.path.append(os.getcwd())

output_file = "verification_output.txt"

with open(output_file, "w") as f:
    f.write("Starting Verification...\n")
    
    # Test Market Data
    try:
        from scripts.market_data import config as market_cfg
        f.write("Market Data Config: OK\n")
    except Exception as e:
        f.write(f"Market Data Config: FAIL - {e}\n")

    # Test Finance Data
    try:
        from scripts.finance_data import config as finance_cfg
        f.write("Finance Data Config: OK\n")
    except Exception as e:
        f.write(f"Finance Data Config: FAIL - {e}\n")

    # Test Earnings Data
    try:
        from scripts.earnings_data import config as earnings_cfg
        f.write("Earnings Data Config: OK\n")
    except Exception as e:
        f.write(f"Earnings Data Config: FAIL - {e}\n")

    # Test Price Target Data
    try:
        from scripts.price_target_data import config as target_cfg
        f.write("Price Target Config: OK\n")
    except Exception as e:
        f.write(f"Price Target Config: FAIL - {e}\n")

    f.write("Verification Complete.\n")
