
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

print("Verifying imports...")

try:
    from scripts.market_data import bronze_market_data
    from scripts.market_data import silver_market_data
    from scripts.market_data import gold_market_data
    print("Market Data imports: OK")
except Exception as e:
    print(f"Market Data imports FAILED: {e}")

try:
    from scripts.finance_data import bronze_finance_data
    from scripts.finance_data import silver_finance_data
    from scripts.finance_data import gold_finance_data
    print("Finance Data imports: OK")
except Exception as e:
    print(f"Finance Data imports FAILED: {e}")

try:
    from scripts.earnings_data import bronze_earnings_data
    from scripts.earnings_data import silver_earnings_data
    from scripts.earnings_data import gold_earnings_data
    print("Earnings Data imports: OK")
except Exception as e:
    print(f"Earnings Data imports FAILED: {e}")

try:
    from scripts.price_target_data import bronze_price_target_data
    from scripts.price_target_data import silver_price_target_data
    from scripts.price_target_data import gold_price_target_data
    print("Price Target Data imports: OK")
except Exception as e:
    print(f"Price Target Data imports FAILED: {e}")
