
import pytest
import os
import sys
import importlib

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

def test_finance_data_scraper_imports():
    """
    Verifies that finance_data_scraper and its dependencies can be imported 
    without errors. This serves as a basic integrity check after refactoring.
    """
    
    # 1. Test importing the module
    try:
        # We use importlib to ensure we're getting fresh imports if multiple tests run
        # but standard import is fine for this scope.
        # Check standard import path
        import scripts.schwab_crawler.finance_data_scraper as yc
        
        # 2. Check critical dependencies that often cause issues
        assert yc.pd is not None
        # assert yc.mdc is not None # mdc is imported as alias
        
    except ImportError as e:
        pytest.fail(f"Failed to import finance_data_scraper: {e}")
    except AttributeError as e:
        pytest.fail(f"finance_data_scraper missing expected attributes: {e}")
    except Exception as e:
        pytest.fail(f"Unexpected error importing finance_data_scraper: {e}")

if __name__ == "__main__":
    test_finance_data_scraper_imports()
    print("Integrity check passed.")
