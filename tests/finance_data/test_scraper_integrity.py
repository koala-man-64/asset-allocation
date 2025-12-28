
import pytest
import os
import sys
import importlib

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

def test_scraper_imports():
    """
    Verifies that scripts.finance_data.scraper and its dependencies can be imported 
    without errors.
    """
    
    # 1. Test importing the module
    try:
        import scripts.finance_data.scraper as yc
        
        # 2. Check critical dependencies and new functions
        assert yc.pd is not None
        assert hasattr(yc, 'process_report_cloud'), "Missing process_report_cloud"
        assert hasattr(yc, 'transpose_dataframe'), "Missing transpose_dataframe"
        
    except ImportError as e:
        pytest.fail(f"Failed to import scraper: {e}")
    except AttributeError as e:
        pytest.fail(f"scraper missing expected attributes: {e}")
    except Exception as e:
        pytest.fail(f"Unexpected error importing scraper: {e}")

if __name__ == "__main__":
    test_scraper_imports()
    print("Integrity check passed.")
