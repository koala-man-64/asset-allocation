import sys
import os
import pytest
from unittest.mock import MagicMock, patch

# Adjust path to allow imports from project root
# Assuming test file is in tests/schwab_crawler/
# Project root is ../../
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

def test_yahoo_crawler_imports():
    """
    Verifies that yahoo_crawler and its dependencies can be imported 
    without SyntaxError or ImportError.
    Mocks modules that might trigger immediate side effects or require GUI/Browser.
    """
    
    # Mock modules that are external or heavy
    # We want to test the script's strict import logic, not the framework content itself 
    # if those frameworks are not installed in the test env.
    # However, for integrity, we should try to rely on real modules if possible.
    # We'll rely on the real modules but mock 'playwright_lib' if needed, 
    # but let's try real import first.
    
    try:
        import scripts.schwab_crawler.yahoo_crawler as yc
        assert yc is not None
        assert hasattr(yc, 'main')
    except ImportError as e:
        pytest.fail(f"Failed to import yahoo_crawler: {e}")
    except Exception as e:
        pytest.fail(f"Unexpected error during import: {e}")

if __name__ == "__main__":
    # check that we can run it
    test_yahoo_crawler_imports()
