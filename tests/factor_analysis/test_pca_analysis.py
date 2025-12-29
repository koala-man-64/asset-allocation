import pytest
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

def test_pca_analysis_import():
    """Simple test to verify the module can be imported (syntax check + dependencies)."""
    from scripts.factor_analysis import runner as pca_analysis_code
    assert pca_analysis_code is not None
