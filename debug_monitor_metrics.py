
import sys
import os
from datetime import datetime

# Add project root
sys.path.append(os.path.abspath(os.getcwd()))

from monitoring.monitor_metrics import _status_for_value, MetricThreshold, _worse_status

def test_logic():
    # Setup from test case
    # warn_above=80, error_above=95
    # Value=90
    
    threshold = MetricThreshold(warn_above=80, error_above=95)
    value = 90.0
    
    print(f"Testing Value={value} with Threshold(warn=80, error=95)")
    status = _status_for_value(value, threshold)
    print(f"Result Status: {status}")
    
    if status != "warning":
        print("FAIL: Expected 'warning'")
        sys.exit(1)
        
    print("PASS: Logic correct in isolation.")

if __name__ == "__main__":
    test_logic()
