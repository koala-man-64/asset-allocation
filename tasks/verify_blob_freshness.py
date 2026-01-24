
import sys
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.append(os.getcwd())

from monitoring import system_health
from monitoring.system_health import LayerProbeSpec

def test_blob_recursive_check():
    print("Verifying recursive blob freshness check...")

    # Mock Spec
    mock_spec = LayerProbeSpec(
        name="test-layer",
        description="Test Layer",
        refresh_frequency="Daily",
        container_env="TEST_CONTAINER",
        max_age_seconds=3600,
        marker_blobs=["data/prefix"], # This is now treated as a prefix
        delta_tables=[]
    )

    # Mock Store
    mock_store = MagicMock()
    # Mock return value for get_container_last_modified
    mock_store.get_container_last_modified.return_value = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Mock Config
    with patch.dict(os.environ, {
        "TEST_CONTAINER": "test-container", 
        "SYSTEM_HEALTH_RUN_IN_TEST": "true"
    }):
        with patch('monitoring.system_health.AzureBlobStore', return_value=mock_store):
            with patch('monitoring.system_health._default_layer_specs', return_value=[mock_spec]):
                # Mock Control Plane things to avoid errors
                with patch('monitoring.system_health.collect_container_apps', return_value=[]), \
                     patch('monitoring.system_health.collect_jobs_and_executions', return_value=[]), \
                     patch('monitoring.system_health.collect_resource_health_signals', return_value=[]), \
                     patch('monitoring.system_health.collect_monitor_metrics', return_value=[]), \
                     patch('monitoring.system_health.collect_log_analytics_signals', return_value=[]):
                    
                    
                    now = datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc) # 30 mins old
                    result = system_health.collect_system_health_snapshot(now=now)

                    # Verify call
                    mock_store.get_container_last_modified.assert_called_with(
                        container="test-container", 
                        prefix="data/prefix"
                    )
                    
                    # Verify result structure
                    domain = result['dataLayers'][0]['domains'][0]
                    print(domain)
                    assert domain['name'] == 'data/prefix'
                    assert domain['status'] == 'healthy'
                    assert domain['lastUpdated'] == '2024-01-01T12:00:00+00:00'
                    
                    print("SUCCESS: get_container_last_modified was called correctly!")

if __name__ == "__main__":
    try:
        test_blob_recursive_check()
    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
