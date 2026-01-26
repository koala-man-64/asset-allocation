import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Add project root to path so we can import modules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from monitoring import system_health


def test_blob_recursive_check():
    # Mock ContainerClient and BlobProperties
    mock_blob = MagicMock()
    mock_blob.last_modified = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    
    mock_container_client = MagicMock()
    mock_container_client.list_blobs.return_value = [mock_blob]

    mock_service_client = MagicMock()
    mock_service_client.get_container_client.return_value = mock_container_client

    mock_store = MagicMock()
    mock_store.get_container_last_modified.return_value = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    mock_spec = MagicMock()
    mock_spec.layer = "silver"
    mock_spec.domain = "market"
    mock_spec.container_name = "test-container"
    mock_spec.blob_prefix = "data/prefix"
    mock_spec.freshness_threshold = 3600  # 1 hour
    mock_spec.check_recursive = True
    # The important part of verify_blob_freshness logic was ensuring system_health logic works?
    # Actually, the original file tested `system_health.collect_system_health_snapshot`
    
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
                    assert domain['name'] == 'data/prefix'
                    assert domain['status'] == 'healthy'
                    assert domain['lastUpdated'] == '2024-01-01T12:00:00+00:00'
