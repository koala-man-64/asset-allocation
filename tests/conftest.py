import pytest
import os
import sys

# Add project root to sys.path if not picked up by pythonpath
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.market_data import config as cfg
from scripts.common.blob_storage import BlobStorageClient

@pytest.fixture(scope="session")
def azure_client():
    """
    Session-scoped fixture to provide an authenticated BlobStorageClient.
    Skips tests if connection string is missing.
    """
    connection_string = cfg.AZURE_STORAGE_CONNECTION_STRING
    if not connection_string:
        pytest.skip("AZURE_STORAGE_CONNECTION_STRING not set in environment.")

    try:
        # Use the configured container name
        client = BlobStorageClient(container_name=cfg.AZURE_CONTAINER_NAME)
        return client
    except Exception as e:
        pytest.fail(f"Failed to initialize BlobStorageClient: {e}")

@pytest.fixture(scope="function")
def temp_test_file(azure_client):
    """
    Fixture to provide a temporary file name and ensure cleanup after test.
    """
    file_name = "pytest_temp_artifact.csv"
    yield file_name
    # Cleanup
    try:
        azure_client.delete_file(file_name)
    except:
        pass
