import pytest
import os
import sys

# Add project root to sys.path if not picked up by pythonpath
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock Environment Variables for Testing (Must be set BEFORE importing config)
if "YAHOO_USERNAME" not in os.environ:
    os.environ["YAHOO_USERNAME"] = "test_user"
if "YAHOO_PASSWORD" not in os.environ:
    os.environ["YAHOO_PASSWORD"] = "test_password"
if "AZURE_STORAGE_ACCOUNT_NAME" not in os.environ:
    os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = "test_account"
if "AZURE_STORAGE_CONNECTION_STRING" not in os.environ:
    os.environ["AZURE_STORAGE_CONNECTION_STRING"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net"
if "HEADLESS_MODE" not in os.environ:
    os.environ["HEADLESS_MODE"] = "True"

# Container Mocks
containers = [
    "AZURE_CONTAINER_MARKET", "AZURE_CONTAINER_FINANCE", 
    "AZURE_CONTAINER_EARNINGS", "AZURE_CONTAINER_TARGETS", 
    "AZURE_CONTAINER_COMMON"
]
for container in containers:
    if container not in os.environ:
        os.environ[container] = "test-container"

from scripts.common import config as cfg
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
        client = BlobStorageClient(container_name=cfg.AZURE_CONTAINER_MARKET)
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
