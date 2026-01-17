import pytest
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to sys.path if not picked up by pythonpath
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# Load environment variables from .env file (if not already done)
from dotenv import load_dotenv
load_dotenv(os.path.join(project_root, '.env'), override=True)

# Mock Environment Variables for Testing (Set fallbacks if missing)
# Note: NASDAQ_API_KEY should be in .env for actual data fetching.
os.environ.setdefault("YAHOO_USERNAME", "test_user")
os.environ.setdefault("YAHOO_PASSWORD", "test_password")
os.environ.setdefault("AZURE_STORAGE_ACCOUNT_NAME", "test_account")
os.environ.setdefault("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")
os.environ.setdefault("HEADLESS_MODE", "True")
os.environ.setdefault("TEST_MODE", "True")

# Container Mocks
containers = [
    "AZURE_CONTAINER_MARKET", "AZURE_CONTAINER_FINANCE", 
    "AZURE_CONTAINER_EARNINGS", "AZURE_CONTAINER_TARGETS", 
    "AZURE_CONTAINER_COMMON",
    "AZURE_CONTAINER_BRONZE",
    "AZURE_CONTAINER_SILVER",
    "AZURE_CONTAINER_GOLD",
]
for container in containers:
    os.environ.setdefault(container, "test-container")

from scripts.common import config as cfg
from scripts.common.blob_storage import BlobStorageClient

@pytest.fixture(scope="session", autouse=True)
def redirect_storage(tmp_path_factory):
    """
    Global autouse fixture to redirect storage calls to a local temp directory.
    This prevents tests from attempting to connect to Azure.
    """
    temp_storage_root = tmp_path_factory.mktemp("local_test_storage")
    
    # Patch delta_core to use local file URIs
    def mock_get_uri(container, path, account_name=None):
        full_path = temp_storage_root / container / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        return str(full_path)

    with patch("scripts.common.delta_core.get_delta_table_uri", side_effect=mock_get_uri), \
         patch("scripts.common.delta_core.get_delta_storage_options", return_value={}), \
         patch("scripts.common.delta_core._ensure_container_exists", return_value=None):
        yield temp_storage_root

@pytest.fixture(scope="session")
def azure_client():
    """
    Provides a Mocked BlobStorageClient for tests if actual Azure config is missing.
    In actual integration tests, this would use a real client.
    """
    mock_client = MagicMock(spec=BlobStorageClient)
    return mock_client

