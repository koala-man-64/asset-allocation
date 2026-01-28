import pytest
import os
from unittest.mock import patch, MagicMock

os.environ.setdefault("DISABLE_DOTENV", "true")
os.environ.setdefault("LOG_FORMAT", "JSON")
os.environ.setdefault("LOG_LEVEL", "INFO")

# Mock Environment Variables for Testing (Set fallbacks if missing)
# Note: NASDAQ_API_KEY should be in .env for actual data fetching.
os.environ.setdefault("YAHOO_USERNAME", "test_user")
os.environ.setdefault("YAHOO_PASSWORD", "test_password")
os.environ.setdefault("AZURE_STORAGE_ACCOUNT_NAME", "test_account")
os.environ.setdefault("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")
os.environ.setdefault("HEADLESS_MODE", "True")
os.environ.setdefault("TEST_MODE", "True")
os.environ.setdefault("SYSTEM_HEALTH_TTL_SECONDS", "10")
os.environ.setdefault("SYSTEM_HEALTH_MAX_AGE_SECONDS", "129600")
os.environ.setdefault("SYSTEM_HEALTH_RANKING_MAX_AGE_SECONDS", "259200")
os.environ.setdefault("SYSTEM_HEALTH_ARM_API_VERSION", "2023-05-01")
os.environ.setdefault("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS", "5.0")
os.environ["SYSTEM_HEALTH_RESOURCE_HEALTH_ENABLED"] = "false"
os.environ.setdefault("SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION", "2022-10-01")
os.environ["SYSTEM_HEALTH_MONITOR_METRICS_ENABLED"] = "false"
os.environ.setdefault("SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION", "2018-01-01")
os.environ.setdefault("SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES", "15")
os.environ.setdefault("SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL", "PT1M")
os.environ.setdefault("SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION", "Average")
os.environ["SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED"] = "false"
os.environ.setdefault("SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS", "5.0")
os.environ.setdefault("SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES", "15")
os.environ.setdefault("SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB", "3")

os.environ.setdefault("BACKTEST_OUTPUT_DIR", "/tmp/backtest_results")
os.environ.setdefault("BACKTEST_DB_PATH", "/tmp/backtest_results/runs.sqlite3")
os.environ.setdefault("BACKTEST_MAX_CONCURRENT", "1")
os.environ.setdefault("BACKTEST_API_KEY_HEADER", "X-API-Key")
os.environ.setdefault("BACKTEST_ALLOW_LOCAL_DATA", "false")
os.environ.setdefault(
    "BACKTEST_ADLS_CONTAINER_ALLOWLIST",
    "bronze,silver,gold,platinum,ranking-data,common,test-container",
)
os.environ.setdefault("BACKTEST_RUN_STORE_MODE", "sqlite")

# Container Mocks
containers = [
    "AZURE_CONTAINER_MARKET", "AZURE_CONTAINER_FINANCE", 
    "AZURE_CONTAINER_EARNINGS", "AZURE_CONTAINER_TARGETS", 
    "AZURE_CONTAINER_COMMON",
    "AZURE_CONTAINER_RANKING",
    "AZURE_CONTAINER_BRONZE",
    "AZURE_CONTAINER_SILVER",
    "AZURE_CONTAINER_GOLD",
]
for container in containers:
    os.environ.setdefault(container, "test-container")
from core.blob_storage import BlobStorageClient

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

    with patch("core.delta_core.get_delta_table_uri", side_effect=mock_get_uri), \
         patch("core.delta_core.get_delta_storage_options", return_value={}), \
         patch("core.delta_core._ensure_container_exists", return_value=None):
        yield temp_storage_root

@pytest.fixture(scope="session")
def azure_client():
    """
    Provides a Mocked BlobStorageClient for tests if actual Azure config is missing.
    In actual integration tests, this would use a real client.
    """
    mock_client = MagicMock(spec=BlobStorageClient)
    return mock_client

