import pytest
import pandas as pd
from scripts.common import config as cfg

@pytest.mark.integration
def test_01_connection_and_list(azure_client):
    """
    Verify Azure connection and file listing.
    """
    print("\n--- Test 01: Connection & List ---")
    try:
        files = azure_client.list_files()
        print(f"✅ Successfully listed files. Count: {len(files)}")
        assert isinstance(files, list)
    except Exception as e:
        pytest.fail(f"Failed to list files: {e}")

@pytest.mark.integration
def test_02_read_write_blob(azure_client, temp_test_file):
    """
    Verify read/write operations to Azure Blob Storage.
    """
    print("\n--- Test 02: Read/Write Blob ---")
    df = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})
    file_name = temp_test_file
    
    try:
        # Write
        azure_client.write_csv(file_name, df)
        print("✅ Write successful.")
        
        # Existence Check
        exists = azure_client.file_exists(file_name)
        assert exists, "File should exist after write"
        print("✅ Existence check passed.")

        # Read
        df_read = azure_client.read_csv(file_name)
        assert df_read is not None, "Read returned None"
        assert len(df_read) == 2, "Read dataframe has wrong length"
        print("✅ Read successful.")
        
    except Exception as e:
        pytest.fail(f"Read/Write failed: {e}")

@pytest.mark.integration
def test_03_delete_blob(azure_client, temp_test_file):
    """
    Verify file deletion.
    """
    print("\n--- Test 03: Delete Blob ---")
    file_name = temp_test_file
    # Ensure file exists first (re-write or assume carried over if not parallel? 
    # Fixtures are localized so let's write quickly to ensure this test is atomic)
    df = pd.DataFrame({'col1': [1]})
    azure_client.write_csv(file_name, df)
    
    try:
        azure_client.delete_file(file_name)
        exists = azure_client.file_exists(file_name)
        assert not exists, "File should not exist after delete"
        print("✅ Delete successful.")
    except Exception as e:
        pytest.fail(f"Delete failed: {e}")

@pytest.mark.integration
def test_04_config_values():
    """
    Verify critical configuration values.
    """
    print("\n--- Test 04: Config Values ---")
    print(f"Container: {cfg.AZURE_CONTAINER_NAME}")
    print(f"Timeout: {cfg.DATA_FRESHNESS_SECONDS}")
    assert cfg.AZURE_CONTAINER_NAME is not None
    assert cfg.DATA_FRESHNESS_SECONDS > 0
    print("✅ Config values valid.")
