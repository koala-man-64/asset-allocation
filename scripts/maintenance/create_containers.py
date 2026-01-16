
import os
import sys
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

from scripts.common import config as cfg

def create_containers():
    print("Initializing BlobServiceClient...")
    
    # Logic copied from mdc.get_storage_client usually, but simplified for script
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    
    if conn_str:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    elif account_name:
        account_url = f"https://{account_name}.blob.core.windows.net"
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url, credential=credential)
    else:
        print("Error: Missing Azure credentials (ACCOUNT_NAME or CONNECTION_STRING).")
        return

    # definition of containers to ensure exist
    # We need a SILVER container. 
    # We implicitly have BRONZE (from config), but we should ensure the new SILVER one.
    
    # Note: cfg.AZURE_CONTAINER_SILVER might not be in config.py yet if we run this BEFORE WI-2.
    # So we will define it loosely here or rely on the user having set it in env, 
    # BUT for this script to be useful it should probably just use the string 'asset-allocation-silver' 
    # or similar if not found. 
    
    # Ideally, we update config first, but the plan said WI-1 first. 
    # Let's assume the name 'silver' or 'asset-allocation-silver'.
    # Let's peek at what 'bronze' is usually named.
    
    containers_to_check = ['silver', 'bronze'] 
    
    for container_name in containers_to_check:
        try:
            print(f"Checking container '{container_name}'...")
            container_client = blob_service_client.get_container_client(container_name)
            if not container_client.exists():
                print(f"Container '{container_name}' does not exist. Creating...")
                container_client.create_container()
                print(f"Container '{container_name}' created successfully.")
            else:
                print(f"Container '{container_name}' already exists.")
        except Exception as e:
            print(f"Failed to check/create container '{container_name}': {e}")

if __name__ == "__main__":
    create_containers()
