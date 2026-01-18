import os
import sys
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from deltalake import DeltaTable

account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "assetallocstorage001")
account_url = f"https://{account_name}.blob.core.windows.net"
credential = DefaultAzureCredential()

print(f"Using account: {account_name}")

try:
    client = BlobServiceClient(account_url, credential=credential)
    container = client.get_container_client("silver")
    print("Checking container 'silver'...")
    if not container.exists():
        print("Container 'silver' DOES NOT EXIST.")
        sys.exit(1)
    print("Container 'silver' exists.")
    
    # List blobs for MSFT
    path = "market-data/MSFT"
    print(f"Listing blobs prefix: {path}")
    blobs = list(container.list_blobs(name_starts_with=path))
    print(f"Found {len(blobs)} blobs under '{path}'.")
    if len(blobs) == 0:
        print("No blobs found! Backtest expects data here.")
    elif len(blobs) < 10:
        for b in blobs:
            print(f" - {b.name}")
            
    if len(blobs) > 0:
        # Try DeltaTable
        uri = f"abfss://silver@{account_name}.dfs.core.windows.net/{path}"
        print(f"Attempting to read DeltaTable at: {uri}")
        # explicit options to force Azure CLI auth which usually works locally
        dt = DeltaTable(uri, storage_options={"account_name": account_name, "use_azure_cli": "true"}) 
        print(f"DeltaTable Version: {dt.version()}")
        print("DeltaTable read SUCCEEDED.")
    
except Exception as e:
    print(f"Error: {e}")
