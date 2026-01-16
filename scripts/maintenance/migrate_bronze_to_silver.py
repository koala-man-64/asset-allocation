import sys
import os
import asyncio
from typing import List
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

# Add project root to sys.path
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from scripts.common import config_shared as cfg

async def copy_blob(blob_service_client: BlobServiceClient, source_container: str, dest_container: str, blob_name: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            source_blob = blob_service_client.get_blob_client(source_container, blob_name)
            dest_blob = blob_service_client.get_blob_client(dest_container, blob_name)

            # Check if destination already exists
            if dest_blob.exists():
                print(f"Skipping {blob_name} (already exists in {dest_container})")
                return

            print(f"Copying {blob_name}...")
            # Start copy
            dest_blob.start_copy_from_url(source_blob.url)
            
            # Wait for completion (optional, but good for script to know when done)
            props = dest_blob.get_blob_properties()
            while props.copy.status == 'pending':
                await asyncio.sleep(0.1)
                props = dest_blob.get_blob_properties()
            
            if props.copy.status != 'success':
                 print(f"Copy failed for {blob_name}: {props.copy.status}")
            else:
                 print(f"Successfully copied {blob_name}")

        except Exception as e:
            print(f"Error copying {blob_name}: {e}")

async def main():
    print("Starting Bronze to Silver Migration...")
    
    # Get Connection String
    conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING') or cfg.AZURE_STORAGE_CONNECTION_STRING
    if not conn_str:
        print("Error: AZURE_STORAGE_CONNECTION_STRING not found.")
        return

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    
    source_container = cfg.AZURE_CONTAINER_BRONZE
    dest_container = cfg.AZURE_CONTAINER_SILVER
    
    print(f"Source: {source_container}")
    print(f"Destination: {dest_container}")

    # Ensure containers exist
    source_client = blob_service_client.get_container_client(source_container)
    if not source_client.exists():
        print(f"Source container '{source_container}' does not exist!")
        return
        
    dest_client = blob_service_client.get_container_client(dest_container)
    if not dest_client.exists():
        print(f"Destination container '{dest_container}' does not exist. Creating...")
        dest_client.create_container()

    # List blobs
    print("Listing blobs in source container...")
    blobs = source_client.list_blobs()
    
    tasks = []
    # Limit concurrency
    semaphore = asyncio.Semaphore(50) 
    
    count = 0
    for blob in blobs:
        count += 1
        tasks.append(copy_blob(blob_service_client, source_container, dest_container, blob.name, semaphore))
    
    print(f"Found {count} blobs to migrate.")
    
    if count > 0:
        await asyncio.gather(*tasks)
    
    print("Migration finished.")

if __name__ == "__main__":
    asyncio.run(main())
