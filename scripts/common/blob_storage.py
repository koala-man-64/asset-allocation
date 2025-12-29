import os
import io
import pandas as pd
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class BlobStorageClient:
    def __init__(self, connection_string=None, container_name='market-data'):
        self.connection_string = connection_string or os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = container_name
        
        if not self.connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set.")
            
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
        
        # Ensure container exists
        if not self.container_client.exists():
            try:
                self.container_client.create_container()
                logger.info(f"Created container: {self.container_name}")
            except Exception as e:
                logger.warning(f"Container creation might have raced: {e}")

    def file_exists(self, remote_path: str) -> bool:
        blob_client = self.container_client.get_blob_client(remote_path)
        return blob_client.exists()

    def read_csv(self, remote_path: str) -> pd.DataFrame:
        """
        Reads a CSV from Azure Blob Storage into a Pandas DataFrame.
        Returns None if the file does not exist or is empty.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            if not blob_client.exists():
                logger.debug(f"File not found in blob storage: {remote_path}")
                return None
            
            download_stream = blob_client.download_blob()
            data = download_stream.readall()
            
            if not data:
                return None

            return pd.read_csv(io.BytesIO(data))
        except Exception as e:
            logger.error(f"Error reading {remote_path}: {e}")
            return None

    def write_csv(self, remote_path: str, df: pd.DataFrame, index=False):
        """
        Writes a Pandas DataFrame to a CSV in Azure Blob Storage.
        """
        try:
            output = io.StringIO()
            df.to_csv(output, index=index)
            data = output.getvalue()
            
            blob_client = self.container_client.get_blob_client(remote_path)
            blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Successfully wrote to blob: {remote_path}")
        except Exception as e:
            logger.error(f"Error writing to {remote_path}: {e}")
            raise

    def upload_file(self, local_path: str, remote_path: str):
        """
        Uploads a local file to Azure Blob Storage.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Uploaded {local_path} to {remote_path}")
        except Exception as e:
            logger.error(f"Error uploading {local_path}: {e}")
            raise

    def delete_file(self, remote_path: str):
        """
        Deletes a file from Azure Blob Storage.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            if blob_client.exists():
                blob_client.delete_blob()
                logger.info(f"Deleted blob: {remote_path}")
        except Exception as e:
            logger.error(f"Error deleting {remote_path}: {e}")

    def list_files(self, prefix: str = None) -> list:
        """
        Lists files (blobs) in the container, optionally filtered by prefix.
        """
        try:
            blobs = self.container_client.list_blobs(name_starts_with=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []

    def get_last_modified(self, remote_path: str) -> datetime:
        """
        Returns the last modified datetime (UTC aware) of a blob.
        Returns None if blob doesn't exist.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            if not blob_client.exists():
                return None
            
            props = blob_client.get_blob_properties()
            return props.last_modified
        except Exception as e:
            logger.error(f"Error getting properties for {remote_path}: {e}")
            return None

    def upload_data(self, remote_path: str, data: bytes, overwrite: bool = True):
        """
        Uploads bytes data to a blob.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            blob_client.upload_blob(data, overwrite=overwrite)
            logger.info(f"Uploaded data to {remote_path}")
        except Exception as e:
            logger.error(f"Error uploading data to {remote_path}: {e}")
            raise

    def download_data(self, remote_path: str) -> bytes:
        """
        Downloads bytes data from a blob.
        Returns None if blob doesn't exist.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            if not blob_client.exists():
                return None
            
            return blob_client.download_blob().readall()
        except Exception as e:
            logger.error(f"Error downloading data from {remote_path}: {e}")
            return None

