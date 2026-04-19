import dagster as dg
import boto3
import s3fs
import logging
from typing import Optional


class S3Resource(dg.ConfigurableResource):
    """
    Unified S3/MinIO resource for object storage access.
    
    Provides both boto3 client (for uploads, downloads, operations) and 
    s3fs filesystem interface (for file-like operations, CSV reading).
    """
    endpoint_url: str          # S3/MinIO endpoint URL
    aws_access_key_id: str     # Access key for authentication
    aws_secret_access_key: str # Secret key for authentication
    
    def get_client(self) -> boto3.client:
        """
        Get boto3 S3 client for standard S3 operations.
        
        Use for: uploads, downloads, bucket operations, object management.
        
        Returns:
            boto3.client: S3 client instance
        """
        s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name="us-east-1",
        )
        return s3_client

    def get_storage_options(self) -> dict:
        """
        Get storage options dict for pandas/dask/other libraries.
        
        Use with: pd.read_csv(..., storage_options=...), dask, etc.
        
        Returns:
            dict: Storage options with credentials and endpoint
        """
        return {
            "key": self.aws_access_key_id,
            "secret": self.aws_secret_access_key,
            "client_kwargs": {"endpoint_url": self.endpoint_url}
        }

    def get_filesystem(self) -> s3fs.S3FileSystem:
        """
        Get s3fs filesystem interface for file-like operations.
        
        Use for: directory listing, file reading, path operations.
        
        Returns:
            s3fs.S3FileSystem: Filesystem object for file operations
        """
        fs = s3fs.S3FileSystem(
            key=self.aws_access_key_id,
            secret=self.aws_secret_access_key,
            client_kwargs={"endpoint_url": self.endpoint_url}
        )
        return fs

    def get_latest_folder(self, bucket: str, prefix: str) -> str:
        """
        Get the latest folder from S3 bucket/prefix by the newest file's modification date.
        
        Args:
            bucket: S3 bucket name (e.g., '05-curated').
            prefix: S3 key prefix/path to search within (e.g., 'OMOP/vocabularies').
        
        Returns:
            str: Full S3 path to the latest folder (e.g., 'bucket/prefix/2025-01-15').
        
        Raises:
            ValueError: If no subfolders or files are found under the given prefix.
        """
        fs = self.get_filesystem()
        
        full_prefix = f"{bucket}/{prefix}".rstrip("/")
        all_entries = fs.ls(full_prefix, detail=True)

        # Keep only directories
        folders = [entry for entry in all_entries if entry["type"] == "directory"]
        if not folders:
            raise ValueError(f"No subfolders found under {full_prefix}")

        latest_folder: Optional[str] = None
        latest_time = None

        # Check the most recent file inside each folder
        for folder in folders:
            folder_path = folder["name"]
            files = [f for f in fs.ls(folder_path, detail=True) if f["type"] == "file"]

            if not files:
                logging.debug(f"Skipping empty folder: {folder_path}")
                continue

            # Find newest file in this folder
            newest_file = max(files, key=lambda f: f.get("LastModified", 0))
            folder_time = newest_file.get("LastModified", 0)

            if latest_time is None or folder_time > latest_time:
                latest_time = folder_time
                latest_folder = folder_path

        if latest_folder is None:
            raise ValueError(f"No files found in any subfolders under {full_prefix}")

        logging.info(f"Latest folder found: {latest_folder}")
        return latest_folder


# Legacy aliases for backward compatibility (will be deprecated)
S3Client = S3Resource
S3FSClient = S3Resource
