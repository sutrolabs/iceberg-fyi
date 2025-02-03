from ..base import Storage, logger
from typing import Dict, Any
import boto3
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

class AzureADLSStorage(Storage):
    name = "azure_adls"
    description = "Is it deprecated or is it experimental?"

    @property
    def container_name(self) -> str:
        return f"iceberg-test-{self.test_context.test_run_name}"

    @property
    def abfs_url(self) -> str:
        # https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri#uri-syntax
        return f"abfs://{self.container_name}@{self.azure_account_name}.dfs.core.windows.net"

    @property
    def bucket_url(self) -> str:
        # For Pyiceberg - TODO verify
        return self.abfs_url

    # Authentication is done with a service principal; this is known to work
    # with Nessie, likely should work with anything that uses the Azure Java SDK
    @property
    def azure_tenant_id(self) -> str:
        return os.environ["AZURE_TENANT_ID"]

    @property
    def azure_client_id(self) -> str:
        return os.environ["AZURE_CLIENT_ID"]

    @property
    def azure_client_secret(self) -> str:
        return os.environ["AZURE_CLIENT_SECRET"]

    @property
    def azure_account_name(self) -> str:
        return os.environ["AZURE_ACCOUNT_NAME"]

    @property
    def catalog_properties(self) -> Dict[str, Any]:
        return {
            "adls.account-name": self.azure_account_name,
            "adls.tenant-id": self.azure_tenant_id,
            "adls.client-id": self.azure_client_id,
            "adls.client-secret": self.azure_client_secret,
        }

    def setup(self):
        azure_client = self._blob_service_client()
        container_client = azure_client.get_container_client(self.container_name)
        container_client.create_container()

    def teardown(self):
        azure_client = self._blob_service_client()
        container_client = azure_client.get_container_client(self.container_name)

        # List and delete all blobs
        blobs = container_client.list_blobs()
        for blob in blobs:
            container_client.delete_blob(blob.name)

        container_client.delete_container()

    def _blob_service_client(self):
        credential = DefaultAzureCredential() # auth using env vars
        account_url = f"https://{self.azure_account_name}.blob.core.windows.net"
        return BlobServiceClient(account_url, credential=credential)
