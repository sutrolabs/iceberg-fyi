from typing import Any, Dict
import boto3
from iceberg_test.base import Catalog, Storage, TestContext
from pyiceberg.catalog import load_catalog
import pyarrow as pa


class AWSGlueCatalog(Catalog):
    name = "aws_glue"
    description = "AWS Glue"

    @property
    def iceberg_uri(self):
        return "https://glue.us-east-1.amazonaws.com/iceberg"

    @property
    def catalog_name(self):
        return "regression"

    @property
    def catalog_properties(self) -> Dict[str, Any]:
        return {
            "uri": self.iceberg_uri,
            "rest.sigv4-enabled": "true",
            "rest.signing-region": "us-east-1",
            "rest.signing-name": "glue",
        }

    def setup(self):
        glue_client = boto3.client("glue")

        response = glue_client.create_database(
            DatabaseInput={
                "Name": self.catalog_name,
                "LocationUri": self.storage.bucket_url,
            },
        )
        print(f"Created database {response}")

    def teardown(self):
        glue_client = boto3.client("glue")

        try:
            response = glue_client.delete_database(Name=self.catalog_name)
            print(f"Deleted database {response}")
        except Exception as e:
            print(f"Error: {str(e)}")
