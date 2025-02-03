from ..base import Storage, logger
from typing import Dict, Any
import boto3
import os


class S3Storage(Storage):
    """Real, actual S3. This expects AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the env"""

    name = "s3"
    description = "The Simple Storage Service"

    @property
    def s3_endpoint(self):
        return None

    @property
    def bucket_name(self) -> str:
        return f"iceberg-test-{self.test_context.test_run_name}"

    @property
    def bucket_url(self) -> str:
        """The URL for the storage bucket"""
        return f"s3://{self.bucket_name}"

    @property
    def catalog_properties(self) -> Dict[str, Any]:
        return {
            "s3.access-key-id": self.aws_access_key_id,
            "s3.secret-access-key": self.aws_secret_access_key,
        }

    @property
    def aws_access_key_id(self) -> str:
        return os.environ["AWS_ACCESS_KEY_ID"]

    @property
    def aws_secret_access_key(self) -> str:
        return os.environ["AWS_SECRET_ACCESS_KEY"]

    def setup(self):
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=self.bucket_name)

    def teardown(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)

        # Delete all objects
        bucket.objects.all().delete()

        # If versioning is enabled, delete all object versions as well
        bucket.object_versions.all().delete()

        # Delete the bucket
        bucket.delete()
