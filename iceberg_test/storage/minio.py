import boto3
from ..base import Storage, DockerCompose, logger
from typing import Dict, Any
import subprocess
from os import getenv


class MinioStorage(Storage):
    """MinIO storage implementation."""

    name = "minio"  # Used for CLI discovery
    description = "MinIO S3-compatible object storage"

    # TODO: some of these properties should be pushed down to the base class

    @property
    def s3_endpoint(self):
        return f"https://hack25minio-{self.test_context.test_run_name}.ngrok.io"

    @property
    def bucket_name(self):
        return f"iceberg-test-{self.test_context.test_run_name}"

    @property
    def bucket_url(self):
        """The URL for the storage bucket"""
        return f"s3://{self.bucket_name}"

    @property
    def aws_access_key_id(self):
        return "minioadmin"

    @property
    def aws_secret_access_key(self):
        return "minioseekrit"

    @property
    def catalog_properties(self) -> Dict[str, Any]:
        return {
            "s3.access-key-id": self.aws_access_key_id,
            "s3.secret-access-key": self.aws_secret_access_key,
        }

    def setup(self):
        self.start_service()
        self.create_bucket()

    def teardown(self):
        # No need to delete bucket for minio
        self.stop_service()

    def start_service(self):
        # need to expose port so we can use AWS to to call S3API - maybe run this in docker?
        docker_compose_yaml = f"""
services:
    minio:
        image: minio/minio:RELEASE.2025-02-03T21-03-04Z
        environment:
            - MINIO_ROOT_USER={self.aws_access_key_id}
            - MINIO_ROOT_PASSWORD={self.aws_secret_access_key}
            - MINIO_WEB_PORT=9001
        command: server /data
        healthcheck:
            test: ['CMD', 'mc', 'ready', 'local']
            interval: 5s
            timeout: 5s
            retries: 5
        networks:
            - {self.test_context.docker_network_name}
        ports:
            - "9000:9000"
            - "9001:9001"

    minio_ngrok:
        image: ngrok/ngrok:3.19.0-alpine
        environment:
            NGROK_AUTHTOKEN: "{getenv('NGROK_AUTHTOKEN')}"
        command:
            - "http"
            - "http://host.docker.internal:9000"
            - "--domain"
            - "hack25minio-{self.test_context.test_run_name}.ngrok.io"
        networks:
            - {self.test_context.docker_network_name}
        ports:
          - "4041:4040"

networks:
    {self.test_context.docker_network_name}:
        external: true
"""
        self.docker_compose = DockerCompose(docker_compose_yaml)
        self.docker_compose.start()

    def stop_service(self):
        self.docker_compose.stop()

    def create_bucket(self) -> None:
        s3_client = boto3.client(
            "s3",
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
        s3_client.create_bucket(Bucket=self.bucket_name)
