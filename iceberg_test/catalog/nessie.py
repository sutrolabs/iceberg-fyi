from ..base import Catalog, logger, DockerCompose
from typing import Dict, Any
from pyiceberg.catalog import load_catalog
import requests
from iceberg_test.storage.s3 import S3Storage
from iceberg_test.storage.minio import MinioStorage
from iceberg_test.storage.azure_storage import AzureADLSStorage
from iceberg_test.storage.cloudflare_r2 import CloudflareR2
from os import getenv


class NessieCatalog(Catalog):
    """Nessie catalog implementation."""

    name = "nessie"  # Used for CLI discovery
    description = "Nessie versioned catalog service"

    @property
    def iceberg_uri(self):
        return (
            f"https://hack25nessie-{self.test_context.test_run_name}.ngrok.io/iceberg"
        )

    @property
    def catalog_name(self) -> str:
        return "regression"

    @property
    def catalog_properties(self) -> Dict[str, Any]:
        return {
            "uri": self.iceberg_uri,
        }

    def setup(self):
        self.start_service()
        catalog = load_catalog(**self.catalog_properties)
        catalog.create_namespace(self.catalog_name)

    def teardown(self):
        self.stop_service()

    def start_service(self):
        nessie_catalog_warehouse_config = None

        if isinstance(self.storage, S3Storage) or isinstance(
            self.storage, MinioStorage
        ) or isinstance(self.storage, CloudflareR2):
            nessie_catalog_warehouse_config = f"""
             NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION: {self.storage.bucket_url}
             NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT: {self.storage.s3_endpoint or ""}
             NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_PATH_STYLE_ACCESS: true
             NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_REGION: us-east-1
             AWS_ACCESS_KEY_ID: {self.storage.aws_access_key_id}
             AWS_SECRET_ACCESS_KEY: {self.storage.aws_secret_access_key}
             NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_AUTH_TYPE: APPLICATION_GLOBAL"""
        elif isinstance(self.storage, AzureADLSStorage):
            nessie_catalog_warehouse_config = f"""
             AZURE_TENANT_ID: '{self.storage.azure_tenant_id}'
             AZURE_CLIENT_ID: '{self.storage.azure_client_id}'
             AZURE_CLIENT_SECRET: '{self.storage.azure_client_secret}'
             NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION: '{self.storage.abfs_url}'
             NESSIE_CATALOG_SERVICE_ADLS_DEFAULT_OPTIONS_AUTH_TYPE: APPLICATION_DEFAULT
             NESSIE_CATALOG_SERVICE_ADLS_DEFAULT_OPTIONS_ENDPOINT: 'https://{self.storage.azure_account_name}.dfs.core.windows.net'"""
        else:
            raise NotImplementedError(f"Unsupported storage {self.storage}")

        docker_compose_yaml = f"""
services:
    nessie_postgres:
        image: postgres:15.10
        environment:
            POSTGRES_DB: nessie
            POSTGRES_USER: nessie
            POSTGRES_PASSWORD: password123
        healthcheck:
            test: ['CMD', 'pg_isready', '-U', 'nessie']
            interval: 5s
            timeout: 5s
            retries: 5
        networks:
            - {self.test_context.docker_network_name}

    nessie:
        image: ghcr.io/projectnessie/nessie:0.102.5
        depends_on:
            nessie_postgres:
                condition: service_healthy
        environment:
             NESSIE_VERSION_STORE_TYPE: JDBC
             NESSIE_VERSION_STORE_PERSIST_JDBC_DATASOURCE: postgresql
             NESSIE_CATALOG_DEFAULT_WAREHOUSE: warehouse
             {nessie_catalog_warehouse_config}
             QUARKUS_DATASOURCE_POSTGRESQL_DB_KIND: postgresql
             QUARKUS_DATASOURCE_POSTGRESQL_USERNAME: nessie
             QUARKUS_DATASOURCE_POSTGRESQL_PASSWORD: password123
             QUARKUS_DATASOURCE_POSTGRESQL_JDBC_URL: jdbc:postgresql://nessie_postgres:5432/nessie
             QUARKUS_HTTP_PORT: '19120'
        healthcheck:
            test: ['CMD', 'curl', '-f', 'http://localhost:19120/api/v1/config']
            interval: 10s
            timeout: 5s
            retries: 5
        networks:
            - {self.test_context.docker_network_name}
        ports:
            - "19120:19120"
            - "19121:9000"

    nessie_ngrok:
        image: ngrok/ngrok:3.19.0-alpine
        environment:
            NGROK_AUTHTOKEN: "{getenv('NGROK_AUTHTOKEN')}"
        command:
            - "http"
            - "http://host.docker.internal:19120"
            - "--domain"
            - "hack25nessie-{self.test_context.test_run_name}.ngrok.io"
        networks:
            - {self.test_context.docker_network_name}
        ports:
          - "4042:4040"

networks:
    {self.test_context.docker_network_name}:
        external: true
"""
        self.docker_compose = DockerCompose(docker_compose_yaml)
        self.docker_compose.start()

        # Run a health check on the Quarkus management port (which we've exposed as 19121)
        health_check_data = requests.get("http://localhost:19121/q/health").json()
        if health_check_data["status"] != "UP":
            raise Exception(f"Nessie failed deep health check: {health_check_data}")

    def stop_service(self):
        self.docker_compose.stop()
