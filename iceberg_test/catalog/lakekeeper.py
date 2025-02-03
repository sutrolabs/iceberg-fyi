from ..base import Catalog, logger, DockerCompose
from typing import Dict, Any
import requests
import time
import json


class LakekeeperCatalog(Catalog):
    """Lakekeeper catalog implementation."""

    name = "lakekeeper"  # Used for CLI discovery
    description = "Lakekeeper REST catalog service for Apache Iceberg"

    @property
    def catalog_name(self) -> str:
        raise NotImplementedError

    @property
    def catalog_properties(self) -> Dict[str, Any]:
        raise NotImplementedError

    def setup(self):
        self.start_service()

    def teardown(self):
        self.stop_service()

    def start_service(self):
        # Adapted from https://github.com/lakekeeper/lakekeeper/blob/main/examples/minimal/docker-compose.yaml
        # TODO: some minio stuff hard-coded here!
        warehouse_bootstrap = {
            "warehouse-name": "demo",
            "project-id": "00000000-0000-0000-0000-000000000000",
            "storage-profile": {
                "type": "s3",
                "bucket": self.storage.bucket_name,
                "key-prefix": "initial-warehouse",
                "assume-role-arn": None,
                "endpoint": "http://minio:9000",  # TODO
                "region": "local-01",  # TODO
                "path-style-access": True,
                "flavor": "minio",  # TODO
                "sts-enabled": True,
            },
            "storage-credential": {  # TODO
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": self.storage.aws_access_key_id,
                "aws-secret-access-key": self.storage.aws_secret_access_key,
            },
        }

        # TODO: inject storage config here
        docker_compose_yaml = f"""
services:
    server:
        image: quay.io/lakekeeper/catalog:latest-main
        environment:
            - LAKEKEEPER__BASE_URI=http://server:8181
            - LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!
            - LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@db:5432/postgres
            - LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@db:5432/postgres
            - LAKEKEEPER__UI__LAKEKEEPER_URL=http://localhost:8181
            - RUST_LOG=trace,axum=trace,sqlx=trace,iceberg-catalog=trace
        command: [ "serve" ]
        healthcheck:
            test: [ "CMD", "/home/nonroot/iceberg-catalog", "healthcheck" ]
            interval: 1s
            timeout: 10s
            retries: 3
            start_period: 3s
        depends_on:
            migrate:
                condition: service_completed_successfully
            db:
                condition: service_healthy
        networks:
            - {self.test_context.docker_network_name}


    migrate:
        image: quay.io/lakekeeper/catalog:latest-main
        environment:
            - LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!
            - LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@db:5432/postgres
            - LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@db:5432/postgres
            - RUST_LOG=info
        restart: "no"
        command: [ "migrate" ]
        depends_on:
            db:
                condition: service_healthy
        networks:
            - {self.test_context.docker_network_name}

    bootstrap:
        image: curlimages/curl
        depends_on:
            server:
                condition: service_healthy
        restart: "no"
        command:
            - "-X"
            - "POST"
            - "-v"
            - "http://server:8181/management/v1/bootstrap"
            - "-H"
            - "Content-Type: application/json"
            - "--data"
            - '{{"accept-terms-of-use": true}}'
            - "-o"
            - "/dev/null"
        networks:
            - {self.test_context.docker_network_name}

    initialwarehouse:
        image: curlimages/curl
        depends_on:
            server:
                condition: service_healthy
            bootstrap:
                condition: service_completed_successfully
        restart: "no"
        command:
            - "-X"
            - "POST"
            - "-v"
            - "http://server:8181/management/v1/warehouse"
            - "-H"
            - "Content-Type: application/json"
            - "--data"
            - '{json.dumps(warehouse_bootstrap)}'
            - "-o"
            - "/dev/null"
        networks:
            - {self.test_context.docker_network_name}

    db:
        image: bitnami/postgresql:16.3.0
        environment:
            - POSTGRESQL_USERNAME=postgres
            - POSTGRESQL_PASSWORD=postgres
            - POSTGRESQL_DATABASE=postgres
        healthcheck:
            test: [ "CMD-SHELL", "pg_isready -U postgres -p 5432 -d postgres" ]
            interval: 2s
            timeout: 10s
            retries: 2
            start_period: 10s
        networks:
            - {self.test_context.docker_network_name}


networks:
    {self.test_context.docker_network_name}:
        external: true

"""
        self.docker_compose = DockerCompose(docker_compose_yaml)
        self.docker_compose.start()

    def stop_service(self):
        self.docker_compose.stop()
