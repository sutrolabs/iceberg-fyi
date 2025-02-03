from ..base import Catalog, DockerCompose
from typing import Dict, Any
from iceberg_test.storage.s3 import S3Storage
import requests


class PolarisCatalog(Catalog):
    name = "polaris"
    description = "Apache Polaris, developed and sponsored by Snowflake"

    @property
    def catalog_name(self) -> str:
        return "regression" # TODO

    @property
    def iceberg_uri(self) -> str:
        return "http://polaris:8181/api/catalog"

    @property
    def catalog_properties(self) -> Dict[str, Any]:
        return {
            "uri": self.iceberg_uri,
            "security": "OAUTH2",
            "oauth2.credential": "root:s3cr3t",
            "oauth2.scope": "PRINCIPAL_ROLE:ALL"
        }

    def setup(self):
        self.start_service()
        self.create_catalog()

    def start_service(self):
        if not isinstance(self.storage, S3Storage):
            raise NotImplementedError("Only S3 supported at this time")

        # https://github.com/apache/polaris/blob/cc041c919141eee7effe98c44e334d18cf701767/regtests/docker-compose.yml

        # NOTE: there's no official Polaris docker image; you need to build one.
        # See the README
        docker_compose_yaml = f"""
services:
    polaris:
        image: apache/polaris:latest
        ports:
            - "8181:8181"
            - "8182:8182"
        networks:
            - {self.test_context.docker_network_name}

        environment:
            AWS_REGION: us-east-1
            AWS_ACCESS_KEY_ID: {self.storage.aws_access_key_id}
            AWS_SECRET_ACCESS_KEY: {self.storage.aws_secret_access_key}
            POLARIS_BOOTSTRAP_CREDENTIALS: default-realm,root,s3cr3t
            polaris.realm-context.realms: default-realm
            quarkus.log.file.enable: "false"
            quarkus.otel.sdk.disabled: "true"
        healthcheck:
            test: ["CMD", "curl", "http://localhost:8182/q/health"]
            interval: 10s
            timeout: 10s
            retries: 5

networks:
    {self.test_context.docker_network_name}:
        external: true
"""
        self.docker_compose = DockerCompose(docker_compose_yaml)
        self.docker_compose.start()

    def create_catalog(self):
        # Create the catalog
        # based on polaris/getting-started/trino/create-polaris-catalog.sh
        data_token = {
            "grant_type": "client_credentials",
            "client_id": "root",
            "client_secret": "s3cr3t",
            "scope": "PRINCIPAL_ROLE:ALL"
        }
        headers_token = {"Polaris-Realm": "default-realm"}
        response = requests.post("http://localhost:8181/api/catalog/v1/oauth/tokens", data=data_token, headers=headers_token)
        response.raise_for_status()
        token_data = response.json()
        token = token_data.get("access_token")

        if token == "unauthorized_client":
            raise Exception("Error: Failed to retrieve bearer token")

        url_catalog = "http://localhost:8181/api/management/v1/catalogs"
        data_catalog = {
            "catalog": {
                "name": self.catalog_name,
                "type": "INTERNAL",
                "readOnly": False,
                "properties": {
                    "default-base-location": self.storage.bucket_url
                },
                # TODO: get from storage, support non-S3
                "storageConfigInfo": {
                    "storageType": "S3",
                    "roleArn": "arn:aws:iam::190332891562:role/hack-25-iceberg",
                    "externalId": "hack25-external-id",
                    "region": "us-east-1",
                    "allowedLocations": [self.storage.bucket_url]
                }
            }
        }

        headers_catalog = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        response = requests.post(url_catalog, json=data_catalog, headers=headers_catalog)
        response.raise_for_status()

        # Add TABLE_WRITE_DATA privilege
        url_grant = f"http://localhost:8181/api/management/v1/catalogs/{self.catalog_name}/catalog-roles/catalog_admin/grants"
        data_grant = {
            "type": "catalog",
            "privilege": "TABLE_WRITE_DATA"
        }
        response = requests.put(url_grant, json=data_grant, headers=headers_catalog)
        response.raise_for_status()


    def teardown(self):
        self.stop_service()

    def stop_service(self):
        self.docker_compose.stop()
