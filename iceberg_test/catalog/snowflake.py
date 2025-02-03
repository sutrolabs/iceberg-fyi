import json
from time import sleep

import boto3
from ..base import Catalog, logger, DockerCompose
from typing import Dict, Any
from pyiceberg.catalog import load_catalog
import requests
from iceberg_test.storage.s3 import S3Storage
from iceberg_test.storage.minio import MinioStorage
from iceberg_test.storage.azure_storage import AzureADLSStorage
from os import getenv
import requests


class SnowflakeCatalog(Catalog):
    """Snowflake Open Catalog implementation."""

    name = "snowflake"  # Used for CLI discovery
    description = "Snowflake Open Catalog service"

    @property
    def iceberg_uri(self):
        account = getenv("SNOWFLAKE_OPEN_CATALOG_ACCOUNT_NAME")
        return f"https://{account}.us-east-1.snowflakecomputing.com/polaris/api/catalog"

    @property
    def catalog_name(self) -> str:
        return "regression"

    @property
    def oauth2_credential(self) -> str:
        client_id = getenv("SNOWFLAKE_OPEN_CATALOG_CLIENT_ID")
        client_secret = getenv("SNOWFLAKE_OPEN_CATALOG_CLIENT_SECRET")
        return f"{client_id}:{client_secret}"

    @property
    def oauth2_scope(self) -> str:
        return "PRINCIPAL_ROLE:ALL"

    @property
    def warehouse_name(str) -> str:
        return "hack25"

    @property
    def catalog_properties(self) -> Dict[str, Any]:
        return {
            "uri": self.iceberg_uri,
            "credential": self.oauth2_credential,
            "scope": self.oauth2_scope,
            "warehouse": self.warehouse_name,
        }

    def setup(self):
        iam_client = boto3.client("iam")

        if isinstance(self.storage, S3Storage):
            role_policy_statements = []
            role_policy_statements.append(
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:GetObjectVersion",
                        "s3:DeleteObject",
                        "s3:DeleteObjectVersion",
                    ],
                    "Resource": f"arn:aws:s3:::{self.storage.bucket_name}/*",
                },
            )
            role_policy_statements.append(
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                    "Resource": f"arn:aws:s3:::{self.storage.bucket_name}",
                    "Condition": {"StringLike": {"s3:prefix": ["*"]}},
                },
            )
            response = iam_client.create_role(
                RoleName="Hack25S3RoleForSnowflakeCatalog",
                AssumeRolePolicyDocument=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Sid": "",
                                "Effect": "Allow",
                                "Principal": {"Service": "lakeformation.amazonaws.com"},
                                "Action": "sts:AssumeRole",
                                "Condition": {
                                    "StringEquals": {
                                        "sts:ExternalId": "<api_aws_external_id>"
                                    }
                                },
                            }
                        ],
                    }
                ),
            )
            print(f"Created role {response}")
            role_arn = response["Role"]["Arn"]

            response = iam_client.put_role_policy(
                RoleName="Hack25S3RoleForSnowflakeCatalog",
                PolicyName="Hack25S3RoleForSnowflakeCatalog",
                PolicyDocument=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": role_policy_statements,
                    }
                ),
            )
            print(f"Created role policy {response}")
        else:
            role_arn = None

        logger.info(
            f"ACTION REQUIRED: create {self.warehouse_name} catalog for {self.storage.bucket_url} with S3 role ARN {role_arn} + catalog role CATALOG_MANAGE_CONTENT"
        )

        if isinstance(self.storage, S3Storage):
            volume_user_arn = input("USER ARN: ")
            volume_external_id = input("EXTERNAL ID: ")
            role_assume_statements = []
            role_assume_statements.append(
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {"AWS": volume_user_arn},
                    "Action": "sts:AssumeRole",
                    "Condition": {
                        "StringEquals": {"sts:ExternalId": volume_external_id}
                    },
                },
            )
            response = iam_client.update_assume_role_policy(
                RoleName="Hack25S3RoleForSnowflakeCatalog",
                PolicyDocument=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": role_assume_statements,
                    }
                ),
            )
            print(f"Updated role {response}")
            sleep(10)

        catalog = load_catalog(**self.catalog_properties)
        catalog.create_namespace(self.catalog_name)

    def teardown(self):
        iam_client = boto3.client("iam")

        if isinstance(self.storage, S3Storage):
            try:
                response = iam_client.delete_role_policy(
                    RoleName="Hack25S3RoleForSnowflakeCatalog",
                    PolicyName="Hack25S3RoleForSnowflakeCatalog",
                )
                print(f"Deleted role policy {response}")
            except Exception as e:
                print(f"Error: {str(e)}")

            try:
                response = iam_client.delete_role(
                    RoleName="Hack25S3RoleForSnowflakeCatalog"
                )
                print(f"Deleted role {response}")
            except Exception as e:
                print(f"Error: {str(e)}")

        catalog = load_catalog(**self.catalog_properties)
        catalog.drop_namespace(self.catalog_name)
        logger.info(f"ACTION REQUIRED: delete {self.warehouse_name} catalog")
        input()
