import json
from os import getenv
from time import sleep
from typing import Any, Dict, List
import boto3
import snowflake.connector
from snowflake.connector import DictCursor

from iceberg_test.catalog.aws_glue import AWSGlueCatalog
from iceberg_test.storage.s3 import S3Storage
from ..base import Catalog, QueryEngine, Storage, TestContext


class SnowflakeQueryEngine(QueryEngine):
    """Snowflake query engine implementation."""

    name = "snowflake"  # Used for CLI discovery
    description = "Snowflake SQL query engine"

    def __init__(self, test_context: TestContext, storage: Storage, catalog: Catalog):
        super().__init__(test_context, storage, catalog)
        self.sts_client = boto3.client("sts")
        self.iam_client = boto3.client("iam")
        self.ctx = snowflake.connector.connect(
            user=getenv("SNOWFLAKE_USER"),
            password=getenv("SNOWFLAKE_PASSWORD"),
            account=getenv("SNOWFLAKE_ACCOUNT"),
        )
        self.account_id = self.sts_client.get_caller_identity()["Account"]

    def setup(self) -> None:
        role_policy_statements = []
        if isinstance(self.catalog, AWSGlueCatalog):
            role_policy_statements.append(
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:GetCatalog",
                        "glue:GetConfig",
                        "glue:GetDatabase",
                        "glue:GetDatabases",
                        "glue:GetTable",
                        "glue:GetTables",
                    ],
                    "Resource": [
                        f"arn:aws:glue:*:{self.account_id}:table/*/*",
                        f"arn:aws:glue:*:{self.account_id}:catalog",
                        f"arn:aws:glue:*:{self.account_id}:database/regression",
                    ],
                }
            )
        if isinstance(self.storage, S3Storage):
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

        if role_policy_statements:
            response = self.iam_client.create_role(
                RoleName="Hack25S3RoleForSnowflake",
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

            response = self.iam_client.put_role_policy(
                RoleName="Hack25S3RoleForSnowflake",
                PolicyName="Hack25S3RoleForSnowflakeGlue",
                PolicyDocument=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": role_policy_statements,
                    }
                ),
            )
            print(f"Created role policy {response}")

        if isinstance(self.catalog, AWSGlueCatalog):
            cur = self.ctx.cursor().execute(
                f"""
                CREATE CATALOG INTEGRATION hack_25_iceberg_rest_catalog_int
                  CATALOG_SOURCE = ICEBERG_REST
                  TABLE_FORMAT = ICEBERG
                  CATALOG_NAMESPACE = '{self.catalog.catalog_name}'
                  REST_CONFIG = (
                    CATALOG_URI = '{self.catalog.iceberg_uri}'
                    CATALOG_API_TYPE = AWS_GLUE
                    WAREHOUSE = '{self.account_id}'
                  )
                  REST_AUTHENTICATION = (
                    TYPE = SIGV4
                    SIGV4_IAM_ROLE = '{role_arn}'
                    SIGV4_SIGNING_REGION = 'us-east-1'
                  )
                  ENABLED = TRUE;
                """
            )
            print(f"Created catalog integration {cur.fetchall()}")
        else:
            cur = self.ctx.cursor().execute(
                f"""
                CREATE CATALOG INTEGRATION hack_25_iceberg_rest_catalog_int
                  CATALOG_SOURCE = ICEBERG_REST
                  TABLE_FORMAT = ICEBERG
                  CATALOG_NAMESPACE = '{self.catalog.catalog_name}'
                  REST_CONFIG = (
                    CATALOG_URI = '{self.catalog.iceberg_uri}'
                    CATALOG_API_TYPE = PUBLIC
                    WAREHOUSE = 'warehouse'
                  )
                  REST_AUTHENTICATION = (
                    TYPE = BEARER
                    BEARER_TOKEN = 'x'
                  )
                  ENABLED = TRUE;
                """
            )
            print(f"Created catalog integration {cur.fetchall()}")

        role_assume_statements = []
        if isinstance(self.catalog, AWSGlueCatalog):
            cur = self.ctx.cursor(DictCursor).execute(
                "DESCRIBE CATALOG INTEGRATION hack_25_iceberg_rest_catalog_int;"
            )
            catalog = {
                item["property"]: item["property_value"] for item in cur.fetchall()
            }
            catalog_user_arn = catalog["API_AWS_IAM_USER_ARN"]
            catalog_external_id = catalog["API_AWS_EXTERNAL_ID"]
            role_assume_statements.append(
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {"AWS": catalog_user_arn},
                    "Action": "sts:AssumeRole",
                    "Condition": {
                        "StringEquals": {"sts:ExternalId": catalog_external_id}
                    },
                }
            )

        if isinstance(self.storage, S3Storage):
            cur = self.ctx.cursor().execute(
                f"""
                CREATE EXTERNAL VOLUME hack_25_s3_iceberg_external_volume
                STORAGE_LOCATIONS =
                   (
                      (
                         NAME = 'default'
                         STORAGE_PROVIDER = 'S3'
                         STORAGE_BASE_URL = '{self.storage.bucket_url}'
                         STORAGE_AWS_ROLE_ARN = '{role_arn}'
                      )
                   );
                """
            )
            print(f"Created external volume {cur.fetchall()}")
        else:
            cur = self.ctx.cursor().execute(
                f"""
                CREATE EXTERNAL VOLUME hack_25_s3_iceberg_external_volume
                STORAGE_LOCATIONS =
                   (
                      (
                         NAME = 'default'
                         STORAGE_PROVIDER = 'S3COMPAT'
                         STORAGE_BASE_URL = '{self.storage.bucket_url.replace('s3:', 's3compat:')}'
                         CREDENTIALS = (
                            AWS_KEY_ID = '{self.storage.aws_access_key_id}'
                            AWS_SECRET_KEY = '{self.storage.aws_secret_access_key}'
                         )
                         STORAGE_ENDPOINT = '{self.storage.s3_endpoint.replace('https://', '')}'
                      )
                   );
                """
            )
            print(f"Created external volume {cur.fetchall()}")

        if isinstance(self.storage, S3Storage):
            cur = self.ctx.cursor(DictCursor).execute(
                "DESCRIBE EXTERNAL VOLUME hack_25_s3_iceberg_external_volume;"
            )
            volume = json.loads(
                {item["property"]: item["property_value"] for item in cur.fetchall()}[
                    "STORAGE_LOCATION_1"
                ]
            )
            volume_user_arn = volume["STORAGE_AWS_IAM_USER_ARN"]
            volume_external_id = volume["STORAGE_AWS_EXTERNAL_ID"]
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

        if role_assume_statements:
            response = self.iam_client.update_assume_role_policy(
                RoleName="Hack25S3RoleForSnowflake",
                PolicyDocument=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": role_assume_statements,
                    }
                ),
            )
            print(f"Updated role {response}")
            sleep(10)

        cur = self.ctx.cursor().execute(
            "SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('hack_25_iceberg_rest_catalog_int');"
        )
        print(f"Verified catalog {cur.fetchall()}")

        cur = self.ctx.cursor().execute(
            "SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('hack_25_s3_iceberg_external_volume');"
        )
        print(f"Verified storage {cur.fetchall()}")

    def teardown(self) -> None:
        try:
            cur = self.ctx.cursor().execute(
                "DROP CATALOG INTEGRATION IF EXISTS hack_25_iceberg_rest_catalog_int"
            )
            print(f"Dropped catalog integration {cur.fetchall()}")
        except Exception as e:
            print(f"Error: {str(e)}")

        try:
            cur = self.ctx.cursor().execute(
                "DROP EXTERNAL VOLUME IF EXISTS hack_25_s3_iceberg_external_volume"
            )
            print(f"Dropped external volume {cur.fetchall()}")
        except Exception as e:
            print(f"Error: {str(e)}")

        if isinstance(self.catalog, AWSGlueCatalog) or isinstance(
            self.storage, S3Storage
        ):
            try:
                response = self.iam_client.delete_role_policy(
                    RoleName="Hack25S3RoleForSnowflake",
                    PolicyName="Hack25S3RoleForSnowflakeGlue",
                )
                print(f"Deleted role policy {response}")
            except Exception as e:
                print(f"Error: {str(e)}")

            try:
                response = self.iam_client.delete_role(
                    RoleName="Hack25S3RoleForSnowflake"
                )
                print(f"Deleted role {response}")
            except Exception as e:
                print(f"Error: {str(e)}")

        try:
            self.ctx.close()
        except Exception as e:
            print(f"Error: {str(e)}")

    def link_table(self, test_table: str) -> None:
        cur = self.ctx.cursor().execute(
            f"""
            CREATE ICEBERG TABLE {test_table}
              EXTERNAL_VOLUME = 'hack_25_s3_iceberg_external_volume'
              CATALOG = 'hack_25_iceberg_rest_catalog_int'
              CATALOG_TABLE_NAME = '{test_table.split('.')[-1]}'
              CATALOG_NAMESPACE = '{self.catalog.catalog_name}';
            """
        )
        print(f"Created table {cur.fetchall()}")

    def unlink_table(self, test_table: str) -> None:
        cur = self.ctx.cursor().execute(f"DROP ICEBERG TABLE IF EXISTS {test_table};")
        print(f"Dropped table {cur.fetchall()}")

    def execute_query(self, query: str) -> List[List[Any]]:
        cur = self.ctx.cursor()
        try:
            cur.execute(query)
        except Exception as e:
            print(f"Error: {str(e)}")
            raise e

        try:
            return [list(row) for row in cur.fetchall()]
        except:
            # Some queries (like CREATE) don't return results
            return []

    def create_table(self, test_table: str) -> None:
        raise NotImplementedError
