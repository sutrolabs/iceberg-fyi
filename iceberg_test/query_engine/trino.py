from pathlib import Path
from typing import Dict, Any, List
import tempfile
import shutil
import trino
from os import getenv

from iceberg_test.catalog.aws_glue import AWSGlueCatalog
from iceberg_test.catalog.snowflake import SnowflakeCatalog
from iceberg_test.catalog.polaris import PolarisCatalog
from ..base import QueryEngine, Storage, Catalog, logger, DockerCompose
from iceberg_test.storage.s3 import S3Storage
from iceberg_test.storage.minio import MinioStorage
from iceberg_test.storage.azure_storage import AzureADLSStorage
from iceberg_test.storage.cloudflare_r2 import CloudflareR2


class TrinoQueryEngine(QueryEngine):
    """Trino query engine implementation."""

    name = "trino"  # Used for CLI discovery
    description = "Trino distributed SQL query engine"

    def setup(self):
        self.start_service()

    def teardown(self):
        self.stop_service()

    def start_service(self):
        # Create temporary config directory
        self.config_dir = Path(tempfile.mkdtemp())
        trino_config = self.config_dir / "config" / "trino"
        trino_config.mkdir(parents=True)

        # Write Trino configuration files
        self._write_config_files(trino_config)

        docker_compose_yaml = f"""
services:
    trino:
        image: trinodb/trino:469
        volumes: [ '{str(self.config_dir)}/config/trino:/etc/trino' ]
        healthcheck:
            test: ['CMD', 'curl', '-f', 'http://localhost:8080/v1/status']
            interval: 10s
            timeout: 5s
            retries: 5
        networks:
            - {self.test_context.docker_network_name}
        ports:
            - "8081:8080"

networks:
    {self.test_context.docker_network_name}:
        external: true
"""
        self.docker_compose = DockerCompose(docker_compose_yaml)
        self.docker_compose.start()
        self._create_catalog()

    def stop_service(self):
        if self.config_dir:
            shutil.rmtree(self.config_dir)
        self.docker_compose.stop()

    def _write_config_files(self, config_dir: Path) -> None:
        """Write all required Trino configuration files."""
        # Create catalog directory
        catalog_dir = config_dir / "catalog"
        catalog_dir.mkdir()

        # Write node.properties
        node_properties = """
node.environment=docker
node.id=local
node.data-dir=/data/trino
""".strip()

        (config_dir / "node.properties").write_text(node_properties)

        # Write config.properties
        config_properties = """
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
web-ui.preview.enabled=true
catalog.management=dynamic
catalog.store=memory
""".strip()

        (config_dir / "config.properties").write_text(config_properties)

        # Write jvm.properties
        jvm_properties = """
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-XX:+UseGCOverheadLimit
""".strip()

        (config_dir / "jvm.config").write_text(jvm_properties)

    def _create_catalog(self):
        # TODO: change nessie to https://trino.io/docs/current/object-storage/metastores.html#iceberg-rest-catalog

        """Create the Iceberg catalog using dynamic SQL after components are ready."""

        if isinstance(self.catalog, AWSGlueCatalog):
            self._create_catalog_glue()
        elif isinstance(self.catalog, SnowflakeCatalog):
            self._create_catalog_snowflake()
        elif (
            isinstance(self.storage, S3Storage)
            or isinstance(self.storage, MinioStorage)
            or isinstance(self.storage, CloudflareR2)
        ):
            self._create_catalog_s3()
        elif isinstance(self.storage, AzureADLSStorage):
            self._create_catalog_azure()
        else:
            raise NotImplementedError(f"Unsupported storage {self.storage}")

    def _create_catalog_s3(self):
        # TODO: get all of this from catalog / storage metadata
        #
        # Storage
        # * bucket uri
        # * endpoint uri
        # * auth
        #
        # Catalog
        # * REST URL
        # * REST Auth

        s3_endpoint_config = (
            ""
            if self.storage.s3_endpoint is None
            else f"\"s3.endpoint\" = '{self.storage.s3_endpoint}',"
        )

        # TODO: make trino options more dynamic:
        # * need to handle polaris oauth
        # * need to handle warehouse name differs from bucket name
        create_catalog_sql = None
        if isinstance(self.catalog, PolarisCatalog):
            create_catalog_sql = f"""
            create catalog iceberg_test using iceberg with (
                "iceberg.catalog.type" = 'rest',
                "iceberg.rest-catalog.uri" = '{self.catalog.iceberg_uri}',
                "iceberg.rest-catalog.security" = 'OAUTH2',
                "iceberg.rest-catalog.oauth2.credential" = 'root:s3cr3t',
                "iceberg.rest-catalog.oauth2.scope" = 'PRINCIPAL_ROLE:ALL',
                "iceberg.rest-catalog.warehouse" = '{self.catalog.catalog_name}',
                "iceberg.file-format" = 'parquet',
                "fs.native-s3.enabled" = 'true',
                {s3_endpoint_config}
                "s3.region" = 'us-east-1',
                "s3.aws-access-key" = '{self.storage.aws_access_key_id}',
                "s3.aws-secret-key" = '{self.storage.aws_secret_access_key}',
                "s3.path-style-access" = 'true'
            )
            """

        else:
            create_catalog_sql = f"""
            create catalog iceberg_test using iceberg with (
                "iceberg.catalog.type" = 'rest',
                "iceberg.rest-catalog.uri" = '{self.catalog.iceberg_uri}',
                "iceberg.rest-catalog.warehouse" = '{self.storage.bucket_url}',
                "iceberg.file-format" = 'parquet',
                "fs.native-s3.enabled" = 'true',
                {s3_endpoint_config}
                "s3.region" = 'us-east-1',
                "s3.aws-access-key" = '{self.storage.aws_access_key_id}',
                "s3.aws-secret-key" = '{self.storage.aws_secret_access_key}',
                "s3.path-style-access" = 'true'
            )
            """

        # Execute the catalog creation SQL
        self.execute_query(create_catalog_sql)
        logger.info("Successfully created Iceberg catalog in Trino")

    def _create_catalog_snowflake(self):
        s3_endpoint_config = (
            ""
            if self.storage.s3_endpoint is None
            else f"\"s3.endpoint\" = '{self.storage.s3_endpoint}',"
        )

        create_catalog_sql = f"""
        create catalog iceberg_test using iceberg with (
            "iceberg.catalog.type" = 'rest',
            "iceberg.rest-catalog.uri" = '{self.catalog.iceberg_uri}',
            "iceberg.rest-catalog.warehouse" = '{self.catalog.warehouse_name}',
            "iceberg.rest-catalog.security" = 'OAUTH2',
            "iceberg.rest-catalog.oauth2.credential" = '{self.catalog.oauth2_credential}',
            "iceberg.rest-catalog.oauth2.scope" = '{self.catalog.oauth2_scope}',
            "iceberg.file-format" = 'parquet',
            "fs.native-s3.enabled" = 'true',
            {s3_endpoint_config}
            "s3.region" = 'us-east-1',
            "s3.aws-access-key" = '{self.storage.aws_access_key_id}',
            "s3.aws-secret-key" = '{self.storage.aws_secret_access_key}',
            "s3.path-style-access" = 'true'
        )
        """

        # Execute the catalog creation SQL
        self.execute_query(create_catalog_sql)
        logger.info("Successfully created Iceberg catalog in Trino")

    def _create_catalog_snowflake(self):
        s3_endpoint_config = (
            ""
            if self.storage.s3_endpoint is None
            else f"\"s3.endpoint\" = '{self.storage.s3_endpoint}',"
        )

        create_catalog_sql = f"""
        create catalog iceberg_test using iceberg with (
            "iceberg.catalog.type" = 'rest',
            "iceberg.rest-catalog.uri" = '{self.catalog.iceberg_uri}',
            "iceberg.rest-catalog.warehouse" = '{self.catalog.warehouse_name}',
            "iceberg.rest-catalog.security" = 'OAUTH2',
            "iceberg.rest-catalog.oauth2.credential" = '{self.catalog.oauth2_credential}',
            "iceberg.rest-catalog.oauth2.scope" = '{self.catalog.oauth2_scope}',
            "iceberg.file-format" = 'parquet',
            "fs.native-s3.enabled" = 'true',
            {s3_endpoint_config}
            "s3.region" = 'us-east-1',
            "s3.aws-access-key" = '{self.storage.aws_access_key_id}',
            "s3.aws-secret-key" = '{self.storage.aws_secret_access_key}',
            "s3.path-style-access" = 'true'
        )
        """

        # Execute the catalog creation SQL
        self.execute_query(create_catalog_sql)
        logger.info("Successfully created Iceberg catalog in Trino")

    def _create_catalog_glue(self):
        s3_endpoint_config = (
            ""
            if self.storage.s3_endpoint is None
            else f"\"s3.endpoint\" = '{self.storage.s3_endpoint}',"
        )

        create_catalog_sql = f"""
        create catalog iceberg_test using iceberg with (
            "iceberg.catalog.type" = 'glue',
            "iceberg.file-format" = 'parquet',
            "hive.metastore.glue.region" = 'us-east-1',
            "hive.metastore.glue.endpoint-url" = 'https://glue.us-east-1.amazonaws.com',
            "hive.metastore.glue.aws-access-key" = '{getenv("AWS_ACCESS_KEY_ID")}',
            "hive.metastore.glue.aws-secret-key" = '{getenv("AWS_SECRET_ACCESS_KEY")}',
            "hive.metastore.glue.default-warehouse-dir" = '/',
            "fs.native-s3.enabled" = 'true',
            {s3_endpoint_config}
            "s3.region" = 'us-east-1',
            "s3.aws-access-key" = '{self.storage.aws_access_key_id}',
            "s3.aws-secret-key" = '{self.storage.aws_secret_access_key}',
            "s3.path-style-access" = 'true'
        )
        """

        # Execute the catalog creation SQL
        self.execute_query(create_catalog_sql)
        logger.info("Successfully created Iceberg catalog in Trino")

    def _create_catalog_azure(self):
        create_catalog_sql = f"""
        create catalog iceberg_test using iceberg with (
            "iceberg.catalog.type" = 'rest',
            "iceberg.rest-catalog.uri" = '{self.catalog.iceberg_uri}',
            "fs.native-azure.enabled"  = 'true',
            "azure.auth-type" = 'OAUTH',
            "azure.oauth.tenant-id" = '{self.storage.azure_tenant_id}',
            "azure.oauth.client-id" = '{self.storage.azure_client_id}',
            "azure.oauth.secret" = '{self.storage.azure_client_secret}',
            "azure.oauth.endpoint" = 'https://login.microsoftonline.com/{self.storage.azure_tenant_id}'
        )
        """

        # Execute the catalog creation SQL
        self.execute_query(create_catalog_sql)
        logger.info("Successfully created Iceberg catalog in Trino")

    def execute_query(self, query: str) -> List[List[Any]]:
        """Execute a SQL query against Trino."""
        logger.info(f"Executing query: {query}")

        with trino.dbapi.connect(
            host="localhost",
            port=8081,
            user="admin",
        ) as conn:
            cur = conn.cursor()
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

    def link_table(self, test_table: str) -> None:
        pass

    def unlink_table(self, test_table: str) -> None:
        pass

    # TODO partitioned by
    def create_table(self, test_table: str) -> None:
        """Test creating a table with Iceberg schema."""
        create_table_sql = f"""
        CREATE TABLE {test_table} (
            customer_id BIGINT,
            order_id BIGINT,
            order_date DATE,
            total_amount DECIMAL(10,2),
            status VARCHAR
        )
        """
        # TODO: try to list tables, columns
        self.execute_query(create_table_sql)
