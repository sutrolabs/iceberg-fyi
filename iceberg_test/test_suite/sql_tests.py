from ..base import Storage, Catalog, QueryEngine, logger
from iceberg_test.base import Catalog, Storage
from pyiceberg.catalog import load_catalog
from datetime import date
import pyarrow as pa


class SQLTestSuite:
    """Basic SQL operation test suite."""

    def __init__(self, storage: Storage, catalog: Catalog, query_engine: QueryEngine):
        self.storage = storage
        self.catalog = catalog
        self.query_engine = query_engine

        self.test_catalog = "iceberg_test"
        self.test_schema = f"{self.test_catalog}.regression"
        self.test_name = "customer_orders"
        self.test_table = f"{self.test_schema}.{self.test_name}"

    def run(self) -> bool:
        """Run all tests in the suite."""
        logger.info("Running SQL test suite...")

        success = True
        tests = [
            "test_create_catalog_table",
            "test_verify_data",
            "test_modify_data",
            "test_verify_modified_data",
            "test_drop_catalog_table",
            "test_advanced_create_table",
            "test_advanced_insert_data",
            "test_advanced_verify_data",
            "test_advanced_modify_data",
            "test_advanced_verify_modified_data",
        ]

        results = []

        for test in tests:
            try:
                getattr(self, test)()
                logger.info(f"✅ {test}")
                results.append({"test": test, "status": "success"})
            except Exception as e:
                logger.error(f"❌ {test}: {str(e)}", exc_info=True)
                results.append({"test": test, "status": "failed"})
                success = False

        logger.info("All tests passed successfully!")
        return success, results

    def test_create_catalog_table(self):
        df = pa.Table.from_pylist(
            [
                {
                    "customer_id": 1,
                    "order_id": 1001,
                    "order_date": date(2024, 1, 1),
                    "total_amount": 100.50,
                    "status": "COMPLETED",
                },
                {
                    "customer_id": 1,
                    "order_id": 1002,
                    "order_date": date(2024, 1, 2),
                    "total_amount": 200.75,
                    "status": "PENDING",
                },
                {
                    "customer_id": 2,
                    "order_id": 1003,
                    "order_date": date(2024, 1, 3),
                    "total_amount": 150.25,
                    "status": "COMPLETED",
                },
                {
                    "customer_id": 2,
                    "order_id": 1004,
                    "order_date": date(2024, 1, 4),
                    "total_amount": 300.00,
                    "status": "PENDING",
                },
            ],
        )
        catalog = load_catalog(
            **self.storage.catalog_properties, **self.catalog.catalog_properties
        )
        table = catalog.create_table(
            f"{self.catalog.catalog_name}.{self.test_name}",
            schema=df.schema,
            location=f"{self.storage.bucket_url}/{self.catalog.catalog_name}/{self.test_name}",
        )
        table.overwrite(df)
        print(f"Created table {table}")

        self.query_engine.link_table(self.test_table)

    def test_drop_catalog_table(self):
        self.query_engine.unlink_table(self.test_table)

        catalog = load_catalog(
            **self.storage.catalog_properties, **self.catalog.catalog_properties
        )
        catalog.drop_table(
            f"{self.catalog.catalog_name}.{self.test_name}",
        )

    # TODO partitioned by
    def test_advanced_create_table(self):
        self.query_engine.create_table(self.test_table)

    def test_advanced_insert_data(self):
        """Test inserting initial dataset."""
        insert_sql = f"""
        INSERT INTO {self.test_table}
        VALUES
            (1, 1001, DATE '2024-01-01', 100.50, 'COMPLETED'),
            (1, 1002, DATE '2024-01-02', 200.75, 'PENDING'),
            (2, 1003, DATE '2024-01-03', 150.25, 'COMPLETED'),
            (2, 1004, DATE '2024-01-04', 300.00, 'PENDING')
        """
        self.query_engine.execute_query(insert_sql)

    def test_verify_data(self):
        """Verify the initial dataset."""
        result = self.query_engine.execute_query(
            f"""
        SELECT status, COUNT(*) as count, SUM(total_amount) as total
        FROM {self.test_table}
        GROUP BY status
        ORDER BY status
        """
        )

        expected = [["COMPLETED", 2, 250.75], ["PENDING", 2, 500.75]]

        assert sorted(expected) == sorted(result)

    def test_advanced_verify_data(self):
        self.test_verify_data()

    def test_modify_data(self):
        """Test modifying existing data."""
        update_sql = f"""
        UPDATE {self.test_table}
        SET status = 'COMPLETED'
        WHERE order_id = 1002
        """
        self.query_engine.execute_query(update_sql)

    def test_advanced_modify_data(self):
        self.test_modify_data()

    def test_verify_modified_data(self):
        """Verify the modified dataset."""
        result = self.query_engine.execute_query(
            f"""
        SELECT status, COUNT(*) as count, SUM(total_amount) as total
        FROM {self.test_table}
        GROUP BY status
        ORDER BY status
        """
        )

        expected = [["COMPLETED", 3, 451.50], ["PENDING", 1, 300.00]]

        assert sorted(expected) == sorted(result)

    def test_advanced_verify_modified_data(self):
        self.test_verify_modified_data()
