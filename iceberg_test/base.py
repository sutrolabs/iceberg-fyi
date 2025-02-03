from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import logging
import tempfile
import yaml
from pathlib import Path
import subprocess
import os
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestContext:
    def __init__(self):
        # Generate a unique name for this test run
        self.test_run_name = uuid.uuid4().hex[:8]
        self.docker_network_name = f"iceberg-test-{self.test_run_name}"
        self._docker_network_created = False

    def __enter__(self):
        # All components are given a shared docker network because many of them
        # need it and it's easier to model this as a global variable. TODO: we
        # could be smarter and elide this for component topologies that don't
        # use any docker-compose
        self.ensure_network()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup_network()
        return False

    def ensure_network(self):
        """Ensure the test network exists."""
        if not self._docker_network_created:
            # Check if network exists
            result = subprocess.run(
                ["docker", "network", "inspect", self.docker_network_name],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                logger.info(f"Creating network {self.docker_network_name}")
                subprocess.run(
                    [
                        "docker",
                        "network",
                        "create",
                        "--driver",
                        "bridge",
                        self.docker_network_name,
                    ],
                    check=True,
                )
            self._docker_network_created = True

    def cleanup_network(self):
        """Remove the test network."""
        if self._docker_network_created:
            try:
                subprocess.run(
                    ["docker", "network", "rm", self.docker_network_name], check=True
                )
                self._docker_network_created = False
            except subprocess.CalledProcessError as e:
                logger.warning(
                    f"Failed to remove network {self.docker_network_name}: {e}"
                )


class DockerCompose:
    def __init__(self, yaml):
        self.yaml = yaml

    def start(self):
        # Create temporary docker-compose file
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False)
        self._compose_file = Path(tmp.name)

        with tmp:
            tmp.write(self.yaml)

        # Start services and wait for them to be healthy
        self._run_compose_command(
            "-f", str(self._compose_file), "up", "-d", "-V", "--wait"
        )

    def stop(self) -> None:
        if self._compose_file:
            try:
                logger.info(f"Tearing down services")
                self._run_compose_command("-f", str(self._compose_file), "down", "-v")
            finally:
                # Clean up temporary file
                self._compose_file.unlink()
                self._compose_file = None

    def _run_compose_command(self, *args: str) -> None:
        """Run a docker-compose command"""
        cmd = ["docker-compose", "-p", "iceberg_test", *args]
        logger.debug(f"Running command: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True, env=os.environ)
        except subprocess.CalledProcessError as e:
            logger.error(f"Docker Compose command failed: {e}")
            raise


class IcebergComponent(ABC):
    """Base class for all Iceberg test components."""

    def __init__(self, test_context: TestContext):
        self.test_context = test_context

    def __enter__(self):
        logger.info(f"{self.name} setup()")
        self.setup()
        logger.info(f"{self.name} setup() done")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.info(f"{self.name} teardown()")
        self.teardown()
        logger.info(f"{self.name} teardown() done")
        return False  # Return True to suppress the exception

    @abstractmethod
    def setup(self) -> None:
        """Start the component and wait for it to be ready"""
        pass

    @abstractmethod
    def teardown(self) -> None:
        """Clean up resources"""
        pass


class Storage(IcebergComponent):
    """Base class for storage implementations."""

    def __init__(self, test_context: TestContext):
        super().__init__(test_context)


class Catalog(IcebergComponent):
    """Base class for catalog implementations."""

    def __init__(self, test_context: TestContext, storage: Storage):
        super().__init__(test_context)
        self.storage = storage

    @property
    @abstractmethod
    def catalog_name(self) -> str:
        pass

    @property
    @abstractmethod
    def catalog_properties(self) -> Dict[str, Any]:
        """PyIceberg formatted catalog properties"""
        pass


class QueryEngine(IcebergComponent):
    """Base class for query engine implementations."""

    def __init__(self, test_context: TestContext, storage: Storage, catalog: Catalog):
        super().__init__(test_context)
        self.storage = storage
        self.catalog = catalog

    @abstractmethod
    def link_table(self, test_table: str) -> None:
        pass

    @abstractmethod
    def unlink_table(self, test_table: str) -> None:
        pass

    @abstractmethod
    def execute_query(self, query: str) -> List[List[Any]]:
        pass

    @abstractmethod
    def create_table(self, test_table: str) -> None:
        pass
