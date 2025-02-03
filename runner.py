#!/usr/bin/env python3
import click
import importlib
import pkgutil
import inspect
from typing import Any, Type, Dict, List
from pathlib import Path
import sys
import yaml
from datetime import datetime

# Add the package to the Python path
sys.path.append(str(Path(__file__).parent))

import env_vars
from iceberg_test.base import (
    Storage,
    Catalog,
    QueryEngine,
    IcebergComponent,
    TestContext,
)
from iceberg_test.test_suite.sql_tests import SQLTestSuite


class ComponentType:
    """Wrapper for component types with their implementations."""

    def __init__(
        self, base_class: Type[IcebergComponent], package_path: str, display_name: str
    ):
        self.base_class = base_class
        self.package_path = package_path
        self.display_name = display_name
        self._implementations = None

    @property
    def implementations(self) -> Dict[str, Type[IcebergComponent]]:
        """Lazy load implementations only when needed."""
        if self._implementations is None:
            self._implementations = self._discover_implementations()
        return self._implementations

    def _discover_implementations(self) -> Dict[str, Type[IcebergComponent]]:
        """Dynamically discover all implementations of a component type."""
        implementations = {}

        try:
            package = importlib.import_module(f"iceberg_test.{self.package_path}")
            package_dir = Path(package.__file__).parent

            for _, module_name, _ in pkgutil.iter_modules([str(package_dir)]):
                if module_name == "__init__":
                    continue

                try:
                    full_module_name = f"iceberg_test.{self.package_path}.{module_name}"
                    module = importlib.import_module(full_module_name)

                    for name, obj in inspect.getmembers(module, inspect.isclass):
                        if issubclass(obj, self.base_class) and obj != self.base_class:
                            if hasattr(obj, "name") and hasattr(obj, "description"):
                                implementations[obj.name] = obj

                except ImportError as e:
                    click.secho(
                        f"Warning: Failed to import {module_name}: {e}",
                        fg="yellow",
                        err=True,
                    )

        except ImportError as e:
            click.secho(
                f"Error: Failed to import package {self.package_path}. "
                f"Error: {str(e)}\n"
                "Check that the package is installed correctly.",
                fg="red",
                err=True,
            )

        return implementations

    def get_click_choices(self) -> List[str]:
        """Get list of implementation names for Click choices."""
        return sorted(self.implementations.keys())

    def get_implementation(self, name: str) -> Type[IcebergComponent]:
        """Get the implementation class for a given name."""
        if not self.implementations:
            raise click.UsageError(
                f"No {self.display_name} implementations found. "
                f"Check that {self.package_path} contains valid implementations."
            )
        if name not in self.implementations:
            available = ", ".join(self.get_click_choices()) or "none found"
            raise click.UsageError(
                f"Invalid {self.display_name} '{name}'. "
                f"Available options: {available}"
            )
        return self.implementations[name]

    def get_descriptions(self) -> str:
        """Get formatted description of all implementations."""
        if not self.implementations:
            return f"  No {self.display_name} implementations found"
        return "\n".join(
            f"  {name}: {impl.description}"
            for name, impl in sorted(self.implementations.items())
        )


def record_results(
    query_engine: str,
    catalog: str,
    storage: str,
    success: bool,
    results: List[Dict[str, Any]],
):
    successful_count = sum(1 for result in results if result.get("status") == "success")
    status = "failed"
    if successful_count == len(results):
        status = "success"
    elif successful_count > 0:
        status = "partial"

    new_result = {
        "query_engine": query_engine,
        "catalog": catalog,
        "storage": storage,
        "storage_interface": "unknown",
        "catalog_interface": "unknown",
        "results": {
            "as_of": datetime.now().strftime("%Y-%m-%d"),
            "status": status,
            "tests": results,
        },
    }

    with open("database/results.yml", "r") as file:
        results = yaml.safe_load(file)

    results["results"].insert(0, new_result)  # Insert at the front

    with open("database/results.yml", "w") as file:
        yaml.dump(results, file)


# Define component types
STORAGE = ComponentType(Storage, "storage", "storage backend")
CATALOG = ComponentType(Catalog, "catalog", "catalog service")
QUERY_ENGINE = ComponentType(QueryEngine, "query_engine", "query engine")


@click.group()
def cli():
    """Iceberg REST tester"""
    env_vars.setup()


@cli.command(name="list-components")
def list_components():
    """List all available components."""
    click.echo("Available components:\n")

    click.secho("Storage backends:", bold=True)
    click.echo(STORAGE.get_descriptions())

    click.secho("\nCatalogs:", bold=True)
    click.echo(CATALOG.get_descriptions())

    click.secho("\nQuery engines:", bold=True)
    click.echo(QUERY_ENGINE.get_descriptions())


@cli.command(name="start")
@click.option(
    "--storage",
    required=True,
    type=click.Choice(STORAGE.get_click_choices()),
    help="Storage backend to test",
)
@click.option(
    "--catalog",
    required=False,
    type=click.Choice(CATALOG.get_click_choices()),
    help="Catalog service to test",
)
@click.option(
    "--query-engine",
    required=False,
    type=click.Choice(QUERY_ENGINE.get_click_choices()),
    help="Query engine to test",
)
@click.option(
    "--wait/--no-wait",
    default=False,
    help="If set, component will be kept alive for manual testing",
)
@click.option(
    "--break/--no-break",
    "break_",
    default=False,
    help="If set, a local breakpoint will be set",
)
def start(storage, catalog, query_engine, wait, break_):
    """Set up and tear down one or more components. Does not run tests, but does
    allow partial configuration (storage or storage+catalog)"""

    click.echo("Component boot test...")
    storage_class = STORAGE.get_implementation(storage) if storage is not None else None
    catalog_class = CATALOG.get_implementation(catalog) if catalog is not None else None
    query_engine_class = (
        QUERY_ENGINE.get_implementation(query_engine)
        if query_engine is not None
        else None
    )

    # Validate configuration
    if query_engine_class is not None:
        if storage_class is None or catalog is None:
            click.secho(
                f"Error: --query-engine requires --storage and --catalog",
                fg="red",
                err=True,
            )
            sys.exit(1)

    if catalog is not None:
        if storage_class is None:
            click.secho(f"Error: --catalog requires --storage", fg="red", err=True)
            sys.exit(1)

    with TestContext() as test_context:
        with storage_class(test_context) as storage_impl:
            if catalog_class is not None:
                with catalog_class(test_context, storage_impl) as catalog_impl:
                    if query_engine_class is not None:
                        with query_engine_class(
                            test_context, storage_impl, catalog_impl
                        ) as query_engine_impl:
                            if wait:
                                click.echo(
                                    "Stack ready for manual testing (--wait), press enter to tear it down"
                                )
                                input()
                            elif break_:
                                breakpoint()
                    else:
                        if wait:
                            click.echo(
                                "Stack ready for manual testing (--wait), press enter to tear it down"
                            )
                            input()
                        elif break_:
                            breakpoint()

            else:
                if wait:
                    click.echo(
                        "Stack ready for manual testing (--wait), press enter to tear it down"
                    )
                    input()
                elif break_:
                    breakpoint()


@cli.command(name="test")
@click.option(
    "--storage",
    required=True,
    type=click.Choice(STORAGE.get_click_choices()),
    help="Storage backend to test",
)
@click.option(
    "--catalog",
    required=True,
    type=click.Choice(CATALOG.get_click_choices()),
    help="Catalog service to test",
)
@click.option(
    "--query-engine",
    required=True,
    type=click.Choice(QUERY_ENGINE.get_click_choices()),
    help="Query engine to test",
)
@click.option(
    "--wait/--no-wait",
    default=False,
    help="If set, component will be kept alive for manual testing",
)
@click.option(
    "--record/--no-record",
    default=False,
    help="If set, test result will be recorded in the database",
)
def test(storage, catalog, query_engine, wait, record):
    """Run Iceberg REST stack compatibility tests."""
    click.echo("Starting compatibility test run...")
    storage_class = STORAGE.get_implementation(storage)
    catalog_class = CATALOG.get_implementation(catalog)
    query_engine_class = QUERY_ENGINE.get_implementation(query_engine)
    with TestContext() as test_context:
        with storage_class(test_context) as storage_impl:
            with catalog_class(test_context, storage_impl) as catalog_impl:
                with query_engine_class(
                    test_context, storage_impl, catalog_impl
                ) as query_engine_impl:
                    click.echo("\nRunning SQL test suite...")
                    sql_suite = SQLTestSuite(
                        storage_impl, catalog_impl, query_engine_impl
                    )
                    success, results = sql_suite.run()

                    if record:
                        record_results(query_engine, catalog, storage, success, results)

                    if success:
                        click.secho(
                            "\n✨ All SQL tests passed successfully!",
                            fg="green",
                            bold=True,
                        )
                    else:
                        click.secho("\n❌ SQL tests failed", fg="red", bold=True)

                    if wait:
                        click.echo(
                            "--wait was passed, keeping stack up. Press enter to exit"
                        )
                        input()

                    sys.exit(0 if success else 1)


if __name__ == "__main__":
    cli()
