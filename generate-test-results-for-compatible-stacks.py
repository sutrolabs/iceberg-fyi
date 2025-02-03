import yaml
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

@dataclass
class Describable:
    key: str
    name: Optional[str] = None
    description: Optional[str] = None
    citations: List[str] = field(default_factory=list)

@dataclass
class Resource(Describable):
    cloud: bool = False

@dataclass
class StorageInterface(Describable):
    pass

@dataclass
class CatalogInterface(Describable):
    pass

@dataclass
class Storage(Resource):
    implements_storage_interfaces: List[str] = field(default_factory=list)

@dataclass
class Catalog(Resource):
    consumes_storage_interfaces: List[str] = field(default_factory=list)
    implements_catalog_interfaces: Dict[str, List[str]] = field(default_factory=dict)

@dataclass
class QueryEngine(Resource):
    consumes_storage_interfaces: List[str] = field(default_factory=list)
    consumes_catalog_interfaces: List[str] = field(default_factory=list)
    supports_path_based_access: Optional[bool] = None

@dataclass
class Stack:
    query_engine: QueryEngine
    catalog: Optional[Catalog]
    storage: Storage
    catalog_interface: Optional[CatalogInterface]
    storage_interface: StorageInterface

class Loader:
    def __init__(self, warn_on_error=False):
        self.warn_on_error = warn_on_error
        self.storage_interfaces: Dict[str, StorageInterface] = {}
        self.storages: Dict[str, Storage] = {}
        self.catalog_interfaces: Dict[str, CatalogInterface] = {}
        self.catalogs: Dict[str, Catalog] = {}
        self.query_engines: Dict[str, QueryEngine] = {}

    def load_yaml(self, filepath):
        try:
            with open(filepath, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            if self.warn_on_error:
                print(f"Warning: Failed to load {filepath}: {e}")
            return {}

    def load_data(self):
        files = {
            "database/storage_interface.yml": (self.storage_interfaces, StorageInterface),
            "database/storage.yml": (self.storages, Storage),
            "database/catalog_interface.yml": (self.catalog_interfaces, CatalogInterface),
            "database/catalog.yml": (self.catalogs, Catalog),
            "database/query_engine.yml": (self.query_engines, QueryEngine),
        }

        for filename, (store, cls) in files.items():
            data = self.load_yaml(filename)
            for key, value in data.items():
                store[key] = cls(key=key, **value)

    def get_valid_stacks(self):
        stacks = []
        for query_engine in self.query_engines.values():
            for storage in self.storages.values():
                matching_storage_interfaces = [
                    self.storage_interfaces[si] for si in storage.implements_storage_interfaces
                    if si in query_engine.consumes_storage_interfaces and si in self.storage_interfaces
                ]

                if not matching_storage_interfaces:
                    continue  # No compatible storage interface

                valid_catalogs = [
                    catalog for catalog in self.catalogs.values()
                    if any(si in catalog.consumes_storage_interfaces for si in storage.implements_storage_interfaces)
                    or not query_engine.consumes_catalog_interfaces
                ]

                if not valid_catalogs:
                    # No catalog needed; create stack with direct storage-query engine connection
                    for si in matching_storage_interfaces:
                        stacks.append(Stack(
                            query_engine=query_engine,
                            catalog=None,
                            storage=storage,
                            catalog_interface=None,
                            storage_interface=si
                        ))
                else:
                    for catalog in valid_catalogs:
                        matching_catalog_interfaces = [
                            self.catalog_interfaces[ci] for ci in catalog.implements_catalog_interfaces
                            if ci in query_engine.consumes_catalog_interfaces and ci in self.catalog_interfaces
                        ]

                        if not matching_catalog_interfaces:
                            continue  # No matching catalog interface

                        for si in matching_storage_interfaces:
                            for ci in matching_catalog_interfaces:
                                stacks.append(Stack(
                                    query_engine=query_engine,
                                    catalog=catalog,
                                    storage=storage,
                                    catalog_interface=ci,
                                    storage_interface=si
                                ))

        return stacks


if __name__ == "__main__":
    loader = Loader(warn_on_error=True)
    loader.load_data()

    valid_stacks = loader.get_valid_stacks()

    for stack in valid_stacks:
        print(stack)

        # Check to see if the stack is already in the database. If not, add it as "compatible"
        # TODO: this is horrifically inefficient (reopening and rewriting the file on each iteration)
        existing_database = None
        with open('database/results.yml', 'r') as file:
            existing_database = yaml.safe_load(file)

        existing_result = next((r for r in existing_database['results'] if
                                # TODO: handle catalog-free
                                stack.catalog is not None and
                                r['catalog'] == stack.catalog.key and
                                r['catalog_interface'] == stack.catalog_interface.key and
                                r['query_engine'] == stack.query_engine.key and
                                r['storage'] == stack.storage.key and
                                r['storage_interface'] == stack.storage_interface.key), None)

        if existing_result is None and stack.catalog is not None:
            print(f"No test results for {stack}, marking it as 'compatible'")
            compatible_result = {
                'catalog': stack.catalog.key,
                'catalog_interface': stack.catalog_interface.key,
                'query_engine': stack.query_engine.key,
                'storage': stack.storage.key,
                'storage_interface': stack.storage_interface.key,
                'results': {
                    'as_of': '2025-02-06',
                    'status': 'compatible',
                    'explanation': "This combination should work, but we haven't tested it yet!",
                    'tests': []
                }
            }

            existing_database['results'].append(compatible_result)

            with open('database/results.yml', 'w') as file:
                yaml.dump(existing_database, file, default_flow_style=False)

        else:
            print(f"Found existing test result for {stack}, leaving it out")
