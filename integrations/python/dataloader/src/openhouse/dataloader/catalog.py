import logging
from typing import Any

import requests
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import load_file_io
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
from pyiceberg.typedef import Identifier

logger = logging.getLogger(__name__)

_TABLE_LOCATION = "tableLocation"


class OpenHouseCatalogError(Exception):
    """Error raised when the OpenHouse catalog fails to load a table."""


class OpenHouseCatalog(Catalog):
    """Client-side catalog implementation for Iceberg tables in OpenHouse.

    Leverages the OpenHouse Tables Service REST API to load table metadata.

    Args:
        name: Catalog name
        uri: OpenHouse Tables Service base URL
        auth_token: JWT Bearer token for authentication
        ssl_ca_cert: Path to CA cert bundle for SSL verification
        timeout_seconds: HTTP request timeout in seconds
    """

    def __init__(
        self,
        name: str,
        uri: str,
        auth_token: str | None = None,
        ssl_ca_cert: str | None = None,
        timeout_seconds: float = 30,
    ):
        super().__init__(name, uri=uri)
        self._uri = uri.rstrip("/")
        self._timeout = timeout_seconds
        logger.info("Initializing OpenHouseCatalog for service at %s", self._uri)
        self._session = requests.Session()
        self._session.headers["Content-Type"] = "application/json"

        if auth_token is not None:
            self._session.headers["Authorization"] = f"Bearer {auth_token}"

        if ssl_ca_cert is not None:
            self._session.verify = ssl_ca_cert

    def __enter__(self):
        return self

    def __exit__(self, *_: Any):
        self.close()

    def close(self):
        self._session.close()

    def load_table(self, identifier: str | Identifier) -> Table:
        database, table = self.identifier_to_database_and_table(identifier)
        url = f"{self._uri}/v1/databases/{database}/tables/{table}"
        logger.info("Calling load_table for table: %s.%s", database, table)

        response = self._session.get(url, timeout=self._timeout)
        if not response.ok:
            if response.status_code == 404:
                raise NoSuchTableError(f"Table {database}.{table} does not exist")
            raise OSError(
                f"Failed to load table {database}.{table}: HTTP {response.status_code}. Response: {response.text}"
            )

        table_response = response.json()
        metadata_location = table_response.get(_TABLE_LOCATION)
        if not metadata_location:
            raise OpenHouseCatalogError(
                f"Response for table {database}.{table} is missing '{_TABLE_LOCATION}'. Response: {table_response}"
            )

        file_io = load_file_io(properties=self.properties, location=metadata_location)
        metadata_file = file_io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(metadata_file)

        logger.debug("Calling load_table succeeded")
        return Table(
            identifier=(database, table),
            metadata=metadata,
            metadata_location=metadata_location,
            io=file_io,
            catalog=self,
        )

    # -- Unsupported operations --
    # Required by the Catalog ABC but not needed for read-only table loading.

    def drop_table(self, *_: Any, **__: Any) -> None:
        raise NotImplementedError

    def purge_table(self, *_: Any, **__: Any) -> None:
        raise NotImplementedError

    def rename_table(self, *_: Any, **__: Any) -> Table:
        raise NotImplementedError

    def create_table(self, *_: Any, **__: Any) -> Table:
        raise NotImplementedError

    def create_table_transaction(self, *_: Any, **__: Any) -> Any:
        raise NotImplementedError

    def register_table(self, *_: Any, **__: Any) -> Table:
        raise NotImplementedError

    def commit_table(self, *_: Any, **__: Any) -> Any:
        raise NotImplementedError

    def list_tables(self, *_: Any, **__: Any) -> list[Identifier]:
        raise NotImplementedError

    def list_namespaces(self, *_: Any, **__: Any) -> list[Identifier]:
        raise NotImplementedError

    def create_namespace(self, *_: Any, **__: Any) -> None:
        raise NotImplementedError

    def drop_namespace(self, *_: Any, **__: Any) -> None:
        raise NotImplementedError

    def load_namespace_properties(self, *_: Any, **__: Any) -> dict[str, str]:
        raise NotImplementedError

    def update_namespace_properties(self, *_: Any, **__: Any) -> Any:
        raise NotImplementedError

    def list_views(self, *_: Any, **__: Any) -> list[Identifier]:
        raise NotImplementedError

    def drop_view(self, *_: Any, **__: Any) -> None:
        raise NotImplementedError

    def table_exists(self, *_: Any, **__: Any) -> bool:
        raise NotImplementedError

    def view_exists(self, *_: Any, **__: Any) -> bool:
        raise NotImplementedError

    def namespace_exists(self, *_: Any, **__: Any) -> bool:
        raise NotImplementedError
