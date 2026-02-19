import logging
from typing import Any

import requests
from pyiceberg.catalog import Catalog
from pyiceberg.io import load_file_io
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
from pyiceberg.typedef import Identifier

from openhouse.dataloader.table_identifier import TableIdentifier

logger = logging.getLogger(__name__)

_AUTH_TOKEN = "auth-token"
_TRUST_STORE = "trust-store"
_TABLE_LOCATION = "tableLocation"


class OpenHouseCatalogError(Exception):
    """Error raised when the OpenHouse catalog fails to load a table."""


class OpenHouseCatalog(Catalog):
    """Client-side catalog implementation for Iceberg tables in OpenHouse.

    Leverages the OpenHouse Tables Service REST API to load table metadata.

    Properties:
        uri: OpenHouse Tables Service base URL (required)
        auth-token: JWT Bearer token for authentication (optional)
        trust-store: Path to CA cert bundle for SSL verification (optional)
    """

    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)
        uri = properties.get("uri")
        if uri is None:
            raise ValueError("OpenHouse Table Service URI is required")

        self._uri = uri.rstrip("/")
        logger.info("Initializing OpenHouseCatalog for service at %s", self._uri)
        self._session = requests.Session()
        self._session.headers["Content-Type"] = "application/json"

        token = properties.get(_AUTH_TOKEN)
        if token is not None:
            self._session.headers["Authorization"] = f"Bearer {token}"

        trust_store = properties.get(_TRUST_STORE)
        if trust_store is not None:
            self._session.verify = trust_store

    def __enter__(self):
        return self

    def __exit__(self, *_: Any):
        self.close()

    def close(self):
        self._session.close()

    def load_table(self, identifier: str | Identifier) -> Table:
        table_id = self._parse_identifier(identifier)
        url = f"{self._uri}/v1/databases/{table_id.database}/tables/{table_id.table}"
        logger.info("Calling load_table for table: %s", table_id)

        response = self._session.get(url)
        if not response.ok:
            if response.status_code == 404:
                raise OpenHouseCatalogError(f"Table {table_id} does not exist")
            raise OSError(f"Failed to load table {table_id}: HTTP {response.status_code}. Response: {response.text}")

        table_response = response.json()
        metadata_location = table_response.get(_TABLE_LOCATION)
        if not metadata_location:
            raise OpenHouseCatalogError(
                f"Response for table {table_id} is missing '{_TABLE_LOCATION}'. Response: {table_response}"
            )

        file_io = load_file_io(properties=self.properties, location=metadata_location)
        metadata_file = file_io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(metadata_file)

        logger.debug("Calling load_table succeeded")
        return Table(
            identifier=(table_id.database, table_id.table),
            metadata=metadata,
            metadata_location=metadata_location,
            io=file_io,
            catalog=self,
        )

    @staticmethod
    def _parse_identifier(identifier: str | Identifier) -> TableIdentifier:
        parts = identifier.split(".") if isinstance(identifier, str) else list(identifier)
        if len(parts) != 2:
            raise ValueError(f"Expected identifier with 2 parts (database, table), got {len(parts)}: {identifier}")
        return TableIdentifier(database=parts[0], table=parts[1])

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
