import logging
from typing import Any

import requests
from pyiceberg.catalog import Catalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
from pyiceberg.typedef import Identifier

logger = logging.getLogger(__name__)

_AUTH_TOKEN = "auth-token"
_TRUST_STORE = "trust-store"


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
        self._session = requests.Session()
        self._session.headers["Content-Type"] = "application/json"

        token = properties.get(_AUTH_TOKEN)
        if token is not None:
            self._session.headers["Authorization"] = f"Bearer {token}"

        trust_store = properties.get(_TRUST_STORE)
        if trust_store is not None:
            self._session.verify = trust_store

    def load_table(self, identifier: str | Identifier) -> Table:
        database, table = self.identifier_to_database_and_table(identifier)
        url = f"{self._uri}/v1/databases/{database}/tables/{table}"
        logger.info("Loading table '%s.%s' from %s", database, table, url)

        response = self._session.get(url)
        response.raise_for_status()

        table_response = response.json()
        metadata_location = table_response["tableLocation"]

        file_io = PyArrowFileIO()
        metadata_file = file_io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(metadata_file)

        return Table(
            identifier=(database, table),
            metadata=metadata,
            metadata_location=metadata_location,
            io=file_io,
            catalog=self,
        )

    @staticmethod
    def identifier_to_database_and_table(identifier: str | Identifier) -> tuple[str, str]:
        parts = identifier.split(".") if isinstance(identifier, str) else list(identifier)
        if len(parts) != 2:
            raise ValueError(f"Expected identifier with 2 parts (database, table), got {len(parts)}: {identifier}")
        return parts[0], parts[1]

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
