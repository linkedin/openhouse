import logging
from typing import Protocol, runtime_checkable

import requests
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
from pyiceberg.typedef import Identifier

logger = logging.getLogger(__name__)


@runtime_checkable
class TableCatalog(Protocol):
    """Protocol for catalogs that can load Iceberg tables.

    Any PyIceberg Catalog satisfies this protocol, as does OpenHouseTableCatalog.
    """

    def load_table(self, identifier: str | Identifier) -> Table: ...


class OpenHouseTableCatalog:
    """Catalog for loading Iceberg table metadata from the OpenHouse Tables Service.

    Follows the same authentication pattern as the Java OpenHouseCatalog:
    optional Bearer token auth and optional SSL trust store configuration.
    """

    def __init__(
        self,
        uri: str,
        token: str | None = None,
        trust_store: str | None = None,
    ):
        """
        Args:
            uri: OpenHouse Tables Service base URL (required)
            token: JWT Bearer token for authentication (optional)
            trust_store: Path to CA cert bundle for SSL verification (optional)
        """
        self._uri = uri.rstrip("/")
        self._session = requests.Session()
        self._session.headers["Content-Type"] = "application/json"

        if token is not None:
            self._session.headers["Authorization"] = f"Bearer {token}"

        if trust_store is not None:
            self._session.verify = trust_store

    def load_table(self, identifier: str | Identifier) -> Table:
        """Load an Iceberg Table from the OpenHouse Tables Service.

        Args:
            identifier: Table identifier as "database.table" string or ("database", "table") tuple

        Returns:
            PyIceberg Table object

        Raises:
            requests.HTTPError: If the API request fails
            ValueError: If the identifier does not contain exactly two parts
        """
        parts = identifier.split(".") if isinstance(identifier, str) else list(identifier)

        if len(parts) != 2:
            raise ValueError(f"Expected identifier with 2 parts (database, table), got {len(parts)}: {identifier}")

        database, table = parts
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
            catalog=None,
        )
