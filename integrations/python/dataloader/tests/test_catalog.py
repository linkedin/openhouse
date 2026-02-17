import json
from unittest.mock import MagicMock, patch

import pytest
import responses

from openhouse.dataloader.catalog import OpenHouseCatalog, OpenHouseCatalogError

CATALOG_NAME = "openhouse"
BASE_URL = "http://localhost:8080"
DATABASE_NAME = "my_db"
TABLE_NAME = "my_table"
TABLE_URL = f"{BASE_URL}/v1/databases/{DATABASE_NAME}/tables/{TABLE_NAME}"
# Not a real file — file I/O is mocked via the mock_iceberg_io fixture.
METADATA_LOCATION = "file:///test-metadata.json"
TABLE_RESPONSE_BODY = json.dumps(
    {
        "databaseId": DATABASE_NAME,
        "tableId": TABLE_NAME,
        "tableLocation": METADATA_LOCATION,
    }
)


@pytest.fixture
def mock_iceberg_io():
    """Mock PyIceberg's file I/O and metadata parsing.

    These are mocked because the catalog tests verify HTTP behavior (request
    construction, headers, error handling), not Iceberg metadata deserialization.
    Without these mocks, PyArrowFileIO would attempt real filesystem/storage
    access and FromInputFile would try to parse actual Iceberg metadata files.
    """
    with (
        patch("openhouse.dataloader.catalog.FromInputFile") as mock_from_input,
        patch("openhouse.dataloader.catalog.PyArrowFileIO") as mock_file_io_cls,
    ):
        mock_metadata = MagicMock()
        mock_from_input.table_metadata.return_value = mock_metadata
        mock_file_io = mock_file_io_cls.return_value
        yield mock_metadata, mock_file_io


class TestOpenHouseCatalogLoadTable:
    @responses.activate
    def test_load_table_returns_table_with_correct_properties(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)
        catalog = OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL)
        mock_metadata, mock_file_io = mock_iceberg_io

        table = catalog.load_table((DATABASE_NAME, TABLE_NAME))

        assert table.name() == (DATABASE_NAME, TABLE_NAME)
        assert table.metadata == mock_metadata
        assert table.metadata_location == METADATA_LOCATION
        assert table.io == mock_file_io
        assert table.catalog == catalog

    @responses.activate
    def test_load_table_with_string_identifier(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)
        catalog = OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL)

        catalog.load_table(f"{DATABASE_NAME}.{TABLE_NAME}")

        assert responses.calls[0].request.url == TABLE_URL

    @responses.activate
    def test_load_table_sends_json_content_type(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)
        catalog = OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL)

        catalog.load_table((DATABASE_NAME, TABLE_NAME))

        assert responses.calls[0].request.headers["Content-Type"] == "application/json"

    @responses.activate
    def test_load_table_uri_trailing_slash_stripped(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)
        catalog = OpenHouseCatalog(CATALOG_NAME, uri="http://localhost:8080/")

        catalog.load_table((DATABASE_NAME, TABLE_NAME))

        assert responses.calls[0].request.url == TABLE_URL

    def test_load_table_rejects_invalid_identifier(self):
        catalog = OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL)

        with pytest.raises(ValueError, match="Expected identifier with 2 parts"):
            catalog.load_table("only_one_part")

        with pytest.raises(ValueError, match="Expected identifier with 2 parts"):
            catalog.load_table(("a", "b", "c"))

    @responses.activate
    def test_load_table_404_raises_catalog_error(self):
        responses.get(TABLE_URL, status=404)
        catalog = OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL)

        with pytest.raises(OpenHouseCatalogError, match="my_db.my_table does not exist"):
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

    @responses.activate
    def test_load_table_500_raises_catalog_error(self):
        responses.get(TABLE_URL, body="Internal Server Error", status=500)
        catalog = OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL)

        with pytest.raises(OpenHouseCatalogError, match="HTTP 500"):
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

    @responses.activate
    def test_load_table_missing_table_location(self):
        responses.get(TABLE_URL, json={"databaseId": DATABASE_NAME, "tableId": TABLE_NAME}, status=200)
        catalog = OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL)

        with pytest.raises(OpenHouseCatalogError, match="missing 'tableLocation'"):
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

    @responses.activate
    def test_load_table_empty_table_location(self):
        responses.get(
            TABLE_URL,
            json={"databaseId": DATABASE_NAME, "tableId": TABLE_NAME, "tableLocation": ""},
            status=200,
        )
        catalog = OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL)

        with pytest.raises(OpenHouseCatalogError, match="missing 'tableLocation'"):
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

    @responses.activate
    def test_load_table_unreadable_metadata_raises_catalog_error(self, tmp_path):
        # File I/O is NOT mocked here — uses a real nonexistent path to trigger the error.
        nonexistent = f"file://{tmp_path}/nonexistent/metadata.json"
        responses.get(
            TABLE_URL,
            json={"databaseId": DATABASE_NAME, "tableId": TABLE_NAME, "tableLocation": nonexistent},
            status=200,
        )
        catalog = OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL)

        with pytest.raises(OpenHouseCatalogError, match="Failed to read table metadata"):
            catalog.load_table((DATABASE_NAME, TABLE_NAME))


class TestOpenHouseCatalogAuth:
    AUTH_TOKEN = "test-jwt-token"

    @responses.activate
    def test_load_table_sends_auth_token(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)
        catalog = OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL, **{"auth-token": self.AUTH_TOKEN})

        catalog.load_table((DATABASE_NAME, TABLE_NAME))

        assert responses.calls[0].request.headers["Authorization"] == f"Bearer {self.AUTH_TOKEN}"

    @responses.activate
    def test_load_table_without_token_sends_no_auth_header(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)
        catalog = OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL)

        catalog.load_table((DATABASE_NAME, TABLE_NAME))

        assert "Authorization" not in responses.calls[0].request.headers
