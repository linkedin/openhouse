import json
from unittest.mock import MagicMock, patch

import pytest
import responses
from pyiceberg.exceptions import NoSuchTableError

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
        patch("openhouse.dataloader.catalog.load_file_io") as mock_load_file_io,
    ):
        mock_metadata = MagicMock()
        mock_from_input.table_metadata.return_value = mock_metadata
        mock_file_io = mock_load_file_io.return_value
        yield mock_metadata, mock_file_io


class TestOpenHouseCatalogLoadTable:
    @responses.activate
    def test_load_table_returns_table_with_correct_properties(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)
        mock_metadata, mock_file_io = mock_iceberg_io

        with OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL) as catalog:
            table = catalog.load_table((DATABASE_NAME, TABLE_NAME))

        assert table.name() == (DATABASE_NAME, TABLE_NAME)
        assert table.metadata == mock_metadata
        assert table.metadata_location == METADATA_LOCATION
        assert table.io == mock_file_io
        assert table.catalog == catalog

    @responses.activate
    def test_load_table_with_string_identifier(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)

        with OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL) as catalog:
            catalog.load_table(f"{DATABASE_NAME}.{TABLE_NAME}")

        assert responses.calls[0].request.url == TABLE_URL

    @responses.activate
    def test_load_table_sends_json_content_type(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)

        with OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL) as catalog:
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

        assert responses.calls[0].request.headers["Content-Type"] == "application/json"

    @responses.activate
    def test_load_table_uri_trailing_slash_stripped(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)

        with OpenHouseCatalog(CATALOG_NAME, uri=f"{BASE_URL}/") as catalog:
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

        assert responses.calls[0].request.url == TABLE_URL

    def test_load_table_rejects_invalid_identifier(self):
        with OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL) as catalog:
            with pytest.raises(ValueError, match="hierarchical namespaces are not supported"):
                catalog.load_table("only_one_part")

            with pytest.raises(ValueError, match="hierarchical namespaces are not supported"):
                catalog.load_table(("a", "b", "c"))

    @responses.activate
    def test_load_table_404_raises_no_such_table_error(self):
        responses.get(TABLE_URL, status=404)

        with (
            OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL) as catalog,
            pytest.raises(NoSuchTableError, match=f"{DATABASE_NAME}.{TABLE_NAME} does not exist"),
        ):
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

    @responses.activate
    def test_load_table_500_raises_os_error(self):
        responses.get(TABLE_URL, body="Internal Server Error", status=500)

        with (
            OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL) as catalog,
            pytest.raises(OSError, match="HTTP 500"),
        ):
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

    @responses.activate
    def test_load_table_missing_table_location(self):
        responses.get(TABLE_URL, json={"databaseId": DATABASE_NAME, "tableId": TABLE_NAME}, status=200)

        with (
            OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL) as catalog,
            pytest.raises(OpenHouseCatalogError, match="missing 'tableLocation'"),
        ):
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

    @responses.activate
    def test_load_table_empty_table_location(self):
        responses.get(
            TABLE_URL,
            json={"databaseId": DATABASE_NAME, "tableId": TABLE_NAME, "tableLocation": ""},
            status=200,
        )

        with (
            OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL) as catalog,
            pytest.raises(OpenHouseCatalogError, match="missing 'tableLocation'"),
        ):
            catalog.load_table((DATABASE_NAME, TABLE_NAME))


class TestOpenHouseCatalogProperties:
    @responses.activate
    def test_schemeless_metadata_location_reads_using_default_scheme(self):
        """Verify that a schemeless metadata location is read using the hdfs scheme."""
        # Simulate an API response with a schemeless tableLocation
        schemeless_location = "/warehouse/db/table/metadata.json"
        table_response = json.dumps(
            {
                "databaseId": DATABASE_NAME,
                "tableId": TABLE_NAME,
                "tableLocation": schemeless_location,
            }
        )
        responses.get(TABLE_URL, body=table_response, status=200)

        # Wrap load_file_io: let it create a real PyArrowFileIO with the catalog's properties,
        # but mock fs_by_scheme so it doesn't try to connect to a real HDFS cluster.
        from pyiceberg.io import load_file_io as real_load_file_io

        mock_fs = MagicMock()
        captured_file_io = None

        def patched_load_file_io(**kwargs):
            nonlocal captured_file_io
            captured_file_io = real_load_file_io(**kwargs)
            captured_file_io.fs_by_scheme = MagicMock(return_value=mock_fs)
            return captured_file_io

        with (
            patch("openhouse.dataloader.catalog.FromInputFile") as mock_from_input,
            patch("openhouse.dataloader.catalog.load_file_io", side_effect=patched_load_file_io),
            OpenHouseCatalog(
                CATALOG_NAME,
                uri=BASE_URL,
                properties={"DEFAULT_SCHEME": "hdfs", "DEFAULT_NETLOC": "namenode.example.com:9000"},
            ) as catalog,
        ):
            mock_from_input.table_metadata.return_value = MagicMock()
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

        # Verify the schemeless path was resolved to hdfs using the catalog's properties
        captured_file_io.fs_by_scheme.assert_called_once_with("hdfs", "namenode.example.com:9000")


class TestOpenHouseCatalogAuth:
    AUTH_TOKEN = "test-jwt-token"

    @responses.activate
    def test_load_table_sends_auth_token(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)

        with OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL, auth_token=self.AUTH_TOKEN) as catalog:
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

        assert responses.calls[0].request.headers["Authorization"] == f"Bearer {self.AUTH_TOKEN}"

    @responses.activate
    def test_load_table_without_token_sends_no_auth_header(self, mock_iceberg_io):
        responses.get(TABLE_URL, body=TABLE_RESPONSE_BODY, status=200)

        with OpenHouseCatalog(CATALOG_NAME, uri=BASE_URL) as catalog:
            catalog.load_table((DATABASE_NAME, TABLE_NAME))

        assert "Authorization" not in responses.calls[0].request.headers
