from unittest.mock import MagicMock, patch

import pytest
from pyiceberg.catalog import Catalog

from openhouse.dataloader.catalog import OpenHouseCatalog


class TestOpenHouseCatalogIsACatalog:
    def test_inherits_from_catalog(self):
        assert issubclass(OpenHouseCatalog, Catalog)

    def test_instantiates_as_catalog(self):
        catalog = OpenHouseCatalog("openhouse", uri="http://localhost:8080")
        assert isinstance(catalog, Catalog)


class TestOpenHouseCatalogInit:
    def test_uri_required(self):
        with pytest.raises(ValueError, match="URI is required"):
            OpenHouseCatalog("openhouse")

    def test_auth_token_header_set(self):
        catalog = OpenHouseCatalog("openhouse", uri="http://localhost:8080", **{"auth-token": "my-token"})
        assert catalog._session.headers["Authorization"] == "Bearer my-token"

    def test_no_auth_header_when_no_token(self):
        catalog = OpenHouseCatalog("openhouse", uri="http://localhost:8080")
        assert "Authorization" not in catalog._session.headers

    def test_trust_store_sets_ssl_verify(self):
        catalog = OpenHouseCatalog(
            "openhouse", uri="http://localhost:8080", **{"trust-store": "/path/to/ca-bundle.crt"}
        )
        assert catalog._session.verify == "/path/to/ca-bundle.crt"

    def test_no_trust_store_default_verify(self):
        catalog = OpenHouseCatalog("openhouse", uri="http://localhost:8080")
        assert catalog._session.verify is True

    def test_uri_trailing_slash_stripped(self):
        catalog = OpenHouseCatalog("openhouse", uri="http://localhost:8080/")
        assert catalog._uri == "http://localhost:8080"


class TestOpenHouseCatalogLoadTable:
    def _make_catalog_with_mock_session(self):
        catalog = OpenHouseCatalog("openhouse", uri="http://localhost:8080", **{"auth-token": "test-token"})
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "databaseId": "my_db",
            "tableId": "my_table",
            "tableLocation": "file:///tmp/test-metadata.json",
        }
        mock_response.raise_for_status = MagicMock()
        catalog._session.get = MagicMock(return_value=mock_response)
        return catalog

    def test_load_table_with_tuple(self):
        catalog = self._make_catalog_with_mock_session()

        with patch("openhouse.dataloader.catalog.FromInputFile") as mock_from_input:
            mock_from_input.table_metadata.return_value = MagicMock()
            table = catalog.load_table(("my_db", "my_table"))

        catalog._session.get.assert_called_once_with("http://localhost:8080/v1/databases/my_db/tables/my_table")
        assert table.name() == ("my_db", "my_table")
        assert table.metadata_location == "file:///tmp/test-metadata.json"

    def test_load_table_with_string(self):
        catalog = self._make_catalog_with_mock_session()

        with patch("openhouse.dataloader.catalog.FromInputFile") as mock_from_input:
            mock_from_input.table_metadata.return_value = MagicMock()
            catalog.load_table("my_db.my_table")

        catalog._session.get.assert_called_once_with("http://localhost:8080/v1/databases/my_db/tables/my_table")

    def test_load_table_rejects_invalid_identifier(self):
        catalog = self._make_catalog_with_mock_session()

        with pytest.raises(ValueError, match="Expected identifier with 2 parts"):
            catalog.load_table("only_one_part")

        with pytest.raises(ValueError, match="Expected identifier with 2 parts"):
            catalog.load_table(("a", "b", "c"))
