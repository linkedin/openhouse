from unittest.mock import MagicMock, patch

import pytest

from openhouse.dataloader.openhouse_table_catalog import OpenHouseTableCatalog, TableCatalog


class TestTableCatalogProtocol:
    """Tests that catalogs satisfy the TableCatalog protocol."""

    def test_openhouse_table_catalog_satisfies_protocol(self):
        catalog = OpenHouseTableCatalog(uri="http://localhost:8080")
        assert isinstance(catalog, TableCatalog)

    def test_pyiceberg_catalog_satisfies_protocol(self):
        from pyiceberg.catalog import Catalog

        assert issubclass(Catalog, TableCatalog)


class TestOpenHouseTableCatalogAuth:
    """Tests for OpenHouseTableCatalog authentication, mirroring Java SmokeTest patterns."""

    def test_auth_token_header_set(self):
        """When token is provided, Authorization header should be set."""
        catalog = OpenHouseTableCatalog(uri="http://localhost:8080", token="my-token")
        assert catalog._session.headers["Authorization"] == "Bearer my-token"

    def test_no_auth_header_when_no_token(self):
        """When token is None, no Authorization header should be set."""
        catalog = OpenHouseTableCatalog(uri="http://localhost:8080")
        assert "Authorization" not in catalog._session.headers

    def test_trust_store_sets_ssl_verify(self):
        """When trust_store is provided, session.verify should be set to the path."""
        catalog = OpenHouseTableCatalog(uri="http://localhost:8080", trust_store="/path/to/ca-bundle.crt")
        assert catalog._session.verify == "/path/to/ca-bundle.crt"

    def test_no_trust_store_default_verify(self):
        """When trust_store is None, session.verify should use the default (True)."""
        catalog = OpenHouseTableCatalog(uri="http://localhost:8080")
        assert catalog._session.verify is True

    def test_uri_trailing_slash_stripped(self):
        """Trailing slash on URI should be stripped."""
        catalog = OpenHouseTableCatalog(uri="http://localhost:8080/")
        assert catalog._uri == "http://localhost:8080"


class TestOpenHouseTableCatalogLoadTable:
    """Tests for OpenHouseTableCatalog.load_table, mirroring Java SmokeTest request verification."""

    def _make_catalog_with_mock_session(self):
        catalog = OpenHouseTableCatalog(uri="http://localhost:8080", token="test-token")
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
        """load_table should accept a tuple identifier."""
        catalog = self._make_catalog_with_mock_session()

        with patch("openhouse.dataloader.openhouse_table_catalog.FromInputFile") as mock_from_input:
            mock_from_input.table_metadata.return_value = MagicMock()
            table = catalog.load_table(("my_db", "my_table"))

        catalog._session.get.assert_called_once_with("http://localhost:8080/v1/databases/my_db/tables/my_table")
        assert table.name() == ("my_db", "my_table")
        assert table.metadata_location == "file:///tmp/test-metadata.json"

    def test_load_table_with_string(self):
        """load_table should accept a dot-separated string identifier."""
        catalog = self._make_catalog_with_mock_session()

        with patch("openhouse.dataloader.openhouse_table_catalog.FromInputFile") as mock_from_input:
            mock_from_input.table_metadata.return_value = MagicMock()
            catalog.load_table("my_db.my_table")

        catalog._session.get.assert_called_once_with("http://localhost:8080/v1/databases/my_db/tables/my_table")

    def test_load_table_rejects_invalid_identifier(self):
        """load_table should raise ValueError for identifiers that aren't 2 parts."""
        catalog = self._make_catalog_with_mock_session()

        with pytest.raises(ValueError, match="Expected identifier with 2 parts"):
            catalog.load_table("only_one_part")

        with pytest.raises(ValueError, match="Expected identifier with 2 parts"):
            catalog.load_table(("a", "b", "c"))
