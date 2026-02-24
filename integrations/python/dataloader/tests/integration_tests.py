"""Integration test for OpenHouseCatalog against a running OpenHouse instance."""

import os
import shutil
import subprocess
import sys

import pyarrow as pa
import requests
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import load_file_io
from pyiceberg.serializers import FromInputFile, ToOutputFile
from pyiceberg.table import CommitTableResponse, Table, update_table_metadata

from openhouse.dataloader import OpenHouseDataLoader
from openhouse.dataloader.catalog import OpenHouseCatalog

BASE_URL = "http://localhost:8000"
DATABASE_ID = "d_e2e"
TABLE_ID = "t_catalog"
TABLE_ID_DATA = "t_data"
CONTAINER_NAME = "oh-only-openhouse-tables-1"


def _headers(token: str) -> dict:
    return {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}


def _create_table(token: str, table_id: str, schema: str, **extra_payload) -> dict:
    url = f"{BASE_URL}/v1/databases/{DATABASE_ID}/tables/"
    payload = {
        "tableId": table_id,
        "databaseId": DATABASE_ID,
        "baseTableVersion": "INITIAL_VERSION",
        "clusterId": "LocalFSCluster",
        "schema": schema,
        **extra_payload,
    }
    response = requests.post(url, json=payload, headers=_headers(token))
    assert response.status_code == 201, f"Failed to create table: {response.status_code} {response.text}"
    print(f"Created table {DATABASE_ID}.{table_id}")
    return response.json()


def _delete_table(token: str, table_id: str) -> None:
    url = f"{BASE_URL}/v1/databases/{DATABASE_ID}/tables/{table_id}"
    response = requests.delete(url, headers=_headers(token))
    assert response.status_code == 204, f"Failed to delete table: {response.status_code} {response.text}"
    print(f"Deleted table {DATABASE_ID}.{table_id}")


def _get_metadata_path(token: str, table_id: str) -> str:
    """Get the host filesystem path to the table metadata file."""
    url = f"{BASE_URL}/v1/databases/{DATABASE_ID}/tables/{table_id}"
    response = requests.get(url, headers=_headers(token))
    assert response.status_code == 200, f"Failed to get table: {response.status_code} {response.text}"
    location = response.json()["tableLocation"]
    return location.removeprefix("file:")


def _copy_metadata_from_container(token: str, table_id: str) -> str:
    """Copy table metadata from the Docker container to the host filesystem.

    Returns the host filesystem path to the metadata file.
    """
    metadata_path = _get_metadata_path(token, table_id)
    metadata_dir = os.path.dirname(metadata_path)

    os.makedirs(metadata_dir, exist_ok=True)
    container_name = os.environ.get("OH_CONTAINER", CONTAINER_NAME)
    result = subprocess.run(
        ["docker", "cp", f"{container_name}:{metadata_path}", metadata_path],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"docker cp failed: {result.stderr}"
    print(f"Copied metadata from container to {metadata_path}")
    return metadata_path


class _LocalCommitCatalog(NoopCatalog):
    """Minimal catalog that applies metadata updates without persisting them.

    PyIceberg's table.append() writes data files and manifests to the table
    location, then calls catalog.commit_table() to update the metadata.
    This catalog applies the metadata updates in memory so the Table object
    receives the new snapshot. The caller is responsible for persisting the
    updated metadata to disk afterward.
    """

    def commit_table(self, table, requirements, updates):
        new_metadata = update_table_metadata(base_metadata=table.metadata, updates=updates)
        return CommitTableResponse(metadata=new_metadata, metadata_location=table.metadata_location)


def _append_data(token: str, table_id: str, df: pa.Table) -> str:
    """Write data to an OpenHouse table using PyIceberg.

    Fetches the table metadata from the Docker container, writes Parquet
    data files and manifests to the host via PyIceberg's append API, then
    writes the updated metadata (with the new snapshot) to the host path
    that the OpenHouseCatalog will read.

    Returns the host filesystem path to the metadata file.
    """
    container_metadata_path = _get_metadata_path(token, table_id)
    host_metadata_dir = os.path.dirname(container_metadata_path)
    os.makedirs(host_metadata_dir, exist_ok=True)

    # Copy metadata from container to a temp file (not the final path)
    tmp_metadata = container_metadata_path + ".tmp"
    container_name = os.environ.get("OH_CONTAINER", CONTAINER_NAME)
    result = subprocess.run(
        ["docker", "cp", f"{container_name}:{container_metadata_path}", tmp_metadata],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"docker cp failed: {result.stderr}"

    io = load_file_io(properties={}, location=tmp_metadata)
    metadata = FromInputFile.table_metadata(io.new_input(tmp_metadata))
    os.remove(tmp_metadata)

    table_io = load_file_io({**metadata.properties}, location=metadata.location)
    table = Table(
        identifier=(DATABASE_ID, table_id),
        metadata=metadata,
        metadata_location=container_metadata_path,
        io=table_io,
        catalog=_LocalCommitCatalog("local"),
    )
    table.append(df)

    # Write the updated metadata (with snapshot) to the path the OpenHouseCatalog expects
    ToOutputFile.table_metadata(table.metadata, table_io.new_output(container_metadata_path))
    print(f"Wrote {df.num_rows} rows and updated metadata at {container_metadata_path}")
    return container_metadata_path


CATALOG_TABLE_SCHEMA = (
    '{"type": "struct", "fields": ['
    '{"id": 1, "required": true, "name": "id", "type": "string"},'
    '{"id": 2, "required": true, "name": "name", "type": "string"},'
    '{"id": 3, "required": true, "name": "ts", "type": "timestamp"}'
    "]}"
)

DATA_TABLE_SCHEMA = (
    '{"type": "struct", "fields": ['
    '{"id": 1, "required": false, "name": "id", "type": "long"},'
    '{"id": 2, "required": false, "name": "name", "type": "string"},'
    '{"id": 3, "required": false, "name": "score", "type": "double"}'
    "]}"
)

EXPECTED_DATA = pa.table(
    {
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "name": pa.array(["alice", "bob", "charlie"], type=pa.string()),
        "score": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
    }
)


def test_load_table(catalog: OpenHouseCatalog) -> None:
    table = catalog.load_table(f"{DATABASE_ID}.{TABLE_ID}")

    assert table.name() == (DATABASE_ID, TABLE_ID), f"Unexpected name: {table.name()}"
    assert table.metadata_location, "metadata_location should not be empty"

    field_names = {field.name for field in table.schema().fields}
    assert field_names == {"id", "name", "ts"}, f"Unexpected schema fields: {field_names}"

    print(f"load_table returned table with name={table.name()}, metadata_location={table.metadata_location}")


def test_load_nonexistent_table(catalog: OpenHouseCatalog) -> None:
    try:
        catalog.load_table(f"{DATABASE_ID}.nonexistent_table")
        raise AssertionError("Expected NoSuchTableError")
    except NoSuchTableError:
        print("load_table correctly raised NoSuchTableError for nonexistent table")


def test_data_loader_empty_table(catalog: OpenHouseCatalog) -> None:
    """Verify DataLoader yields no splits for an empty table."""
    loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID)
    splits = list(loader)
    assert splits == [], f"Expected no splits, got {len(splits)}"
    print("DataLoader correctly yielded no splits for empty table")


def test_table_properties(catalog: OpenHouseCatalog) -> None:
    """Verify table properties set during creation are accessible."""
    table = catalog.load_table(f"{DATABASE_ID}.{TABLE_ID}")
    assert table.metadata.properties.get("key") == "value", (
        f"Expected property key='value', got {table.metadata.properties}"
    )
    assert table.metadata.properties.get("myProp") == "hello", (
        f"Expected property myProp='hello', got {table.metadata.properties}"
    )
    print(f"Table properties verified: {table.metadata.properties}")


def test_data_loader_reads_data(catalog: OpenHouseCatalog) -> None:
    """Verify DataLoader reads data written via PyIceberg."""
    loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID_DATA)

    batches = [batch for split in loader for batch in split]
    assert len(batches) > 0, "Expected at least one batch"

    result = pa.concat_tables([pa.Table.from_batches([b]) for b in batches])
    result = result.sort_by("id")

    assert result.num_rows == EXPECTED_DATA.num_rows, f"Expected {EXPECTED_DATA.num_rows} rows, got {result.num_rows}"
    assert result.column("id").to_pylist() == EXPECTED_DATA.column("id").to_pylist()
    assert result.column("name").to_pylist() == EXPECTED_DATA.column("name").to_pylist()
    assert result.column("score").to_pylist() == EXPECTED_DATA.column("score").to_pylist()
    print(f"DataLoader read {result.num_rows} rows with correct values")


def read_token(path: str) -> str:
    try:
        with open(path) as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"Token file not found: {path}")
        sys.exit(1)


def _cleanup_table(token: str, table_id: str, metadata_path: str | None = None) -> None:
    """Delete a table via REST API and clean up local files."""
    try:
        _delete_table(token, table_id)
    except Exception as e:
        print(f"Warning: failed to delete {table_id}: {e}")
    if metadata_path:
        table_dir = os.path.dirname(metadata_path)
        shutil.rmtree(table_dir, ignore_errors=True)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python integration_tests.py <token_file>")
        sys.exit(1)

    token_str = read_token(sys.argv[1])
    catalog = OpenHouseCatalog(name="integration-test", uri=BASE_URL, auth_token=token_str)

    # --- Catalog tests (empty table) ---
    try:
        _create_table(
            token_str,
            TABLE_ID,
            CATALOG_TABLE_SCHEMA,
            timePartitioning={"columnName": "ts", "granularity": "HOUR"},
            clustering=[{"columnName": "name"}],
            tableProperties={"key": "value", "myProp": "hello"},
        )
        _copy_metadata_from_container(token_str, TABLE_ID)
        test_load_table(catalog)
        test_load_nonexistent_table(catalog)
        test_data_loader_empty_table(catalog)
        test_table_properties(catalog)
    finally:
        _cleanup_table(token_str, TABLE_ID)

    # --- Data loading tests (table with rows) ---
    data_metadata_path = None
    try:
        _create_table(
            token_str,
            TABLE_ID_DATA,
            DATA_TABLE_SCHEMA,
            tableProperties={"write.format.default": "parquet"},
        )
        data_metadata_path = _append_data(token_str, TABLE_ID_DATA, EXPECTED_DATA)
        test_data_loader_reads_data(catalog)
    finally:
        _cleanup_table(token_str, TABLE_ID_DATA, data_metadata_path)

    print("All integration tests passed")
