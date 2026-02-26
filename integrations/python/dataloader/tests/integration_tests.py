"""Integration tests for OpenHouseCatalog against a running OpenHouse instance.

Tables are created via the OpenHouse REST API. For tests that need data,
we use PyIceberg to write Parquet files to the table's location on the
host filesystem. Since there is no real Iceberg catalog running locally,
we use a _LocalCommitCatalog that applies metadata updates in memory so
PyIceberg's table.append() can write data files and manifests. The updated
metadata is then written to the path that OpenHouseCatalog reads from.
Only Parquet is tested because PyIceberg does not support ORC writes.
"""

import logging
import os
import shutil
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

import pyarrow as pa
import requests
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import load_file_io
from pyiceberg.serializers import FromInputFile, ToOutputFile
from pyiceberg.table import CommitTableResponse, Table, update_table_metadata

from openhouse.dataloader import OpenHouseDataLoader
from openhouse.dataloader.catalog import OpenHouseCatalog
from openhouse.dataloader.filters import col

BASE_URL = "http://localhost:8000"
DATABASE_ID = "d_e2e"
TABLE_ID_EMPTY = "t_empty"
TABLE_ID_DATA = "t_data"
TABLE_ID_SNAPSHOT = "t_snapshot"
CONTAINER_NAME = "oh-only-openhouse-tables-1"

COL_ID = "id"
COL_NAME = "name"
COL_SCORE = "score"

TABLE_SCHEMA = (
    '{"type": "struct", "fields": ['
    f'{{"id": 1, "required": false, "name": "{COL_ID}", "type": "long"}},'
    f'{{"id": 2, "required": false, "name": "{COL_NAME}", "type": "string"}},'
    f'{{"id": 3, "required": false, "name": "{COL_SCORE}", "type": "double"}}'
    "]}"
)

EXPECTED_DATA = pa.table(
    {
        COL_ID: pa.array([1, 2, 3], type=pa.int64()),
        COL_NAME: pa.array(["alice", "bob", "charlie"], type=pa.string()),
        COL_SCORE: pa.array([1.1, 2.2, 3.3], type=pa.float64()),
    }
)

SNAPSHOT_BATCH_1 = pa.table(
    {
        COL_ID: pa.array([1, 2], type=pa.int64()),
        COL_NAME: pa.array(["alice", "bob"], type=pa.string()),
        COL_SCORE: pa.array([1.1, 2.2], type=pa.float64()),
    }
)

SNAPSHOT_BATCH_2 = pa.table(
    {
        COL_ID: pa.array([3, 4], type=pa.int64()),
        COL_NAME: pa.array(["charlie", "diana"], type=pa.string()),
        COL_SCORE: pa.array([3.3, 4.4], type=pa.float64()),
    }
)


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


def _append_data(token: str, table_id: str, df: pa.Table, metadata_path: str | None = None) -> str:
    """Write data to an OpenHouse table using PyIceberg.

    On the first call, fetches table metadata from the Docker container.
    On subsequent calls, pass the *metadata_path* returned by the previous
    call so the function reads the already-updated host metadata (which
    contains prior snapshots the container doesn't know about).

    Returns the host filesystem path to the metadata file.
    """
    container_metadata_path = _get_metadata_path(token, table_id)
    host_metadata_dir = os.path.dirname(container_metadata_path)
    os.makedirs(host_metadata_dir, exist_ok=True)

    if metadata_path and os.path.exists(metadata_path):
        # Re-use the host metadata written by a previous _append_data call
        io = load_file_io(properties={}, location=metadata_path)
        metadata = FromInputFile.table_metadata(io.new_input(metadata_path))
    else:
        # First append: copy metadata from the Docker container
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
    ToOutputFile.table_metadata(table.metadata, table_io.new_output(container_metadata_path), overwrite=True)
    print(f"Wrote {df.num_rows} rows and updated metadata at {container_metadata_path}")
    return container_metadata_path


def _cleanup_table(token: str, table_id: str, metadata_path: str | None = None) -> None:
    """Delete a table via REST API and clean up local files."""
    try:
        _delete_table(token, table_id)
    except Exception as e:
        print(f"Warning: failed to delete {table_id}: {e}")
    if metadata_path:
        table_dir = os.path.dirname(metadata_path)
        shutil.rmtree(table_dir, ignore_errors=True)


def test_table_with_data(catalog: OpenHouseCatalog) -> None:
    """Load a table with data. Check the data and a custom table property."""
    loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID_DATA)

    batches = [batch for split in loader for batch in split]
    assert len(batches) > 0, "Expected at least one batch"

    result = pa.concat_tables([pa.Table.from_batches([b]) for b in batches])
    result = result.sort_by(COL_ID)

    assert result.num_rows == EXPECTED_DATA.num_rows, f"Expected {EXPECTED_DATA.num_rows} rows, got {result.num_rows}"
    assert result.column(COL_ID).to_pylist() == EXPECTED_DATA.column(COL_ID).to_pylist()
    assert result.column(COL_NAME).to_pylist() == EXPECTED_DATA.column(COL_NAME).to_pylist()
    assert result.column(COL_SCORE).to_pylist() == EXPECTED_DATA.column(COL_SCORE).to_pylist()
    print(f"DataLoader read {result.num_rows} rows with correct values")

    table = catalog.load_table(f"{DATABASE_ID}.{TABLE_ID_DATA}")
    assert table.metadata.properties.get("write.format.default") == "parquet"
    print("Table property verified: write.format.default=parquet")


def test_table_with_data_row_filter(catalog: OpenHouseCatalog) -> None:
    """Load a table with data and apply a row filter."""
    loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID_DATA, filters=col(COL_ID) > 1)

    batches = [batch for split in loader for batch in split]
    assert len(batches) > 0, "Expected at least one batch"

    result = pa.concat_tables([pa.Table.from_batches([b]) for b in batches])
    result = result.sort_by(COL_ID)

    assert result.num_rows == 2, f"Expected 2 rows, got {result.num_rows}"
    assert result.column(COL_ID).to_pylist() == [2, 3]
    assert result.column(COL_NAME).to_pylist() == ["bob", "charlie"]
    assert result.column(COL_SCORE).to_pylist() == [2.2, 3.3]
    print(f"DataLoader read {result.num_rows} filtered rows")


def test_table_with_data_selected_columns(catalog: OpenHouseCatalog) -> None:
    """Load a table with data and select only specific columns."""
    loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID_DATA, columns=[COL_ID, COL_NAME])

    batches = [batch for split in loader for batch in split]
    assert len(batches) > 0, "Expected at least one batch"

    result = pa.concat_tables([pa.Table.from_batches([b]) for b in batches])
    result = result.sort_by(COL_ID)

    assert result.column_names == [COL_ID, COL_NAME], f"Expected [{COL_ID}, {COL_NAME}], got {result.column_names}"
    assert result.num_rows == 3, f"Expected 3 rows, got {result.num_rows}"
    assert result.column(COL_ID).to_pylist() == [1, 2, 3]
    assert result.column(COL_NAME).to_pylist() == ["alice", "bob", "charlie"]
    print(f"DataLoader read {result.num_rows} rows with selected columns {result.column_names}")


def test_empty_table(catalog: OpenHouseCatalog) -> None:
    """Load a table without data. Check splits are empty and a custom table property."""
    loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID_EMPTY)
    splits = list(loader)
    assert splits == [], f"Expected no splits, got {len(splits)}"
    print("DataLoader correctly yielded no splits for empty table")

    table = catalog.load_table(f"{DATABASE_ID}.{TABLE_ID_EMPTY}")
    assert table.metadata.properties.get("myProp") == "hello"
    print("Table property verified: myProp=hello")


def _load_rows(catalog: OpenHouseCatalog, table_id: str, **kwargs) -> pa.Table:
    """Load all rows from a table and return a sorted Arrow table."""
    loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=table_id, **kwargs)
    batches = [batch for split in loader for batch in split]
    if not batches:
        return pa.table({})
    result = pa.concat_tables([pa.Table.from_batches([b]) for b in batches])
    return result.sort_by(COL_ID)


def test_snapshot_id_returns_data_at_snapshot(catalog: OpenHouseCatalog, snap1: int, snap2: int) -> None:
    """Load with snapshot_id=snap1 returns only the first batch of data."""
    result = _load_rows(catalog, TABLE_ID_SNAPSHOT, snapshot_id=snap1)
    assert result.num_rows == 2, f"Expected 2 rows at snapshot 1, got {result.num_rows}"
    assert result.column(COL_ID).to_pylist() == [1, 2]
    print(f"snapshot_id={snap1} correctly returned {result.num_rows} rows (batch 1 only)")

    result = _load_rows(catalog, TABLE_ID_SNAPSHOT, snapshot_id=snap2)
    assert result.num_rows == 4, f"Expected 4 rows at snapshot 2, got {result.num_rows}"
    assert result.column(COL_ID).to_pylist() == [1, 2, 3, 4]
    print(f"snapshot_id={snap2} correctly returned {result.num_rows} rows (both batches)")


def test_snapshot_id_with_filters(catalog: OpenHouseCatalog, snap2: int) -> None:
    """snapshot_id works alongside row filters."""
    result = _load_rows(catalog, TABLE_ID_SNAPSHOT, snapshot_id=snap2, filters=col(COL_ID) > 2)
    assert result.num_rows == 2, f"Expected 2 filtered rows at snapshot 2, got {result.num_rows}"
    assert result.column(COL_ID).to_pylist() == [3, 4]
    print(f"snapshot_id={snap2} with filter correctly returned {result.num_rows} rows")


def test_snapshot_id_invalid(catalog: OpenHouseCatalog) -> None:
    """Loading with a non-existent snapshot_id raises an error."""
    try:
        _load_rows(catalog, TABLE_ID_SNAPSHOT, snapshot_id=-1)
        raise AssertionError("Expected an error for invalid snapshot_id")
    except Exception as e:
        print(f"Invalid snapshot_id correctly raised {type(e).__name__}: {e}")


def test_nonexistent_table(catalog: OpenHouseCatalog) -> None:
    """Check that loading a nonexistent table raises NoSuchTableError."""
    try:
        catalog.load_table(f"{DATABASE_ID}.nonexistent_table")
        raise AssertionError("Expected NoSuchTableError")
    except NoSuchTableError:
        print("load_table correctly raised NoSuchTableError for nonexistent table")


def read_token(path: str) -> str:
    try:
        with open(path) as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"Token file not found: {path}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python integration_tests.py <token_file>")
        sys.exit(1)

    token_str = read_token(sys.argv[1])
    catalog = OpenHouseCatalog(name="integration-test", uri=BASE_URL, auth_token=token_str)

    # --- Table with data ---
    data_metadata_path = None
    try:
        _create_table(
            token_str,
            TABLE_ID_DATA,
            TABLE_SCHEMA,
            tableProperties={"write.format.default": "parquet"},
        )
        data_metadata_path = _append_data(token_str, TABLE_ID_DATA, EXPECTED_DATA)
        test_table_with_data(catalog)
        test_table_with_data_row_filter(catalog)
        test_table_with_data_selected_columns(catalog)
    finally:
        _cleanup_table(token_str, TABLE_ID_DATA, data_metadata_path)

    # --- Snapshot ID ---
    snapshot_metadata_path = None
    try:
        _create_table(
            token_str,
            TABLE_ID_SNAPSHOT,
            TABLE_SCHEMA,
            tableProperties={"write.format.default": "parquet"},
        )
        snapshot_metadata_path = _append_data(token_str, TABLE_ID_SNAPSHOT, SNAPSHOT_BATCH_1)
        table = catalog.load_table(f"{DATABASE_ID}.{TABLE_ID_SNAPSHOT}")
        snap1 = table.metadata.current_snapshot_id

        snapshot_metadata_path = _append_data(
            token_str, TABLE_ID_SNAPSHOT, SNAPSHOT_BATCH_2, metadata_path=snapshot_metadata_path
        )
        table = catalog.load_table(f"{DATABASE_ID}.{TABLE_ID_SNAPSHOT}")
        snap2 = table.metadata.current_snapshot_id

        assert snap1 != snap2, f"Expected different snapshot IDs, got {snap1} for both"
        print(f"Snapshot IDs: snap1={snap1}, snap2={snap2}")

        test_snapshot_id_returns_data_at_snapshot(catalog, snap1, snap2)
        test_snapshot_id_with_filters(catalog, snap2)
        test_snapshot_id_invalid(catalog)
    finally:
        _cleanup_table(token_str, TABLE_ID_SNAPSHOT, snapshot_metadata_path)

    # --- Empty table ---
    try:
        _create_table(
            token_str,
            TABLE_ID_EMPTY,
            TABLE_SCHEMA,
            tableProperties={"myProp": "hello"},
        )
        _copy_metadata_from_container(token_str, TABLE_ID_EMPTY)
        test_empty_table(catalog)
    finally:
        _cleanup_table(token_str, TABLE_ID_EMPTY)

    # --- Nonexistent table ---
    test_nonexistent_table(catalog)

    print("All integration tests passed")
