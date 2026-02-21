"""Integration test for OpenHouseCatalog against a running OpenHouse instance."""

import os
import subprocess
import sys

import requests
from pyiceberg.exceptions import NoSuchTableError

from openhouse.dataloader.catalog import OpenHouseCatalog

BASE_URL = "http://localhost:8000"
DATABASE_ID = "d_e2e"
TABLE_ID = "t_catalog"
CONTAINER_NAME = "oh-only-openhouse-tables-1"


def create_table(token: str) -> dict:
    url = f"{BASE_URL}/v1/databases/{DATABASE_ID}/tables/"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {
        "tableId": TABLE_ID,
        "databaseId": DATABASE_ID,
        "baseTableVersion": "INITIAL_VERSION",
        "clusterId": "LocalFSCluster",
        "schema": (
            '{"type": "struct", "fields": ['
            '{"id": 1, "required": true, "name": "id", "type": "string"},'
            '{"id": 2, "required": true, "name": "name", "type": "string"},'
            '{"id": 3, "required": true, "name": "ts", "type": "timestamp"}'
            "]}"
        ),
        "timePartitioning": {"columnName": "ts", "granularity": "HOUR"},
        "clustering": [{"columnName": "name"}],
        "tableProperties": {"key": "value"},
    }
    response = requests.post(url, json=payload, headers=headers)
    assert response.status_code == 201, f"Failed to create table: {response.status_code} {response.text}"
    print(f"Created table {DATABASE_ID}.{TABLE_ID}")
    return response.json()


def delete_table(token: str) -> None:
    url = f"{BASE_URL}/v1/databases/{DATABASE_ID}/tables/{TABLE_ID}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    response = requests.delete(url, headers=headers)
    assert response.status_code == 204, f"Failed to delete table: {response.status_code} {response.text}"
    print(f"Deleted table {DATABASE_ID}.{TABLE_ID}")


def copy_metadata_from_container(token: str) -> None:
    """Copy table metadata from the Docker container to the host filesystem."""
    url = f"{BASE_URL}/v1/databases/{DATABASE_ID}/tables/{TABLE_ID}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    assert response.status_code == 200, f"Failed to get table: {response.status_code} {response.text}"

    location = response.json()["tableLocation"]
    # Strip file: prefix to get the filesystem path
    metadata_path = location.removeprefix("file:")
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


def read_token(path: str) -> str:
    try:
        with open(path) as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"Token file not found: {path}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python integration_test_catalog.py <token_file>")
        sys.exit(1)

    token_str = read_token(sys.argv[1])
    catalog = OpenHouseCatalog(name="integration-test", uri=BASE_URL, auth_token=token_str)

    try:
        create_table(token_str)
        copy_metadata_from_container(token_str)
        test_load_table(catalog)
        test_load_nonexistent_table(catalog)
    finally:
        try:
            delete_table(token_str)
        except Exception as e:
            print(f"Warning: cleanup failed: {e}")

    print("All integration catalog tests passed")
