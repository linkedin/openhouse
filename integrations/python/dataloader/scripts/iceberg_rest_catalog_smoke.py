from __future__ import annotations

import pathlib
import sys
import uuid

import requests
from pyiceberg.catalog.rest import RestCatalog

DATABASE_ID = "d3"
BASE_URL = "http://localhost:8000"


def read_token(path: str) -> str:
    token = pathlib.Path(path).read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError(f"Token file is empty: {path}")
    return token


def create_table(token: str, table_id: str) -> None:
    create_table_url = f"{BASE_URL}/v1/databases/{DATABASE_ID}/tables/"
    create_table_payload = {
        "tableId": table_id,
        "databaseId": DATABASE_ID,
        "baseTableVersion": "INITIAL_VERSION",
        "clusterId": "LocalFSCluster",
        "schema": '{"type": "struct", "fields": [{"id": 1,"required": true,"name": "id","type": "string"},{"id": 2,"required": true,"name": "name","type": "string"},{"id": 3,"required": true,"name": "ts","type": "timestamp"}]}',
        "timePartitioning": {"columnName": "ts", "granularity": "HOUR"},
        "clustering": [{"columnName": "name"}],
        "tableProperties": {"key": "value"},
    }
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    response = requests.post(create_table_url, json=create_table_payload, headers=headers, timeout=30)
    if response.status_code != 201:
        raise RuntimeError(f"Failed to create smoke table: {response.status_code} {response.text}")


def drop_table(token: str, table_id: str) -> None:
    delete_table_url = f"{BASE_URL}/v1/databases/{DATABASE_ID}/tables/{table_id}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    response = requests.delete(delete_table_url, headers=headers, timeout=30)
    if response.status_code not in (204, 404):
        raise RuntimeError(f"Failed to drop smoke table: {response.status_code} {response.text}")


def run_smoke(token: str) -> None:
    table_id = f"iceberg_rest_smoke_{uuid.uuid4().hex[:8]}"
    table_identifier = (DATABASE_ID, table_id)

    create_table(token, table_id)
    try:
        catalog = RestCatalog(name="openhouse", uri=BASE_URL, token=token, warehouse="openhouse")

        exists = catalog.table_exists(table_identifier)
        if not exists:
            raise AssertionError(f"table_exists returned false for {table_identifier}")

        tables = catalog.list_tables((DATABASE_ID,))
        if table_identifier not in tables:
            raise AssertionError(f"list_tables missing {table_identifier}; got: {tables}")

        loaded_table = catalog.load_table(table_identifier)
        if loaded_table is None:
            raise AssertionError(f"load_table returned None for {table_identifier}")
    finally:
        drop_table(token, table_id)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: uv run python scripts/iceberg_rest_catalog_smoke.py <token_file>")

    token = read_token(sys.argv[1])
    run_smoke(token)
    print("Iceberg REST catalog smoke test passed")


if __name__ == "__main__":
    main()
