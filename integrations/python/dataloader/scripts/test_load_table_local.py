"""Manual integration test for OpenHouseCatalog.load_table against a local Docker instance.

Prerequisites:
    1. mkdir -p /tmp/openhouse
    2. ./gradlew dockerUp -Precipe=oh-only
    3. uv run python scripts/test_load_table_local.py
"""

import json
import logging
import sys

import requests

from pyiceberg.exceptions import NoSuchTableError

from openhouse.dataloader import OpenHouseCatalog

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

SERVICE_URL = "http://localhost:8000"
DUMMY_TOKEN = (
    "eyJhbGciOiJIUzI1NiJ9."
    "eyJpYXQiOjE2NTk2ODI4MDAsImp0aSI6IkRVTU1ZX0FOT05ZTU9VU19VU0VSZGJjNDk3MTMtMzM5ZC00Y2ZkLTkwMDgtZDY4NzlhZDQwZjE2Iiwic3ViIjoie1wiQ09ERVwiOlwiRFVNTVlfQ09ERVwiLFwiVVNFUi1JRFwiOlwiRFVNTVlfQU5PTllNT1VTX1VTRVJcIn0ifQ."
    "W2WVBrMacFrXS8Xa29k_V_yD0yca2nEet5mSYq27Ayo"
)
DATABASE = "test_db"
TABLE = "test_table"

HEADERS = {"Content-Type": "application/json", "Authorization": f"Bearer {DUMMY_TOKEN}"}


def wait_for_service():
    """Check that the tables service is reachable."""
    print("Checking tables service is up...")
    try:
        resp = requests.get(f"{SERVICE_URL}/actuator/health", timeout=5)
        resp.raise_for_status()
        print(f"  Service is up: {resp.json()}")
    except Exception as e:
        print(f"  ERROR: Tables service not reachable at {SERVICE_URL}: {e}")
        print("  Make sure you ran: ./gradlew dockerUp -Precipe=oh-only")
        sys.exit(1)


def create_table():
    """Create a test table via the REST API."""
    print(f"\nCreating table {DATABASE}.{TABLE} via REST API...")
    payload = {
        "tableId": TABLE,
        "databaseId": DATABASE,
        "baseTableVersion": "INITIAL_VERSION",
        "clusterId": "LocalFSCluster",
        "schema": json.dumps({
            "type": "struct",
            "fields": [
                {"id": 1, "required": True, "name": "id", "type": "string"},
                {"id": 2, "required": True, "name": "name", "type": "string"},
                {"id": 3, "required": True, "name": "ts", "type": "timestamp"},
            ],
        }),
        "timePartitioning": {"columnName": "ts", "granularity": "HOUR"},
        "tableProperties": {"key": "value"},
    }
    resp = requests.post(f"{SERVICE_URL}/v1/databases/{DATABASE}/tables/", headers=HEADERS, json=payload, timeout=10)
    if resp.status_code == 409:
        print(f"  Table already exists, fetching it...")
        resp = requests.get(f"{SERVICE_URL}/v1/databases/{DATABASE}/tables/{TABLE}", headers=HEADERS, timeout=10)
    resp.raise_for_status()
    table_response = resp.json()
    print(f"  tableLocation: {table_response.get('tableLocation')}")
    return table_response


def test_load_table():
    """Test OpenHouseCatalog.load_table against the local instance."""
    print(f"\nTesting OpenHouseCatalog.load_table('{DATABASE}.{TABLE}')...")
    with OpenHouseCatalog("test", uri=SERVICE_URL, auth_token=DUMMY_TOKEN) as catalog:
        table = catalog.load_table(f"{DATABASE}.{TABLE}")
        print(f"  Success!")
        print(f"  name: {table.name()}")
        print(f"  metadata_location: {table.metadata_location}")
        print(f"  schema: {table.schema()}")


def test_load_nonexistent_table():
    """Test that loading a nonexistent table raises NoSuchTableError."""
    print("\nTesting load of nonexistent table...")
    with OpenHouseCatalog("test", uri=SERVICE_URL, auth_token=DUMMY_TOKEN) as catalog:
        try:
            catalog.load_table("no_db.no_table")
            print("  FAIL: Expected NoSuchTableError")
            sys.exit(1)
        except NoSuchTableError as e:
            print(f"  Correctly raised NoSuchTableError: {e}")


def cleanup_table():
    """Delete the test table."""
    print(f"\nCleaning up table {DATABASE}.{TABLE}...")
    resp = requests.delete(f"{SERVICE_URL}/v1/databases/{DATABASE}/tables/{TABLE}", headers=HEADERS, timeout=10)
    if resp.ok:
        print("  Deleted.")
    else:
        print(f"  Delete returned {resp.status_code}: {resp.text}")


def main():
    wait_for_service()
    create_table()
    test_load_table()
    test_load_nonexistent_table()
    cleanup_table()
    print("\nAll tests passed!")


if __name__ == "__main__":
    main()
