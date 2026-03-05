"""Integration tests for OpenHouseCatalog against a running OpenHouse instance.

Tables are created and populated via Spark SQL submitted through Livy's REST API.
Data lives in HDFS, so these tests run inside a Docker container on the same
network as the oh-hadoop-spark Docker Compose services.
"""

import logging
import os
import sys
import time

import pyarrow as pa
import pytest
import requests
from pyiceberg.exceptions import NoSuchTableError

from openhouse.dataloader import OpenHouseDataLoader
from openhouse.dataloader.catalog import OpenHouseCatalog
from openhouse.dataloader.filters import col

BASE_URL = "http://openhouse-tables:8080"
LIVY_URL = "http://spark-livy:8998"
HDFS_NETLOC = "namenode:9000"
DATABASE_ID = "d_e2e"
TABLE_ID = "t_itest"

COL_ID = "id"
COL_NAME = "name"
COL_SCORE = "score"

CREATE_COLUMNS = f"{COL_ID} BIGINT, {COL_NAME} STRING, {COL_SCORE} DOUBLE"

SPARK_CONF = {
    "spark.jars": "local:/opt/spark/openhouse-spark-runtime_2.12-latest-all.jar",
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.2.0",
    "spark.sql.extensions": (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions"
    ),
    "spark.sql.catalog.openhouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.openhouse.catalog-impl": "com.linkedin.openhouse.spark.OpenHouseCatalog",
    "spark.sql.catalog.openhouse.uri": BASE_URL,
    "spark.sql.catalog.openhouse.cluster": "LocalHadoopCluster",
}

HEADERS = {"Content-Type": "application/json"}

FQTN = f"openhouse.{DATABASE_ID}.{TABLE_ID}"


class LivySession:
    """Manages a Livy SQL session for executing Spark SQL statements."""

    def __init__(self, livy_url: str, auth_token: str) -> None:
        self._livy_url = livy_url
        conf = {**SPARK_CONF, "spark.sql.catalog.openhouse.auth-token": auth_token}
        data = {"kind": "sql", "conf": conf}
        response = requests.post(f"{livy_url}/sessions", json=data, headers=HEADERS)
        assert response.status_code == 201, f"Session creation failed: {response.status_code} {response.text}"
        self._session_url = livy_url + response.headers["location"]
        self._wait_for_idle()

    def _wait_for_idle(self) -> None:
        while True:
            resp = requests.get(self._session_url, headers=HEADERS)
            state = resp.json()["state"]
            if state == "idle":
                return
            if state in ("dead", "shutting_down", "error", "killed"):
                raise RuntimeError(f"Livy session entered state: {state}")
            time.sleep(2)

    def execute(self, sql: str) -> None:
        """Submit a SQL statement and wait for completion. Raises on error."""
        print(f"  SQL: {sql}")
        resp = requests.post(f"{self._session_url}/statements", json={"code": sql}, headers=HEADERS)
        assert resp.status_code == 201, f"Statement submit failed: {resp.status_code} {resp.text}"
        stmt_url = self._livy_url + resp.headers["location"]

        while True:
            resp = requests.get(stmt_url, headers=HEADERS)
            state = resp.json()["state"]
            if state == "available":
                output = resp.json()["output"]
                if output["status"] == "error":
                    raise RuntimeError(f"SQL failed: {output.get('evalue', output)}")
                return
            if state in ("error", "cancelled"):
                raise RuntimeError(f"Statement entered state: {state}")
            time.sleep(1)

    def close(self) -> None:
        requests.delete(self._session_url, headers=HEADERS)


def _read_all(loader: OpenHouseDataLoader) -> pa.Table:
    """Read all data from a DataLoader and return as a sorted PyArrow table."""
    batches = [batch for split in loader for batch in split]
    if not batches:
        return pa.table({})
    return pa.concat_tables([pa.Table.from_batches([b]) for b in batches]).sort_by(COL_ID)


def read_token() -> str:
    """Read auth token from OH_TOKEN env var or file argument."""
    token = os.environ.get("OH_TOKEN")
    if token:
        return token.strip()
    if len(sys.argv) >= 2:
        try:
            with open(sys.argv[1]) as f:
                return f.read().strip()
        except FileNotFoundError:
            print(f"Token file not found: {sys.argv[1]}")
            sys.exit(1)
    print("Usage: set OH_TOKEN env var or pass token file as argument")
    sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    token_str = read_token()
    catalog = OpenHouseCatalog(
        name="integration-test",
        uri=BASE_URL,
        auth_token=token_str,
        properties={"DEFAULT_SCHEME": "hdfs", "DEFAULT_NETLOC": HDFS_NETLOC},
    )

    livy = LivySession(LIVY_URL, token_str)
    try:
        # 1. Nonexistent table raises NoSuchTableError
        with pytest.raises(NoSuchTableError):
            loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table="nonexistent_table")
            _read_all(loader)
        print("PASS: nonexistent table raised NoSuchTableError")

        # 2. Empty table returns no splits
        livy.execute(f"CREATE TABLE {FQTN} ({CREATE_COLUMNS}) USING iceberg")
        try:
            loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID)
            assert list(loader) == [], "Expected no splits for empty table"
            assert loader.snapshot_id is None, "Expected no snapshot for empty table"
            print("PASS: empty table returned no splits")

            # 3. Write data via Spark
            livy.execute(f"INSERT INTO {FQTN} VALUES (1, 'alice', 1.1), (2, 'bob', 2.2), (3, 'charlie', 3.3)")
            snap1 = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID).snapshot_id
            assert snap1 is not None

            # 4. Read all data and verify
            loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID)
            result = _read_all(loader)
            assert result.num_rows == 3
            assert result.column(COL_ID).to_pylist() == [1, 2, 3]
            assert result.column(COL_NAME).to_pylist() == ["alice", "bob", "charlie"]
            assert result.column(COL_SCORE).to_pylist() == [1.1, 2.2, 3.3]
            print(f"PASS: read all {result.num_rows} rows")

            # 5a. Row filter
            loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID, filters=col(COL_ID) > 1)
            result = _read_all(loader)
            assert result.num_rows == 2
            assert result.column(COL_ID).to_pylist() == [2, 3]
            assert result.column(COL_NAME).to_pylist() == ["bob", "charlie"]
            assert result.column(COL_SCORE).to_pylist() == [2.2, 3.3]
            print(f"PASS: row filter returned {result.num_rows} rows")

            # 5b. Column projection
            loader = OpenHouseDataLoader(
                catalog=catalog, database=DATABASE_ID, table=TABLE_ID, columns=[COL_ID, COL_NAME]
            )
            result = _read_all(loader)
            assert result.column_names == [COL_ID, COL_NAME]
            assert result.num_rows == 3
            assert result.column(COL_ID).to_pylist() == [1, 2, 3]
            assert result.column(COL_NAME).to_pylist() == ["alice", "bob", "charlie"]
            print(f"PASS: column projection returned columns {result.column_names}")

            # 6. Write a second snapshot and verify the new data is read
            livy.execute(f"INSERT INTO {FQTN} VALUES (4, 'diana', 4.4)")
            snap2 = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID).snapshot_id
            assert snap2 is not None
            assert snap2 != snap1

            loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID)
            result = _read_all(loader)
            assert result.num_rows == 4
            assert result.column(COL_ID).to_pylist() == [1, 2, 3, 4]
            assert result.column(COL_NAME).to_pylist() == ["alice", "bob", "charlie", "diana"]
            assert result.column(COL_SCORE).to_pylist() == [1.1, 2.2, 3.3, 4.4]
            print(f"PASS: after second insert, read all {result.num_rows} rows")

            # 7. Pin to the old snapshot and verify only the original data is returned
            loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID, snapshot_id=snap1)
            result = _read_all(loader)
            assert result.num_rows == 3
            assert result.column(COL_ID).to_pylist() == [1, 2, 3]
            assert result.column(COL_NAME).to_pylist() == ["alice", "bob", "charlie"]
            assert result.column(COL_SCORE).to_pylist() == [1.1, 2.2, 3.3]
            print(f"PASS: pinned to snap1, read {result.num_rows} rows (excluded snap2 data)")

            # Verify invalid snapshot raises
            with pytest.raises(ValueError, match="Snapshot .* not found"):
                loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID, snapshot_id=-1)
                list(loader)
            print("PASS: invalid snapshot_id raised ValueError")

        finally:
            livy.execute(f"DROP TABLE IF EXISTS {FQTN}")

        print("All integration tests passed")
    finally:
        livy.close()
