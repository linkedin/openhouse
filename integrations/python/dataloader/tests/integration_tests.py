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
DATABASE_ID = "d_e2e"
TABLE_ID_EMPTY = "t_empty"
TABLE_ID_DATA = "t_data"
TABLE_ID_SNAPSHOT = "t_snapshot"

COL_ID = "id"
COL_NAME = "name"
COL_SCORE = "score"

CREATE_COLUMNS = f"{COL_ID} BIGINT, {COL_NAME} STRING, {COL_SCORE} DOUBLE"

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


def _fqtn(table_id: str) -> str:
    return f"openhouse.{DATABASE_ID}.{table_id}"


def test_table_with_data(catalog: OpenHouseCatalog) -> None:
    """Load a table with data. Check the data, table properties, and snapshot ID."""
    loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID_DATA)

    assert loader.table_properties.get("write.format.default") == "parquet"
    print("Loader table_properties verified: write.format.default=parquet")

    assert loader.snapshot_id is not None, "Expected a snapshot ID for a table with data"
    print(f"Loader snapshot_id verified: {loader.snapshot_id}")

    batches = [batch for split in loader for batch in split]
    assert len(batches) > 0, "Expected at least one batch"

    result = pa.concat_tables([pa.Table.from_batches([b]) for b in batches])
    result = result.sort_by(COL_ID)

    assert result.num_rows == EXPECTED_DATA.num_rows, f"Expected {EXPECTED_DATA.num_rows} rows, got {result.num_rows}"
    assert result.column(COL_ID).to_pylist() == EXPECTED_DATA.column(COL_ID).to_pylist()
    assert result.column(COL_NAME).to_pylist() == EXPECTED_DATA.column(COL_NAME).to_pylist()
    assert result.column(COL_SCORE).to_pylist() == EXPECTED_DATA.column(COL_SCORE).to_pylist()
    print(f"DataLoader read {result.num_rows} rows with correct values")


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
    """Load a table without data. Check splits are empty, table properties, and snapshot ID."""
    loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID_EMPTY)

    assert loader.table_properties.get("myProp") == "hello"
    print("Loader table_properties verified: myProp=hello")

    assert loader.snapshot_id is None, f"Expected no snapshot ID for empty table, got {loader.snapshot_id}"
    print("Loader snapshot_id verified: None (empty table)")

    splits = list(loader)
    assert splits == [], f"Expected no splits, got {len(splits)}"
    print("DataLoader correctly yielded no splits for empty table")


def test_snapshot_id_returns_data_at_snapshot(catalog: OpenHouseCatalog, snap1: int, snap2: int) -> None:
    """Load with snapshot_id=snap1 returns only the first batch of data."""
    loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID_SNAPSHOT, snapshot_id=snap1)
    batches = [batch for split in loader for batch in split]
    result = pa.concat_tables([pa.Table.from_batches([b]) for b in batches]).sort_by(COL_ID)
    assert result.num_rows == 2, f"Expected 2 rows at snapshot 1, got {result.num_rows}"
    assert result.column(COL_ID).to_pylist() == [1, 2]
    print(f"snapshot_id={snap1} correctly returned {result.num_rows} rows (batch 1 only)")

    loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID_SNAPSHOT, snapshot_id=snap2)
    batches = [batch for split in loader for batch in split]
    result = pa.concat_tables([pa.Table.from_batches([b]) for b in batches]).sort_by(COL_ID)
    assert result.num_rows == 4, f"Expected 4 rows at snapshot 2, got {result.num_rows}"
    assert result.column(COL_ID).to_pylist() == [1, 2, 3, 4]
    print(f"snapshot_id={snap2} correctly returned {result.num_rows} rows (both batches)")


def test_snapshot_id_with_filters(catalog: OpenHouseCatalog, snap2: int) -> None:
    """snapshot_id works alongside row filters."""
    loader = OpenHouseDataLoader(
        catalog=catalog, database=DATABASE_ID, table=TABLE_ID_SNAPSHOT, snapshot_id=snap2, filters=col(COL_ID) > 2
    )
    batches = [batch for split in loader for batch in split]
    result = pa.concat_tables([pa.Table.from_batches([b]) for b in batches]).sort_by(COL_ID)
    assert result.num_rows == 2, f"Expected 2 filtered rows at snapshot 2, got {result.num_rows}"
    assert result.column(COL_ID).to_pylist() == [3, 4]
    print(f"snapshot_id={snap2} with filter correctly returned {result.num_rows} rows")


def test_snapshot_id_invalid(catalog: OpenHouseCatalog) -> None:
    """Loading with a non-existent snapshot_id raises an error."""
    with pytest.raises(ValueError, match="Snapshot .* not found"):
        loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID_SNAPSHOT, snapshot_id=-1)
        list(loader)
    print("Invalid snapshot_id correctly raised ValueError")


def test_nonexistent_table(catalog: OpenHouseCatalog) -> None:
    """Check that loading a nonexistent table raises NoSuchTableError."""
    with pytest.raises(NoSuchTableError):
        catalog.load_table(f"{DATABASE_ID}.nonexistent_table")
    print("load_table correctly raised NoSuchTableError for nonexistent table")


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
    catalog = OpenHouseCatalog(name="integration-test", uri=BASE_URL, auth_token=token_str)

    livy = LivySession(LIVY_URL, token_str)
    try:
        # --- Table with data ---
        try:
            livy.execute(
                f"CREATE TABLE {_fqtn(TABLE_ID_DATA)} ({CREATE_COLUMNS}) "
                "USING iceberg TBLPROPERTIES ('write.format.default'='parquet')"
            )
            livy.execute(
                f"INSERT INTO {_fqtn(TABLE_ID_DATA)} VALUES (1, 'alice', 1.1), (2, 'bob', 2.2), (3, 'charlie', 3.3)"
            )
            test_table_with_data(catalog)
            test_table_with_data_row_filter(catalog)
            test_table_with_data_selected_columns(catalog)
        finally:
            livy.execute(f"DROP TABLE IF EXISTS {_fqtn(TABLE_ID_DATA)}")

        # --- Snapshot ID ---
        try:
            livy.execute(
                f"CREATE TABLE {_fqtn(TABLE_ID_SNAPSHOT)} ({CREATE_COLUMNS}) "
                "USING iceberg TBLPROPERTIES ('write.format.default'='parquet')"
            )
            livy.execute(f"INSERT INTO {_fqtn(TABLE_ID_SNAPSHOT)} VALUES (1, 'alice', 1.1), (2, 'bob', 2.2)")
            snap1 = catalog.load_table(f"{DATABASE_ID}.{TABLE_ID_SNAPSHOT}").metadata.current_snapshot_id

            livy.execute(f"INSERT INTO {_fqtn(TABLE_ID_SNAPSHOT)} VALUES (3, 'charlie', 3.3), (4, 'diana', 4.4)")
            snap2 = catalog.load_table(f"{DATABASE_ID}.{TABLE_ID_SNAPSHOT}").metadata.current_snapshot_id

            assert snap1 != snap2, f"Expected different snapshot IDs, got {snap1} for both"
            print(f"Snapshot IDs: snap1={snap1}, snap2={snap2}")

            test_snapshot_id_returns_data_at_snapshot(catalog, snap1, snap2)
            test_snapshot_id_with_filters(catalog, snap2)
            test_snapshot_id_invalid(catalog)
        finally:
            livy.execute(f"DROP TABLE IF EXISTS {_fqtn(TABLE_ID_SNAPSHOT)}")

        # --- Empty table ---
        try:
            livy.execute(
                f"CREATE TABLE {_fqtn(TABLE_ID_EMPTY)} ({CREATE_COLUMNS}) "
                "USING iceberg TBLPROPERTIES ('myProp'='hello')"
            )
            test_empty_table(catalog)
        finally:
            livy.execute(f"DROP TABLE IF EXISTS {_fqtn(TABLE_ID_EMPTY)}")

        # --- Nonexistent table ---
        test_nonexistent_table(catalog)

        print("All integration tests passed")
    finally:
        livy.close()
