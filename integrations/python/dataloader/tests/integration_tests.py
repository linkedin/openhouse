"""Integration tests for OpenHouseCatalog against a running OpenHouse instance.

Tables are created and populated via Spark SQL submitted through Livy's REST API.
Data lives in HDFS, so these tests run inside a Docker container on the same
network as the oh-hadoop-spark Docker Compose services.
"""

import logging
import multiprocessing
import os
import sys
import tempfile
import time

import pyarrow as pa
import pytest
import requests
from pyiceberg.exceptions import NoSuchTableError

from openhouse.dataloader import DataLoaderContext, JvmConfig, OpenHouseDataLoader
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
REQUEST_TIMEOUT = 30
SESSION_TIMEOUT = 300
STATEMENT_TIMEOUT = 300

FQTN = f"openhouse.{DATABASE_ID}.{TABLE_ID}"


class LivySession:
    """Manages a Livy SQL session for executing Spark SQL statements."""

    def __init__(self, livy_url: str, auth_token: str) -> None:
        self._livy_url = livy_url
        conf = {**SPARK_CONF, "spark.sql.catalog.openhouse.auth-token": auth_token}
        data = {"kind": "sql", "conf": conf}
        response = requests.post(f"{livy_url}/sessions", json=data, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        assert response.status_code == 201, f"Session creation failed: {response.status_code} {response.text}"
        self._session_url = livy_url + response.headers["location"]
        self._wait_for_idle()

    def _wait_for_idle(self) -> None:
        deadline = time.monotonic() + SESSION_TIMEOUT
        while True:
            if time.monotonic() > deadline:
                raise RuntimeError(f"Livy session not idle after {SESSION_TIMEOUT}s")
            resp = requests.get(self._session_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            state = resp.json()["state"]
            if state == "idle":
                return
            if state in ("dead", "shutting_down", "error", "killed"):
                raise RuntimeError(f"Livy session entered state: {state}")
            time.sleep(2)

    def execute(self, sql: str) -> None:
        """Submit a SQL statement and wait for completion. Raises on error."""
        print(f"  SQL: {sql}")
        resp = requests.post(
            f"{self._session_url}/statements", json={"code": sql}, headers=HEADERS, timeout=REQUEST_TIMEOUT
        )
        assert resp.status_code == 201, f"Statement submit failed: {resp.status_code} {resp.text}"
        stmt_url = self._livy_url + resp.headers["location"]

        deadline = time.monotonic() + STATEMENT_TIMEOUT
        while True:
            if time.monotonic() > deadline:
                raise RuntimeError(f"SQL statement not complete after {STATEMENT_TIMEOUT}s: {sql}")
            resp = requests.get(stmt_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
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
        requests.delete(self._session_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)


def _parse_max_heap_bytes(jvm_output: str) -> int:
    """Extract MaxHeapSize value in bytes from -XX:+PrintFlagsFinal output."""
    for line in jvm_output.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[1] == "MaxHeapSize":
            return int(parts[3])
    raise ValueError("MaxHeapSize not found in JVM output")


def _assert_jvm_heap(log_path: str, requested_mb: int, upper_bound_mb: int, label: str) -> int:
    """Read a JVM flags log file, assert MaxHeapSize <= upper_bound, and return the actual value."""
    with open(log_path) as f:
        output = f.read()
    os.unlink(log_path)
    assert "MaxHeapSize" in output, f"{label} JVM did not print flags — jvm_args not honored"
    heap = _parse_max_heap_bytes(output)
    assert heap <= upper_bound_mb * 1024 * 1024, (
        f"{label} MaxHeapSize {heap} exceeds {upper_bound_mb}m — -Xmx{requested_mb}m not honored"
    )
    return heap


def _materialize_split_in_child(split, jvm_log_path):
    """Materialize a single split in this process, capturing stdout+stderr to *jvm_log_path*.

    Intended to run via multiprocessing so the child gets a fresh JVM that
    picks up worker_jvm_args from LIBHDFS_OPTS.
    """
    saved_stdout = os.dup(1)
    saved_stderr = os.dup(2)
    log_fd = os.open(jvm_log_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.dup2(log_fd, 1)
    os.dup2(log_fd, 2)
    os.close(log_fd)
    try:
        batches = list(split)
        num_rows = sum(b.num_rows for b in batches)
    finally:
        os.dup2(saved_stdout, 1)
        os.close(saved_stdout)
        os.dup2(saved_stderr, 2)
        os.close(saved_stderr)
    print(f"  child process read {num_rows} rows from split")


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

    # Set jvm_args before any DataLoader is created so LIBHDFS_OPTS is in
    # place when the JVM starts.  We capture both stdout and stderr to a
    # log file because -XX:+PrintFlagsFinal may write to either fd.
    jvm_log_fd, jvm_log = tempfile.mkstemp(suffix=".log")
    os.close(jvm_log_fd)
    ctx = DataLoaderContext(jvm_config=JvmConfig(planner_args="-Xmx127m -XX:+PrintFlagsFinal"))

    livy = LivySession(LIVY_URL, token_str)
    try:
        # 1. Nonexistent table raises NoSuchTableError
        with pytest.raises(NoSuchTableError):
            loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table="nonexistent_table", context=ctx)
            _read_all(loader)
        print("PASS: nonexistent table raised NoSuchTableError")

        # 2. Empty table returns no splits and custom properties are accessible
        livy.execute(
            f"CREATE TABLE {FQTN} ({CREATE_COLUMNS}) USING iceberg TBLPROPERTIES ('itest.custom-key' = 'custom-value')"
        )
        try:
            # Capture stdout+stderr from here through the first HDFS read
            # so we can verify -XX:+PrintFlagsFinal output at the end.
            # The JVM starts during the first table load and prints flags then.
            saved_stdout = os.dup(1)
            saved_stderr = os.dup(2)
            log_fd = os.open(jvm_log, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            os.dup2(log_fd, 1)
            os.dup2(log_fd, 2)
            os.close(log_fd)
            try:
                loader = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID)
                assert list(loader) == [], "Expected no splits for empty table"
                assert loader.snapshot_id is None, "Expected no snapshot for empty table"
                assert loader.table_properties.get("itest.custom-key") == "custom-value"

                # 3. Write data via Spark
                livy.execute(f"INSERT INTO {FQTN} VALUES (1, 'alice', 1.1), (2, 'bob', 2.2), (3, 'charlie', 3.3)")
                snap1 = OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID).snapshot_id
                assert snap1 is not None

                # 4. Read all data
                result = _read_all(OpenHouseDataLoader(catalog=catalog, database=DATABASE_ID, table=TABLE_ID))
            finally:
                os.dup2(saved_stdout, 1)
                os.close(saved_stdout)
                os.dup2(saved_stderr, 2)
                os.close(saved_stderr)
            print("PASS: empty table returned no splits and custom property is accessible")
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

            # 8. Materialize a split in a child process with worker_jvm_args.
            #    The child gets a fresh JVM, so -Xmx254m takes effect there
            #    independently of the planner's -Xmx127m.
            worker_ctx = DataLoaderContext(jvm_config=JvmConfig(worker_args="-Xmx254m -XX:+PrintFlagsFinal"))
            worker_loader = OpenHouseDataLoader(
                catalog=catalog, database=DATABASE_ID, table=TABLE_ID, context=worker_ctx
            )
            splits = list(worker_loader)
            assert splits, "Expected at least one split"
            worker_jvm_log_fd, worker_jvm_log = tempfile.mkstemp(suffix=".log")
            os.close(worker_jvm_log_fd)
            spawn_ctx = multiprocessing.get_context("spawn")
            proc = spawn_ctx.Process(target=_materialize_split_in_child, args=(splits[0], worker_jvm_log))
            proc.start()
            proc.join(timeout=120)
            assert proc.exitcode == 0, f"Child process failed with exit code {proc.exitcode}"
            print("PASS: worker_jvm_args split materialized in child process")

        finally:
            livy.execute(f"DROP TABLE IF EXISTS {FQTN}")

        # Verify planner and worker jvm_args were honored by their respective JVMs
        planner_heap = _assert_jvm_heap(jvm_log, requested_mb=127, upper_bound_mb=128, label="Planner")
        print(f"PASS: planner_jvm_args honored by JVM (MaxHeapSize={planner_heap})")
        worker_heap = _assert_jvm_heap(worker_jvm_log, requested_mb=254, upper_bound_mb=256, label="Worker")
        assert worker_heap > planner_heap, (
            f"Worker MaxHeapSize ({worker_heap}) should be larger than planner ({planner_heap})"
        )
        print(f"PASS: worker_jvm_args honored by child JVM (MaxHeapSize={worker_heap})")

        print("All integration tests passed")
    finally:
        livy.close()
