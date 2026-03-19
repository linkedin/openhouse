"""Tests for DataLoaderSplit functionality."""

import os
import pickle
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as pq
import pytest
from datafusion.context import SessionContext
from pyiceberg.io import load_file_io
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.name_mapping import create_mapping_from_schema
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import BooleanType, DoubleType, LongType, NestedField, StringType

from openhouse.dataloader.data_loader_split import (
    DataLoaderSplit,
    TableScanContext,
    _bind_batch_table,
    to_sql_identifier,
)
from openhouse.dataloader.table_identifier import TableIdentifier
from openhouse.dataloader.udf_registry import UDFRegistry

FILE_FORMATS = pytest.mark.parametrize("file_format", [FileFormat.PARQUET, FileFormat.ORC], ids=["parquet", "orc"])

_DEFAULT_TABLE_ID = TableIdentifier("test_db", "test_tbl")


def _create_test_split(
    tmp_path,
    table: pa.Table,
    file_format: FileFormat,
    iceberg_schema: Schema,
    io_properties: dict[str, str] | None = None,
    filename: str | None = None,
    transform_sql: str | None = None,
    table_id: TableIdentifier = _DEFAULT_TABLE_ID,
    udf_registry: UDFRegistry | None = None,
) -> DataLoaderSplit:
    """Create a DataLoaderSplit for testing by writing data to disk.

    Args:
        tmp_path: Pytest temporary directory path for test files
        table: PyArrow table containing test data
        file_format: File format to use (PARQUET or ORC)
        iceberg_schema: Iceberg schema with field IDs for column mapping
        io_properties: Optional properties passed to load_file_io (e.g. DEFAULT_SCHEME, DEFAULT_NETLOC)
        filename: Optional filename override (default: test.<ext>)
        transform_sql: Optional SQL transformation to apply
        table_id: Table identifier for the scan context
        udf_registry: Optional UDF registry for transform execution

    Returns:
        DataLoaderSplit configured to read the written test file
    """
    ext = file_format.name.lower()
    file_path = str(tmp_path / (filename or f"test.{ext}"))

    properties = {}
    if file_format == FileFormat.PARQUET:
        fields = [
            field.with_metadata({b"PARQUET:field_id": str(i + 1).encode()}) for i, field in enumerate(table.schema)
        ]
        pq.write_table(table.cast(pa.schema(fields)), file_path)
    else:
        orc.write_table(table, file_path)
        nm = create_mapping_from_schema(iceberg_schema)
        properties["schema.name-mapping.default"] = nm.model_dump_json()

    metadata = new_table_metadata(
        schema=iceberg_schema,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
        location=str(tmp_path),
        properties=properties,
    )

    scan_context = TableScanContext(
        table_metadata=metadata,
        io=load_file_io(properties=io_properties or {}, location=file_path),
        projected_schema=iceberg_schema,
        table_id=table_id,
    )

    data_file = DataFile.from_args(
        file_path=file_path,
        file_format=file_format,
        record_count=table.num_rows,
        file_size_in_bytes=os.path.getsize(file_path),
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)

    return DataLoaderSplit(
        file_scan_task=task,
        scan_context=scan_context,
        transform_sql=transform_sql,
        udf_registry=udf_registry,
    )


@FILE_FORMATS
def test_split_iteration_returns_all_rows_with_correct_values(tmp_path, file_format):
    """Test that iterating a DataLoaderSplit returns all rows with correct values and types."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="value", field_type=DoubleType(), required=False),
        NestedField(field_id=4, name="flag", field_type=BooleanType(), required=False),
    )

    expected_data = {
        "id": [1, 2, 3],
        "name": ["alice", "bob", "charlie"],
        "value": [1.1, 2.2, 3.3],
        "flag": [True, False, True],
    }
    table = pa.table(
        {
            "id": pa.array(expected_data["id"], type=pa.int64()),
            "name": pa.array(expected_data["name"], type=pa.string()),
            "value": pa.array(expected_data["value"], type=pa.float64()),
            "flag": pa.array(expected_data["flag"], type=pa.bool_()),
        }
    )

    split = _create_test_split(tmp_path, table, file_format, iceberg_schema)
    batches = list(split)

    assert len(batches) >= 1, "Split iteration should return at least one batch"

    total_rows = sum(batch.num_rows for batch in batches)
    assert total_rows == len(expected_data["id"]), f"Expected {len(expected_data['id'])} rows, got {total_rows}"

    result = pa.Table.from_batches(batches)
    # Sort by ID to ensure deterministic comparison (row order is not guaranteed)
    result = result.sort_by("id")
    assert result.column("id").to_pylist() == expected_data["id"], "ID column values mismatch"
    assert result.column("name").to_pylist() == expected_data["name"], "Name column values mismatch"
    assert result.column("value").to_pylist() == expected_data["value"], "Value column values mismatch"
    assert result.column("flag").to_pylist() == expected_data["flag"], "Flag column values mismatch"


@FILE_FORMATS
def test_split_handles_wide_tables_with_many_columns(tmp_path, file_format):
    """Test that DataLoaderSplit correctly handles tables with many columns."""
    num_cols = 50
    iceberg_schema = Schema(*[NestedField(i + 1, f"col_{i}", LongType(), required=False) for i in range(num_cols)])
    data = {f"col_{i}": list(range(5)) for i in range(num_cols)}
    table = pa.table(data)

    split = _create_test_split(tmp_path, table, file_format, iceberg_schema)
    result = pa.Table.from_batches(list(split))

    assert result.num_rows == 5, f"Expected 5 rows, got {result.num_rows}"
    assert result.num_columns == num_cols, f"Expected {num_cols} columns, got {result.num_columns}"

    for i in range(num_cols):
        assert result.column(f"col_{i}").to_pylist() == list(range(5)), f"Column col_{i} values mismatch"


_ID_SCHEMA = Schema(NestedField(field_id=1, name="x", field_type=LongType(), required=False))
_ID_TABLE = pa.table({"x": pa.array([1], type=pa.int64())})


def test_split_id_differs_for_different_splits(tmp_path):
    """Different splits produce different ids."""
    split_a = _create_test_split(tmp_path, _ID_TABLE, FileFormat.PARQUET, _ID_SCHEMA, filename="a.parquet")
    split_b = _create_test_split(tmp_path, _ID_TABLE, FileFormat.PARQUET, _ID_SCHEMA, filename="b.parquet")
    assert split_a.id != split_b.id


def test_split_id_is_deterministic(tmp_path):
    """Two independently constructed splits from the same file produce the same id."""
    split_a = _create_test_split(tmp_path, _ID_TABLE, FileFormat.PARQUET, _ID_SCHEMA)
    split_b = _create_test_split(tmp_path, _ID_TABLE, FileFormat.PARQUET, _ID_SCHEMA)
    assert split_a.id == split_b.id


def test_split_id_ignores_default_netloc(tmp_path):
    """The id depends only on the file path in the manifest, not the catalog's DEFAULT_NETLOC."""
    netloc_a = "nn1.example.com:9000"
    netloc_b = "nn2.example.com:9000"
    split_a = _create_test_split(
        tmp_path,
        _ID_TABLE,
        FileFormat.PARQUET,
        _ID_SCHEMA,
        io_properties={"DEFAULT_SCHEME": "hdfs", "DEFAULT_NETLOC": netloc_a},
    )
    split_b = _create_test_split(
        tmp_path,
        _ID_TABLE,
        FileFormat.PARQUET,
        _ID_SCHEMA,
        io_properties={"DEFAULT_SCHEME": "hdfs", "DEFAULT_NETLOC": netloc_b},
    )

    assert split_a.id == split_b.id

    # Without this check, the test would pass even if DEFAULT_NETLOC was
    # silently dropped — both splits share the same file path so their ids
    # would match regardless. Spy on fs_by_scheme (where PyIceberg resolves
    # scheme + netloc into a filesystem) to confirm each netloc is used.
    local_fs = load_file_io(properties={}, location=str(tmp_path)).fs_by_scheme("file", None)
    for split, expected_netloc in [(split_a, netloc_a), (split_b, netloc_b)]:
        split._scan_context.io.fs_by_scheme = MagicMock(return_value=local_fs)
        list(split)
        split._scan_context.io.fs_by_scheme.assert_called_with("hdfs", expected_netloc)


# --- Transform tests ---

_TRANSFORM_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
)

_TABLE_ID = TableIdentifier("db", "tbl")

_MASKING_SQL = f"SELECT id, 'MASKED' as name FROM {to_sql_identifier(_TABLE_ID)}"


def _make_transform_split(tmp_path, table, transform_sql, table_id=_TABLE_ID):
    """Create a DataLoaderSplit with a transform SQL string for testing."""
    return _create_test_split(
        tmp_path,
        table,
        FileFormat.PARQUET,
        _TRANSFORM_SCHEMA,
        transform_sql=transform_sql,
        table_id=table_id,
    )


class _CountingRegistry(UDFRegistry):
    def __init__(self):
        self.calls = 0

    def register_udfs(self, session_context: SessionContext) -> None:
        self.calls += 1


def test_split_with_transformer_transforms_batches(tmp_path):
    """A transformer that masks a column is applied to each batch."""
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["alice", "bob"], type=pa.string()),
        }
    )

    split = _make_transform_split(tmp_path, table, _MASKING_SQL)
    result = pa.Table.from_batches(list(split))

    assert result.num_rows == 2
    assert result.column("id").to_pylist() == [1, 2]
    assert result.column("name").to_pylist() == ["MASKED", "MASKED"]


def test_split_with_transformer_and_empty_batches(tmp_path):
    """An empty batch with a transformer yields no rows."""
    table = pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "name": pa.array([], type=pa.string()),
        }
    )

    split = _make_transform_split(tmp_path, table, _MASKING_SQL)
    batches = list(split)
    total_rows = sum(b.num_rows for b in batches)
    assert total_rows == 0


def test_bind_batch_table_rebinds_each_batch():
    """Batch binding always deregisters before registering to avoid collisions."""
    session = MagicMock(spec=SessionContext)
    batch = MagicMock(spec=pa.RecordBatch)

    _bind_batch_table(session, _TABLE_ID, batch)

    session.deregister_table.assert_called_once_with(to_sql_identifier(_TABLE_ID))
    session.register_record_batches.assert_called_once_with(to_sql_identifier(_TABLE_ID), [[batch]])


def test_split_transform_creates_one_session_and_applies_transform(tmp_path, monkeypatch):
    """Transform path creates one DataFusion session with registered UDFs via the TableProvider."""
    table = pa.table(
        {
            "id": pa.array([1], type=pa.int64()),
            "name": pa.array(["alice"], type=pa.string()),
        }
    )
    registry = _CountingRegistry()
    split = _create_test_split(
        tmp_path,
        table,
        FileFormat.PARQUET,
        _TRANSFORM_SCHEMA,
        transform_sql=_MASKING_SQL,
        table_id=_TABLE_ID,
        udf_registry=registry,
    )

    batch_one = pa.record_batch({"id": pa.array([1], type=pa.int64()), "name": pa.array(["alice"], type=pa.string())})
    batch_two = pa.record_batch({"id": pa.array([2], type=pa.int64()), "name": pa.array(["bob"], type=pa.string())})

    def _fake_to_record_batches(self, scan_tasks, **kwargs):
        return iter([batch_one, batch_two])

    monkeypatch.setattr(
        "openhouse.dataloader._iceberg_scan_delegate.ArrowScan.to_record_batches", _fake_to_record_batches
    )

    result = pa.Table.from_batches(list(split)).sort_by("id")

    assert registry.calls == 1
    assert result.column("id").to_pylist() == [1, 2]
    assert result.column("name").to_pylist() == ["MASKED", "MASKED"]


# --- Pickle tests ---


def test_pickle_round_trip_no_plan(tmp_path):
    """A split without a transform survives pickle round-trip."""
    split = _create_test_split(tmp_path, _ID_TABLE, FileFormat.PARQUET, _ID_SCHEMA)
    restored = pickle.loads(pickle.dumps(split))

    result = pa.Table.from_batches(list(restored))
    assert result.column("x").to_pylist() == [1]


def test_pickle_round_trip_with_transform(tmp_path):
    """A split with transform SQL survives pickle round-trip."""
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["alice", "bob"], type=pa.string()),
        }
    )

    split = _make_transform_split(tmp_path, table, _MASKING_SQL)
    restored = pickle.loads(pickle.dumps(split))

    result = pa.Table.from_batches(list(restored))
    assert result.num_rows == 2
    assert result.column("name").to_pylist() == ["MASKED", "MASKED"]


def test_pickle_double_round_trip(tmp_path):
    """A split survives two pickle round-trips."""
    table = pa.table(
        {
            "id": pa.array([1], type=pa.int64()),
            "name": pa.array(["alice"], type=pa.string()),
        }
    )

    split = _make_transform_split(tmp_path, table, _MASKING_SQL)
    restored = pickle.loads(pickle.dumps(pickle.loads(pickle.dumps(split))))

    result = pa.Table.from_batches(list(restored))
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["MASKED"]


# --- Identifier escaping tests ---


def test_to_sql_identifier_escapes_double_quotes():
    """to_sql_identifier escapes embedded double quotes."""
    table_id = TableIdentifier('my"db', 'my"tbl')
    assert to_sql_identifier(table_id) == '"my""db"."my""tbl"'


def test_transform_with_quoted_identifier(tmp_path):
    """A transform works when the table identifier contains characters that need escaping."""
    table_id = TableIdentifier('test"db', "tbl")
    sql = f"SELECT id, 'MASKED' as name FROM {to_sql_identifier(table_id)}"
    table = pa.table(
        {
            "id": pa.array([1], type=pa.int64()),
            "name": pa.array(["alice"], type=pa.string()),
        }
    )

    split = _make_transform_split(tmp_path, table, sql, table_id=table_id)
    result = pa.Table.from_batches(list(split))

    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["MASKED"]
