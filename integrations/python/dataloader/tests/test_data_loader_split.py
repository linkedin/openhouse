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

from openhouse.dataloader.data_loader_split import DataLoaderSplit, TableScanContext

FILE_FORMATS = pytest.mark.parametrize("file_format", [FileFormat.PARQUET, FileFormat.ORC], ids=["parquet", "orc"])


def _create_test_split(
    tmp_path,
    table: pa.Table,
    file_format: FileFormat,
    iceberg_schema: Schema,
    io_properties: dict[str, str] | None = None,
    filename: str | None = None,
) -> DataLoaderSplit:
    """Create a DataLoaderSplit for testing by writing data to disk.

    Args:
        tmp_path: Pytest temporary directory path for test files
        table: PyArrow table containing test data
        file_format: File format to use (PARQUET or ORC)
        iceberg_schema: Iceberg schema with field IDs for column mapping
        io_properties: Optional properties passed to load_file_io (e.g. DEFAULT_SCHEME, DEFAULT_NETLOC)
        filename: Optional filename override (default: test.<ext>)

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
    )

    ctx = SessionContext()
    ctx.register_record_batches("test_table", [table.to_batches()])
    plan = ctx.sql("SELECT * FROM test_table").logical_plan()

    data_file = DataFile.from_args(
        file_path=file_path,
        file_format=file_format,
        record_count=table.num_rows,
        file_size_in_bytes=os.path.getsize(file_path),
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)

    return DataLoaderSplit(
        plan=plan,
        session_context=ctx,
        file_scan_task=task,
        scan_context=scan_context,
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


@FILE_FORMATS
def test_split_is_picklable_and_yields_correct_data(tmp_path, file_format):
    """Test that DataLoaderSplit can be pickled/unpickled and still yields correct data."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )

    expected_data = {
        "id": [1, 2, 3],
        "name": ["alice", "bob", "charlie"],
    }
    table = pa.table(
        {
            "id": pa.array(expected_data["id"], type=pa.int64()),
            "name": pa.array(expected_data["name"], type=pa.string()),
        }
    )

    split = _create_test_split(tmp_path, table, file_format, iceberg_schema)
    assert not hasattr(split, "_plan"), "Plan should not be stored as an instance field"
    assert not hasattr(split, "_session_context"), "Session context should not be stored as an instance field"
    assert isinstance(split._plan_substrait_bytes, bytes), "Substrait bytes should be eagerly serialized in __init__"
    assert len(split._plan_substrait_bytes) > 0, "Substrait bytes should be non-empty"

    restored = pickle.loads(pickle.dumps(split))

    assert not hasattr(restored, "_plan"), "Plan should not exist after unpickling"
    assert not hasattr(restored, "_session_context"), "Session context should not exist after unpickling"
    assert restored._plan_substrait_bytes == split._plan_substrait_bytes, "Substrait bytes mismatch after round-trip"

    result = pa.Table.from_batches(list(restored)).sort_by("id")
    assert result.column("id").to_pylist() == expected_data["id"]
    assert result.column("name").to_pylist() == expected_data["name"]


@FILE_FORMATS
def test_split_pickle_double_round_trip(tmp_path, file_format):
    """Substrait bytes survive two consecutive pickle round-trips."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    )
    table = pa.table({"id": pa.array([1, 2], type=pa.int64())})

    split = _create_test_split(tmp_path, table, file_format, iceberg_schema)
    restored_once = pickle.loads(pickle.dumps(split))
    restored_twice = pickle.loads(pickle.dumps(restored_once))

    assert restored_twice._plan_substrait_bytes == split._plan_substrait_bytes


def test_split_pickle_without_plan(tmp_path):
    """A split constructed without a plan pickles and iterates correctly."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    )
    table = pa.table({"id": pa.array([1], type=pa.int64())})
    file_path = str(tmp_path / "test.parquet")
    fields = [field.with_metadata({b"PARQUET:field_id": b"1"}) for field in table.schema]
    pq.write_table(table.cast(pa.schema(fields)), file_path)

    metadata = new_table_metadata(
        schema=iceberg_schema,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
        location=str(tmp_path),
        properties={},
    )
    scan_context = TableScanContext(
        table_metadata=metadata,
        io=load_file_io(properties={}, location=file_path),
        projected_schema=iceberg_schema,
    )
    data_file = DataFile.from_args(
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        record_count=1,
        file_size_in_bytes=os.path.getsize(file_path),
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)

    split = DataLoaderSplit(file_scan_task=task, scan_context=scan_context)
    assert split._plan_substrait_bytes is None

    restored = pickle.loads(pickle.dumps(split))
    assert restored._plan_substrait_bytes is None

    result = pa.Table.from_batches(list(restored))
    assert result.column("id").to_pylist() == [1]


def test_split_plan_without_session_context_raises():
    """Passing plan without session_context raises ValueError."""
    mock_plan = MagicMock()
    mock_task = MagicMock()
    mock_ctx = MagicMock()

    with pytest.raises(ValueError, match="plan and session_context must both be provided or both be None"):
        DataLoaderSplit(file_scan_task=mock_task, scan_context=mock_ctx, plan=mock_plan)
