"""Tests for DataLoaderSplit functionality."""

import hashlib
import os

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
) -> DataLoaderSplit:
    """Create a DataLoaderSplit for testing by writing data to disk.

    Args:
        tmp_path: Pytest temporary directory path for test files
        table: PyArrow table containing test data
        file_format: File format to use (PARQUET or ORC)
        iceberg_schema: Iceberg schema with field IDs for column mapping

    Returns:
        DataLoaderSplit configured to read the written test file
    """
    ext = file_format.name.lower()
    file_path = str(tmp_path / f"test.{ext}")

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
        io=load_file_io(properties={}, location=file_path),
        projected_schema=iceberg_schema,
    )

    ctx = SessionContext()
    plan = ctx.sql("SELECT 1 as a").logical_plan()

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


def _make_split_with_path(file_path: str) -> DataLoaderSplit:
    """Create a minimal DataLoaderSplit with the given file path (no real file needed)."""
    iceberg_schema = Schema(NestedField(field_id=1, name="x", field_type=LongType(), required=False))
    metadata = new_table_metadata(
        schema=iceberg_schema,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
        location="/tmp",
    )
    scan_context = TableScanContext(
        table_metadata=metadata,
        io=load_file_io(properties={}, location=file_path),
        projected_schema=iceberg_schema,
    )
    data_file = DataFile.from_args(
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        record_count=0,
        file_size_in_bytes=0,
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)
    return DataLoaderSplit(file_scan_task=task, scan_context=scan_context)


def test_split_id_is_deterministic():
    """Two splits with the same file path produce the same id."""
    path = "s3://bucket/warehouse/db/table/data/00001.parquet"
    assert _make_split_with_path(path).id == _make_split_with_path(path).id


def test_split_id_differs_for_different_paths():
    """Two splits with different file paths produce different ids."""
    id_a = _make_split_with_path("s3://bucket/warehouse/db/table/data/00001.parquet").id
    id_b = _make_split_with_path("s3://bucket/warehouse/db/table/data/00002.parquet").id
    assert id_a != id_b


def test_split_id_ignores_default_netloc():
    """The id depends only on the file path in the manifest, not the catalog's DEFAULT_NETLOC."""
    schemeless_path = "/warehouse/db/table/data/00001.parquet"
    iceberg_schema = Schema(NestedField(field_id=1, name="x", field_type=LongType(), required=False))
    metadata = new_table_metadata(
        schema=iceberg_schema,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
        location="/tmp",
    )
    data_file = DataFile.from_args(
        file_path=schemeless_path,
        file_format=FileFormat.PARQUET,
        record_count=0,
        file_size_in_bytes=0,
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)

    ids = []
    for netloc in ["nn1.example.com:9000", "nn2.example.com:9000"]:
        scan_context = TableScanContext(
            table_metadata=metadata,
            io=load_file_io(
                properties={"DEFAULT_SCHEME": "hdfs", "DEFAULT_NETLOC": netloc},
                location=schemeless_path,
            ),
            projected_schema=iceberg_schema,
        )
        split = DataLoaderSplit(file_scan_task=task, scan_context=scan_context)
        ids.append(split.id)

    assert ids[0] == ids[1]


def test_split_id_is_sha256_hex():
    """The id is a 64-character hex string matching SHA-256 of the file path."""
    path = "s3://bucket/warehouse/db/table/data/00001.parquet"
    split = _make_split_with_path(path)
    expected = hashlib.sha256(path.encode("utf-8")).hexdigest()
    assert split.id == expected
    assert len(split.id) == 64
