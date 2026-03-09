"""Tests verifying the ArrivalOrder API from pyiceberg PR #3046 is available and functional.

These tests confirm that the openhouse dataloader can access the new ScanOrder class hierarchy
added upstream (apache/iceberg-python#3046) and that ArrowScan.to_record_batches accepts the
order parameter.
"""

import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io import load_file_io
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import ArrivalOrder, FileScanTask, ScanOrder, TaskOrder
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import LongType, NestedField, StringType

_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
)


def _write_parquet(tmp_path: object, table: pa.Table) -> str:
    """Write a parquet file with Iceberg field IDs and return its path."""
    file_path = str(tmp_path / "test.parquet")  # type: ignore[operator]
    fields = [field.with_metadata({b"PARQUET:field_id": str(i + 1).encode()}) for i, field in enumerate(table.schema)]
    pq.write_table(table.cast(pa.schema(fields)), file_path)
    return file_path


def _make_arrow_scan_and_task(tmp_path: object, table: pa.Table) -> tuple[ArrowScan, FileScanTask]:
    """Create an ArrowScan and FileScanTask from a PyArrow table written to disk."""
    file_path = _write_parquet(tmp_path, table)

    metadata = new_table_metadata(
        schema=_SCHEMA,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
        location=str(tmp_path),
        properties={},
    )

    arrow_scan = ArrowScan(
        table_metadata=metadata,
        io=load_file_io(properties={}, location=file_path),
        projected_schema=_SCHEMA,
        row_filter=AlwaysTrue(),
    )

    data_file = DataFile.from_args(
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        record_count=table.num_rows,
        file_size_in_bytes=os.path.getsize(file_path),
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)

    return arrow_scan, task


def _sample_table() -> pa.Table:
    return pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["alice", "bob", "charlie"], type=pa.string()),
        }
    )


class TestScanOrderImports:
    """Verify the ScanOrder class hierarchy is importable from pyiceberg.table."""

    def test_scan_order_base_class_exists(self) -> None:
        assert ScanOrder is not None

    def test_task_order_is_scan_order(self) -> None:
        assert issubclass(TaskOrder, ScanOrder)

    def test_arrival_order_is_scan_order(self) -> None:
        assert issubclass(ArrivalOrder, ScanOrder)

    def test_arrival_order_default_params(self) -> None:
        ao = ArrivalOrder()
        assert ao.concurrent_streams == 8
        assert ao.batch_size is None
        assert ao.max_buffered_batches == 16

    def test_arrival_order_custom_params(self) -> None:
        ao = ArrivalOrder(concurrent_streams=4, batch_size=32768, max_buffered_batches=8)
        assert ao.concurrent_streams == 4
        assert ao.batch_size == 32768
        assert ao.max_buffered_batches == 8

    def test_arrival_order_rejects_invalid_concurrent_streams(self) -> None:
        with pytest.raises(ValueError, match="concurrent_streams"):
            ArrivalOrder(concurrent_streams=0)

    def test_arrival_order_rejects_invalid_max_buffered_batches(self) -> None:
        with pytest.raises(ValueError, match="max_buffered_batches"):
            ArrivalOrder(max_buffered_batches=0)


class TestToRecordBatchesOrder:
    """Verify ArrowScan.to_record_batches accepts the order parameter and returns correct data."""

    def test_default_order_returns_all_rows(self, tmp_path: object) -> None:
        """Default (TaskOrder) still works — backward compatible."""
        arrow_scan, task = _make_arrow_scan_and_task(tmp_path, _sample_table())
        batches = list(arrow_scan.to_record_batches([task]))
        result = pa.Table.from_batches(batches).sort_by("id")
        assert result.column("id").to_pylist() == [1, 2, 3]

    def test_explicit_task_order_returns_all_rows(self, tmp_path: object) -> None:
        arrow_scan, task = _make_arrow_scan_and_task(tmp_path, _sample_table())
        batches = list(arrow_scan.to_record_batches([task], order=TaskOrder()))
        result = pa.Table.from_batches(batches).sort_by("id")
        assert result.column("id").to_pylist() == [1, 2, 3]

    def test_arrival_order_returns_all_rows(self, tmp_path: object) -> None:
        arrow_scan, task = _make_arrow_scan_and_task(tmp_path, _sample_table())
        batches = list(arrow_scan.to_record_batches([task], order=ArrivalOrder(concurrent_streams=2)))
        result = pa.Table.from_batches(batches).sort_by("id")
        assert result.column("id").to_pylist() == [1, 2, 3]
        assert result.column("name").to_pylist() == ["alice", "bob", "charlie"]
