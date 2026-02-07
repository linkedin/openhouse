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
from openhouse.dataloader.udf_registry import UDFRegistry

FILE_FORMATS = pytest.mark.parametrize("file_format", [FileFormat.PARQUET, FileFormat.ORC], ids=["parquet", "orc"])


class NoOpRegistry(UDFRegistry):
    def register_udfs(self, session_context):
        pass


def _write_and_split(
    tmp_path,
    table: pa.Table,
    file_format: FileFormat,
    iceberg_schema: Schema,
) -> DataLoaderSplit:
    """Write an Arrow table to disk and return a DataLoaderSplit backed by it."""
    ext = file_format.name.lower()
    file_path = str(tmp_path / f"test.{ext}")

    # ORC needs a name mapping stored in table properties for field-id resolution
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
        record_count=0,
        file_size_in_bytes=os.path.getsize(file_path),
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)
    return DataLoaderSplit(
        plan=plan,
        file_scan_task=task,
        udf_registry=NoOpRegistry(),
        scan_context=scan_context,
    )


@FILE_FORMATS
def test_iter_reads_real_file(tmp_path, file_format):
    """Reads a real file through __iter__ and verifies row count and column values."""
    iceberg_schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "value", DoubleType(), required=False),
        NestedField(4, "flag", BooleanType(), required=False),
    )
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
            "value": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
            "flag": pa.array([True, False, True], type=pa.bool_()),
        }
    )

    split = _write_and_split(tmp_path, table, file_format, iceberg_schema)
    batches = list(split)
    assert len(batches) >= 1
    assert sum(b.num_rows for b in batches) == 3

    result = pa.Table.from_batches(batches)
    assert result.column("id").to_pylist() == [1, 2, 3]
    assert result.column("name").to_pylist() == ["a", "b", "c"]
    assert result.column("value").to_pylist() == [1.1, 2.2, 3.3]
    assert result.column("flag").to_pylist() == [True, False, True]


@FILE_FORMATS
def test_iter_wide_table(tmp_path, file_format):
    """A table with many columns is read correctly."""
    num_cols = 50
    iceberg_schema = Schema(*[NestedField(i + 1, f"col_{i}", LongType(), required=False) for i in range(num_cols)])
    data = {f"col_{i}": list(range(5)) for i in range(num_cols)}
    table = pa.table(data)

    split = _write_and_split(tmp_path, table, file_format, iceberg_schema)
    result = pa.Table.from_batches(list(split))
    assert result.num_rows == 5
    assert result.num_columns == num_cols
    for i in range(num_cols):
        assert result.column(f"col_{i}").to_pylist() == list(range(5))
