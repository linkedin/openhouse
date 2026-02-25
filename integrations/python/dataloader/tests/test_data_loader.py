import os
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.io import load_file_io
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import DoubleType, LongType, NestedField, StringType
from requests import ConnectionError as RequestsConnectionError
from requests import HTTPError, Response, Timeout

from openhouse.dataloader import DataLoaderContext, OpenHouseDataLoader, __version__
from openhouse.dataloader.data_loader_split import DataLoaderSplit
from openhouse.dataloader.filters import col


def test_package_imports():
    """Test that package imports work correctly"""
    assert OpenHouseDataLoader is not None
    assert DataLoaderContext is not None
    assert DataLoaderSplit is not None
    assert isinstance(__version__, str)
    assert len(__version__) > 0


COL_ID = "id"
COL_NAME = "name"
COL_VALUE = "value"

TEST_SCHEMA = Schema(
    NestedField(field_id=1, name=COL_ID, field_type=LongType(), required=False),
    NestedField(field_id=2, name=COL_NAME, field_type=StringType(), required=False),
    NestedField(field_id=3, name=COL_VALUE, field_type=DoubleType(), required=False),
)

TEST_DATA = {
    COL_ID: [1, 2, 3],
    COL_NAME: ["alice", "bob", "charlie"],
    COL_VALUE: [1.1, 2.2, 3.3],
}

EMPTY_DATA = {
    COL_ID: pa.array([], type=pa.int64()),
    COL_NAME: pa.array([], type=pa.string()),
    COL_VALUE: pa.array([], type=pa.float64()),
}


def _write_parquet(tmp_path, data: dict) -> str:
    """Write a Parquet file with Iceberg field IDs in column metadata."""
    file_path = str(tmp_path / "test.parquet")
    table = pa.table(data)
    fields = [field.with_metadata({b"PARQUET:field_id": str(i + 1).encode()}) for i, field in enumerate(table.schema)]
    pq.write_table(table.cast(pa.schema(fields)), file_path)
    return file_path


def _make_real_catalog(tmp_path, data: dict = TEST_DATA, iceberg_schema: Schema = TEST_SCHEMA):
    """Create a mock catalog backed by real Parquet data.

    The catalog mock only stubs the catalog boundary. The table's metadata, io,
    and file scan tasks are real, so DataLoaderSplits can be materialized.
    """
    file_path = _write_parquet(tmp_path, data)

    metadata = new_table_metadata(
        schema=iceberg_schema,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
        location=str(tmp_path),
    )
    io = load_file_io(properties={}, location=file_path)

    data_file = DataFile.from_args(
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        record_count=len(next(iter(data.values()))),
        file_size_in_bytes=os.path.getsize(file_path),
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)

    def fake_scan(**kwargs):
        selected = kwargs.get("selected_fields")
        projected = Schema(*[f for f in iceberg_schema.fields if f.name in selected]) if selected else iceberg_schema

        scan = MagicMock()
        scan.projection.return_value = projected
        scan.plan_files.return_value = [task]
        return scan

    mock_table = MagicMock()
    mock_table.metadata = metadata
    mock_table.io = io
    mock_table.scan.side_effect = fake_scan

    catalog = MagicMock()
    catalog.load_table.return_value = mock_table
    return catalog


def _materialize(loader: OpenHouseDataLoader) -> pa.Table:
    """Iterate the loader and concatenate all splits into a single Arrow table."""
    batches = [batch for split in loader for batch in split]
    return pa.Table.from_batches(batches) if batches else pa.table({})


def test_iter_returns_all_columns_when_no_selection(tmp_path):
    catalog = _make_real_catalog(tmp_path)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    result = _materialize(loader)

    assert set(result.column_names) == {COL_ID, COL_NAME, COL_VALUE}
    assert result.num_rows == 3
    result = result.sort_by(COL_ID)
    assert result.column(COL_ID).to_pylist() == TEST_DATA[COL_ID]
    assert result.column(COL_NAME).to_pylist() == TEST_DATA[COL_NAME]
    assert result.column(COL_VALUE).to_pylist() == TEST_DATA[COL_VALUE]


def test_iter_returns_only_selected_columns(tmp_path):
    catalog = _make_real_catalog(tmp_path)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", columns=[COL_ID, COL_NAME])
    result = _materialize(loader)

    assert set(result.column_names) == {COL_ID, COL_NAME}
    assert COL_VALUE not in result.column_names
    assert result.num_rows == 3
    result = result.sort_by(COL_ID)
    assert result.column(COL_ID).to_pylist() == TEST_DATA[COL_ID]
    assert result.column(COL_NAME).to_pylist() == TEST_DATA[COL_NAME]


def test_iter_with_filter_returns_matching_rows(tmp_path):
    catalog = _make_real_catalog(tmp_path)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", filters=col(COL_ID) == 1)
    result = _materialize(loader)

    assert result.num_rows == 1
    assert result.column(COL_ID).to_pylist() == [1]
    assert result.column(COL_NAME).to_pylist() == ["alice"]


def test_iter_empty_table_yields_nothing(tmp_path):
    catalog = _make_real_catalog(tmp_path, data=EMPTY_DATA)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    result = _materialize(loader)

    assert result.num_rows == 0


# --- Retry tests ---


def _make_http_error(status_code: int) -> HTTPError:
    """Create an HTTPError with a mock response carrying the given status code."""
    response = Response()
    response.status_code = status_code
    return HTTPError(response=response)


@pytest.mark.parametrize(
    "error",
    [
        OSError("connection reset"),
        RequestsConnectionError("refused"),
        Timeout("timed out"),
        _make_http_error(503),
    ],
    ids=["OSError", "ConnectionError", "Timeout", "5xx"],
)
def test_load_table_retries_on_transient_error(tmp_path, error):
    """load_table retries on transient errors and succeeds on the second attempt."""
    catalog = _make_real_catalog(tmp_path)
    real_table = catalog.load_table.return_value
    catalog.load_table.side_effect = [error, real_table]

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    result = _materialize(loader)

    assert result.num_rows == 3
    assert catalog.load_table.call_count == 2


def test_load_table_does_not_retry_on_4xx_http_error():
    """load_table does not retry on 4xx HTTPError (e.g. 404 Not Found)."""
    catalog = MagicMock()
    catalog.load_table.side_effect = _make_http_error(404)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")

    with pytest.raises(HTTPError):
        list(loader)

    catalog.load_table.assert_called_once()


def test_does_not_retry_non_transient_error():
    """Non-transient exceptions are raised immediately without retry."""
    catalog = MagicMock()
    catalog.load_table.side_effect = ValueError("bad argument")

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")

    with pytest.raises(ValueError, match="bad argument"):
        list(loader)

    catalog.load_table.assert_called_once()


def test_plan_files_retries_on_transient_error(tmp_path):
    """plan_files retries on OSError and succeeds on the second attempt."""
    catalog = _make_real_catalog(tmp_path)
    mock_table = catalog.load_table.return_value
    original_scan_side_effect = mock_table.scan.side_effect

    # Capture the real scan's plan_files result, then inject a failure before it
    real_scan = original_scan_side_effect()
    real_tasks = real_scan.plan_files.return_value

    def failing_then_real_scan(**kwargs):
        scan = original_scan_side_effect(**kwargs)
        scan.plan_files.side_effect = [OSError("read timeout"), real_tasks]
        return scan

    mock_table.scan.side_effect = failing_then_real_scan

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    result = _materialize(loader)

    assert result.num_rows == 3


def test_retries_exhausted_reraises():
    """After all retry attempts are exhausted, the last exception is re-raised."""
    catalog = MagicMock()
    catalog.load_table.side_effect = OSError("persistent failure")

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")

    with pytest.raises(OSError, match="persistent failure"):
        list(loader)

    assert catalog.load_table.call_count == 3
