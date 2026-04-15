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

from openhouse.dataloader import DataLoaderContext, JvmConfig, OpenHouseDataLoader, __version__
from openhouse.dataloader.data_loader_split import DataLoaderSplit, to_sql_identifier
from openhouse.dataloader.filters import col
from openhouse.dataloader.table_transformer import TableTransformer
from openhouse.dataloader.udf_registry import UDFRegistry


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


def _write_parquet(tmp_path, data: dict, filename: str = "test.parquet") -> str:
    """Write a Parquet file with Iceberg field IDs in column metadata."""
    file_path = str(tmp_path / filename)
    table = pa.table(data)
    fields = [field.with_metadata({b"PARQUET:field_id": str(i + 1).encode()}) for i, field in enumerate(table.schema)]
    pq.write_table(table.cast(pa.schema(fields)), file_path)
    return file_path


def _make_real_catalog(
    tmp_path, data: dict = TEST_DATA, iceberg_schema: Schema = TEST_SCHEMA, properties: dict | None = None
):
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
        properties=properties or {},
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
    mock_table.schema.return_value = iceberg_schema
    mock_table.scan.side_effect = fake_scan

    catalog = MagicMock()
    catalog.load_table.return_value = mock_table
    return catalog


def _materialize(loader: OpenHouseDataLoader) -> pa.Table:
    """Iterate the loader and concatenate all splits into a single Arrow table."""
    batches = [batch for split in loader for batch in split]
    return pa.Table.from_batches(batches) if batches else pa.table({})


def test_table_properties_returns_metadata_properties(tmp_path):
    catalog = _make_real_catalog(tmp_path, properties={"custom.key": "myvalue"})

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")

    assert loader.table_properties["custom.key"] == "myvalue"


def test_snapshot_id_returns_current_snapshot_id(tmp_path):
    catalog = _make_real_catalog(tmp_path)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")

    assert loader.snapshot_id == catalog.load_table.return_value.metadata.current_snapshot_id


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


# --- snapshot_id tests ---


def test_snapshot_id_passed_to_scan(tmp_path):
    """snapshot_id is forwarded to table.scan() when provided."""
    catalog = _make_real_catalog(tmp_path)
    mock_table = catalog.load_table.return_value

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", snapshot_id=42)
    list(loader)

    mock_table.scan.assert_called_once()
    scan_kwargs = mock_table.scan.call_args.kwargs
    assert scan_kwargs["snapshot_id"] == 42


def test_snapshot_id_not_passed_when_none(tmp_path):
    """snapshot_id is omitted from scan kwargs when not provided."""
    catalog = _make_real_catalog(tmp_path)
    mock_table = catalog.load_table.return_value

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    list(loader)

    mock_table.scan.assert_called_once()
    scan_kwargs = mock_table.scan.call_args.kwargs
    assert "snapshot_id" not in scan_kwargs


def test_snapshot_id_with_columns_and_filters(tmp_path):
    """snapshot_id works alongside columns and filters."""
    catalog = _make_real_catalog(tmp_path)
    mock_table = catalog.load_table.return_value

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        snapshot_id=99,
        columns=[COL_ID],
        filters=col(COL_ID) == 1,
    )
    list(loader)

    mock_table.scan.assert_called_once()
    scan_kwargs = mock_table.scan.call_args.kwargs
    assert scan_kwargs["snapshot_id"] == 99
    assert scan_kwargs["selected_fields"] == (COL_ID,)
    assert "row_filter" in scan_kwargs


# --- Transformer tests ---


class _NoneTransformer(TableTransformer):
    """Transformer that returns None (no transformation)."""

    def __init__(self):
        super().__init__(dialect="datafusion")

    def transform(self, table, context):
        return None


class _MaskingTransformer(TableTransformer):
    """Transformer that masks the name column."""

    def __init__(self):
        super().__init__(dialect="datafusion")

    def transform(self, table, context):
        return f"SELECT id, 'MASKED' as name, value FROM {to_sql_identifier(table)}"


def test_iter_with_transformer_returning_none(tmp_path):
    """Transformer returns None → native Iceberg path, selected_fields still passed."""
    catalog = _make_real_catalog(tmp_path)
    mock_table = catalog.load_table.return_value

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        columns=[COL_ID, COL_NAME],
        context=DataLoaderContext(table_transformer=_NoneTransformer()),
    )
    result = _materialize(loader)

    assert result.num_rows == 3
    assert set(result.column_names) == {COL_ID, COL_NAME}
    mock_table.scan.assert_called_once()
    scan_kwargs = mock_table.scan.call_args.kwargs
    assert scan_kwargs["selected_fields"] == (COL_ID, COL_NAME)


def test_iter_with_transformer_returning_sql(tmp_path):
    """Transformer returns SQL → transform is applied to splits."""
    catalog = _make_real_catalog(tmp_path)

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        context=DataLoaderContext(table_transformer=_MaskingTransformer()),
    )
    result = _materialize(loader)

    assert result.num_rows == 3
    assert result.column("name").to_pylist() == ["MASKED", "MASKED", "MASKED"]


def test_iter_with_transformer_and_columns_projects(tmp_path):
    """columns + transformer → output contains only requested columns."""
    catalog = _make_real_catalog(tmp_path)
    mock_table = catalog.load_table.return_value

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        columns=[COL_ID],
        context=DataLoaderContext(table_transformer=_MaskingTransformer()),
    )
    result = _materialize(loader)

    assert result.num_rows == 3
    assert result.column_names == [COL_ID]

    # Verify scan received only the source columns needed for the outer SELECT
    scan_kwargs = mock_table.scan.call_args.kwargs
    assert scan_kwargs["selected_fields"] == (COL_ID,)
    assert "row_filter" in scan_kwargs


def test_iter_with_transformer_and_all_columns(tmp_path):
    """columns requesting all transformer outputs → all source columns projected."""
    catalog = _make_real_catalog(tmp_path)

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        columns=[COL_ID, COL_NAME, COL_VALUE],
        context=DataLoaderContext(table_transformer=_MaskingTransformer()),
    )
    result = _materialize(loader)

    assert result.num_rows == 3
    assert set(result.column_names) == {COL_ID, COL_NAME, COL_VALUE}


class _SparkMaskingTransformer(TableTransformer):
    """Transformer using Spark SQL dialect."""

    def __init__(self):
        super().__init__(dialect="spark")

    def transform(self, table, context):
        return f"SELECT id, CAST('MASKED' AS STRING) AS name, value FROM {to_sql_identifier(table)}"


def test_iter_with_spark_dialect_transformer_transpiles(tmp_path):
    """Spark-dialect transformer SQL is transpiled to DataFusion and applied."""
    catalog = _make_real_catalog(tmp_path)

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        context=DataLoaderContext(table_transformer=_SparkMaskingTransformer()),
    )
    result = _materialize(loader)

    assert result.num_rows == 3
    assert result.column("name").to_pylist() == ["MASKED", "MASKED", "MASKED"]


def test_iter_with_invalid_dialect_raises(tmp_path):
    """Unsupported dialect raises ValueError during iteration."""

    class _BadDialectTransformer(TableTransformer):
        def __init__(self):
            super().__init__(dialect="not_a_real_dialect")

        def transform(self, table, context):
            return f"SELECT * FROM {to_sql_identifier(table)}"

    catalog = _make_real_catalog(tmp_path)
    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        context=DataLoaderContext(table_transformer=_BadDialectTransformer()),
    )

    with pytest.raises(ValueError, match="Unsupported source dialect"):
        _materialize(loader)


def test_iter_with_transformer_and_special_char_database(tmp_path):
    """Transformer works when the database name contains special characters."""
    catalog = _make_real_catalog(tmp_path)

    class _QuotedMaskingTransformer(TableTransformer):
        def __init__(self):
            super().__init__(dialect="datafusion")

        def transform(self, table, context):
            return f"SELECT id, 'MASKED' as name, value FROM {to_sql_identifier(table)}"

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database='my"db',
        table="tbl",
        context=DataLoaderContext(table_transformer=_QuotedMaskingTransformer()),
    )
    result = _materialize(loader)

    assert result.num_rows == 3
    assert result.column("name").to_pylist() == ["MASKED", "MASKED", "MASKED"]


# --- branch tests ---


def test_branch_and_snapshot_id_raises():
    """ValueError is raised when both branch and snapshot_id are provided."""
    catalog = MagicMock()

    with pytest.raises(ValueError, match="Cannot specify both branch and snapshot_id"):
        OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", branch="b", snapshot_id=42)


def test_branch_snapshot_id_resolves():
    """snapshot_id property resolves via snapshot_by_name when branch is set."""
    catalog = MagicMock()
    mock_snapshot = MagicMock()
    mock_snapshot.snapshot_id = 123
    catalog.load_table.return_value.snapshot_by_name.side_effect = lambda name: (
        mock_snapshot if name == "my-branch" else None
    )

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", branch="my-branch")

    assert loader.snapshot_id == 123


def test_branch_snapshot_id_not_found_raises():
    """ValueError is raised when branch does not exist in table metadata."""
    catalog = MagicMock()
    catalog.load_table.return_value.snapshot_by_name.side_effect = lambda name: None

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", branch="missing")

    with pytest.raises(ValueError, match="Branch 'missing' not found"):
        _ = loader.snapshot_id


def test_branch_reads_data_from_branch_snapshot():
    """Branch splits come from the branch snapshot, not the main snapshot."""
    catalog = MagicMock()

    main_task = MagicMock()
    main_task.file.file_path = "main.parquet"
    branch_task = MagicMock()
    branch_task.file.file_path = "branch.parquet"

    branch_snapshot_id = 200

    def fake_scan(**kwargs):
        task = branch_task if kwargs.get("snapshot_id") == branch_snapshot_id else main_task
        scan = MagicMock()
        scan.plan_files.return_value = [task]
        return scan

    mock_snapshot = MagicMock()
    mock_snapshot.snapshot_id = branch_snapshot_id

    mock_table = catalog.load_table.return_value
    mock_table.scan.side_effect = fake_scan
    mock_table.snapshot_by_name.side_effect = lambda name: mock_snapshot if name == "my-branch" else None

    # Without branch: splits come from main snapshot
    main_splits = list(OpenHouseDataLoader(catalog=catalog, database="db", table="tbl"))
    assert len(main_splits) == 1
    assert main_splits[0]._file_scan_tasks[0].file.file_path == "main.parquet"

    # With branch: splits come from branch snapshot
    branch_splits = list(OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", branch="my-branch"))
    assert len(branch_splits) == 1
    assert branch_splits[0]._file_scan_tasks[0].file.file_path == "branch.parquet"


# --- batch_size tests ---


def test_batch_size_forwarded_to_splits(tmp_path):
    """batch_size is correctly passed through to each DataLoaderSplit."""
    catalog = _make_real_catalog(tmp_path)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", batch_size=32768)
    splits = list(loader)

    assert len(splits) >= 1
    for split in splits:
        assert split._batch_size == 32768


def test_batch_size_default_is_none(tmp_path):
    """Omitting batch_size defaults to None in each split."""
    catalog = _make_real_catalog(tmp_path)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    splits = list(loader)

    assert len(splits) >= 1
    for split in splits:
        assert split._batch_size is None


# --- files_per_split tests ---


def _add_file_tasks(catalog, num_tasks: int) -> None:
    """Override plan_files on a catalog from _make_real_catalog to return multiple mock tasks."""
    mock_table = catalog.load_table.return_value
    original_scan = mock_table.scan.side_effect

    def multi_file_scan(**kwargs):
        scan = original_scan(**kwargs)
        scan.plan_files.return_value = [
            MagicMock(file=MagicMock(file_path=f"file_{i}.parquet")) for i in range(num_tasks)
        ]
        return scan

    mock_table.scan.side_effect = multi_file_scan


def test_files_per_split_groups_tasks(tmp_path):
    """files_per_split=2 groups 4 files into 2 splits of 2 files each."""
    catalog = _make_real_catalog(tmp_path)
    _add_file_tasks(catalog, 4)
    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", files_per_split=2)
    splits = list(loader)

    assert len(splits) == 2
    for split in splits:
        assert len(split._file_scan_tasks) == 2


def test_files_per_split_remainder_split(tmp_path):
    """When files don't divide evenly, the last split gets the remainder."""
    catalog = _make_real_catalog(tmp_path)
    _add_file_tasks(catalog, 5)
    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", files_per_split=3)
    splits = list(loader)

    assert len(splits) == 2
    assert len(splits[0]._file_scan_tasks) == 3
    assert len(splits[1]._file_scan_tasks) == 2


def test_files_per_split_invalid_raises():
    """files_per_split < 1 raises ValueError."""
    catalog = MagicMock()
    with pytest.raises(ValueError, match="files_per_split must be at least 1"):
        OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", files_per_split=0)


# --- Predicate pushdown with transformer tests ---


class _FilteringTransformer(TableTransformer):
    """Transformer that has a WHERE clause filtering on status."""

    def __init__(self):
        super().__init__(dialect="datafusion")

    def transform(self, table, context):
        return f"SELECT id, name, value, status FROM {to_sql_identifier(table)} WHERE status = 'active'"


def test_iter_with_transformer_where_extracts_predicate(tmp_path):
    """Transform with WHERE + columns=[id] → scan includes status for predicate, row_filter has predicate."""
    extra_schema = Schema(
        NestedField(field_id=1, name=COL_ID, field_type=LongType(), required=False),
        NestedField(field_id=2, name=COL_NAME, field_type=StringType(), required=False),
        NestedField(field_id=3, name=COL_VALUE, field_type=DoubleType(), required=False),
        NestedField(field_id=4, name="status", field_type=StringType(), required=False),
    )
    data = {
        COL_ID: [1, 2],
        COL_NAME: ["alice", "bob"],
        COL_VALUE: [1.1, 2.2],
        "status": ["active", "inactive"],
    }
    catalog = _make_real_catalog(tmp_path, data=data, iceberg_schema=extra_schema)
    mock_table = catalog.load_table.return_value

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        columns=[COL_ID],
        context=DataLoaderContext(table_transformer=_FilteringTransformer()),
    )
    list(loader)

    scan_kwargs = mock_table.scan.call_args.kwargs
    # row_filter should not be AlwaysTrue (it should contain the extracted predicate)
    from pyiceberg.expressions import AlwaysTrue as IceAlwaysTrue

    assert not isinstance(scan_kwargs["row_filter"], IceAlwaysTrue)


class _MaskingFilteringTransformer(TableTransformer):
    """Transformer that masks name and filters on value."""

    def __init__(self):
        super().__init__(dialect="datafusion")

    def transform(self, table, context):
        return f"SELECT id, 'MASKED' as name, value FROM {to_sql_identifier(table)} WHERE value > 1.5"


def test_iter_with_transformer_projects_subset_of_transform_columns(tmp_path):
    """columns=[id] with transformer referencing id, name, value and WHERE on value → only id in output.

    The transform SQL references all three columns and has a WHERE clause,
    but the user only requests id. The optimizer should prune unused columns
    from the SQL and extract the WHERE predicate for Iceberg pushdown.
    """
    catalog = _make_real_catalog(tmp_path)

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        columns=[COL_ID],
        context=DataLoaderContext(table_transformer=_MaskingFilteringTransformer()),
    )
    result = _materialize(loader)

    # value > 1.5 filters out id=1 (value=1.1), keeps id=2 (2.2) and id=3 (3.3)
    assert result.num_rows == 2
    assert result.column_names == [COL_ID]
    assert sorted(result.column(COL_ID).to_pylist()) == [2, 3]


def test_iter_with_transformer_and_user_filter_on_passthrough(tmp_path):
    """Transform + user filter on passthrough column → pushed to Iceberg."""
    catalog = _make_real_catalog(tmp_path)
    mock_table = catalog.load_table.return_value

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        columns=[COL_ID],
        filters=col(COL_VALUE) > 2.0,
        context=DataLoaderContext(table_transformer=_MaskingTransformer()),
    )
    list(loader)

    scan_kwargs = mock_table.scan.call_args.kwargs
    # value is a passthrough in _MaskingTransformer, so the filter should be pushed
    from pyiceberg.expressions import AlwaysTrue as IceAlwaysTrue

    assert not isinstance(scan_kwargs["row_filter"], IceAlwaysTrue)


def test_iter_with_transformer_filter_injection_produces_correct_results(tmp_path):
    """End-to-end: transformer + user filter + projection."""
    catalog = _make_real_catalog(tmp_path)

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        columns=[COL_ID, COL_NAME],
        filters=col(COL_ID) > 1,
        context=DataLoaderContext(table_transformer=_MaskingTransformer()),
    )
    result = _materialize(loader)

    # id > 1 filters out id=1, keeps id=2,3; name is masked; only requested columns
    assert result.num_rows == 2
    assert result.column_names == [COL_ID, COL_NAME]
    assert sorted(result.column(COL_ID).to_pylist()) == [2, 3]
    assert result.column(COL_NAME).to_pylist() == ["MASKED", "MASKED"]


class _PassthroughTransformer(TableTransformer):
    """Transformer that selects all columns unchanged."""

    def __init__(self):
        super().__init__(dialect="datafusion")

    def transform(self, table, context):
        return f"SELECT id, name, value FROM {to_sql_identifier(table)}"


MIXED_CASE_SCHEMA = Schema(
    NestedField(field_id=1, name="purchaseAmount", field_type=DoubleType(), required=False),
    NestedField(field_id=2, name="itemCount", field_type=LongType(), required=False),
    NestedField(field_id=3, name="discountRate", field_type=DoubleType(), required=False),
)

MIXED_CASE_DATA = {
    "purchaseAmount": [19.99, 49.95, 9.99],
    "itemCount": [2, 5, 1],
    "discountRate": [0.1, 0.2, 0.0],
}


class _MixedCaseTransformer(TableTransformer):
    """Transformer that selects mixed-case columns."""

    def __init__(self):
        super().__init__(dialect="datafusion")

    def transform(self, table, context):
        return f'SELECT "purchaseAmount", "itemCount", "discountRate" FROM {to_sql_identifier(table)}'


def test_iter_with_transformer_preserves_mixed_case_columns(tmp_path):
    """Transformer with mixed-case columns preserves original casing in Iceberg scan.

    camelCase columns like purchaseAmount, itemCount, discountRate would lose
    their casing under a lowercasing dialect, breaking Iceberg field lookups.
    """
    catalog = _make_real_catalog(tmp_path, data=MIXED_CASE_DATA, iceberg_schema=MIXED_CASE_SCHEMA)
    mock_table = catalog.load_table.return_value

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        columns=["purchaseAmount", "itemCount"],
        context=DataLoaderContext(table_transformer=_MixedCaseTransformer()),
    )
    result = _materialize(loader)

    assert result.num_rows == 3
    # Verify scan received original camelCase names, not lowercased
    scan_kwargs = mock_table.scan.call_args.kwargs
    selected = scan_kwargs["selected_fields"]
    assert "purchaseAmount" in selected
    assert "itemCount" in selected
    assert "purchaseamount" not in selected
    assert "itemcount" not in selected


def test_iter_with_transformer_preserves_mixed_case_filter_columns(tmp_path):
    """Pushed-down filter column names preserve original mixed-case casing.

    Filtering on itemCount must not cause it to appear as 'itemcount' in the scan.
    """
    catalog = _make_real_catalog(tmp_path, data=MIXED_CASE_DATA, iceberg_schema=MIXED_CASE_SCHEMA)
    mock_table = catalog.load_table.return_value

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        filters=col("itemCount") > 3,
        context=DataLoaderContext(table_transformer=_MixedCaseTransformer()),
    )
    _materialize(loader)

    scan_kwargs = mock_table.scan.call_args.kwargs
    selected = scan_kwargs["selected_fields"]
    # All projected columns must use original camelCase from the Iceberg schema
    allowed = {"purchaseAmount", "itemCount", "discountRate"}
    for field in selected:
        assert field in allowed, f"Unexpected field '{field}' — likely lowercased"


@pytest.mark.parametrize(
    "filter_expr, expected_names",
    [
        (col(COL_NAME).starts_with("20%"), ["20%off"]),
        (col(COL_NAME).starts_with("item_"), ["item_1"]),
        (col(COL_NAME).starts_with("back\\"), ["back\\slash"]),
        (col(COL_NAME).starts_with("x\\%y\\_"), ["x\\%y\\_z"]),
        (col(COL_NAME).not_starts_with("20%"), ["2000", "back\\slash", "item_1", "itemX1", "other", "x\\%y\\_z"]),
        (col(COL_NAME).not_starts_with("item_"), ["20%off", "2000", "back\\slash", "itemX1", "other", "x\\%y\\_z"]),
        (col(COL_NAME).not_starts_with("back\\"), ["20%off", "2000", "item_1", "itemX1", "other", "x\\%y\\_z"]),
        (col(COL_NAME).not_starts_with("x\\%y\\_"), ["20%off", "2000", "back\\slash", "item_1", "itemX1", "other"]),
    ],
    ids=[
        "starts_with_%",
        "starts_with__",
        "starts_with_backslash",
        "starts_with_combined",
        "not_starts_with_%",
        "not_starts_with__",
        "not_starts_with_backslash",
        "not_starts_with_combined",
    ],
)
def test_starts_with_wildcard_literals(tmp_path, filter_expr, expected_names):
    """StartsWith/NotStartsWith treat %, _, and \\ as literal characters, not SQL wildcards/escapes.

    The data includes "2000", "itemX1", and "other" which would falsely match if %, _, or \\
    were treated as LIKE wildcards (LIKE '20%' matches '2000', LIKE 'item_%' matches 'itemX1').
    """
    wildcard_data = {
        COL_ID: [1, 2, 3, 4, 5, 6, 7],
        COL_NAME: ["20%off", "2000", "item_1", "itemX1", "back\\slash", "x\\%y\\_z", "other"],
        COL_VALUE: [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7],
    }
    catalog = _make_real_catalog(tmp_path, data=wildcard_data)

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        filters=filter_expr,
        context=DataLoaderContext(table_transformer=_PassthroughTransformer()),
    )
    result = _materialize(loader)
    assert sorted(result.column(COL_NAME).to_pylist()) == sorted(expected_names)


# --- JVM args tests ---


def test_planner_jvm_args_sets_libhdfs_opts(tmp_path, monkeypatch):
    """JvmConfig.planner_args is applied to LIBHDFS_OPTS during __init__."""
    from openhouse.dataloader._jvm import LIBHDFS_OPTS_ENV

    monkeypatch.delenv(LIBHDFS_OPTS_ENV, raising=False)
    catalog = _make_real_catalog(tmp_path)

    OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        context=DataLoaderContext(jvm_config=JvmConfig(planner_args="-Xmx256m")),
    )

    assert os.environ[LIBHDFS_OPTS_ENV] == "-Xmx256m"


# --- Regression: materialization of optimized queries with pushdown bugs ---
#
# These tests verify that SQL patterns with nested subqueries, UDFs, and
# mixed-case columns can be optimized by optimize_scan and materialized
# by DataFusion end-to-end.

_PUSHDOWN_SCHEMA = Schema(
    NestedField(field_id=1, name="memberId", field_type=LongType(), required=False),
    NestedField(field_id=2, name="policyField", field_type=StringType(), required=False),
    NestedField(field_id=3, name="otherField", field_type=StringType(), required=False),
)

_PUSHDOWN_DATA = {
    "memberId": [100, 200, 300],
    "policyField": ["policy_a", "policy_b", "policy_c"],
    "otherField": ["x", "y", "z"],
}


class _FooBarUDFRegistry(UDFRegistry):
    """Registers mock UDFs for testing pushdown materialization.

    foo(utf8, int64) -> bool   — always returns true (filter UDF)
    bar(bool, utf8, utf8, utf8) -> utf8 — conditional replacement UDF
    """

    def register_udfs(self, session_context):
        import datafusion

        def foo(arg, member_id):
            return pa.array([True] * len(arg), type=pa.bool_())

        session_context.register_udf(datafusion.udf(foo, [pa.utf8(), pa.int64()], pa.bool_(), "stable", name="foo"))

        def bar(condition, value, field_name, replacement):
            cond = condition.to_pylist()
            vals = value.to_pylist()
            repls = replacement.to_pylist()
            return pa.array([r if c else v for c, v, r in zip(cond, vals, repls, strict=True)], type=pa.utf8())

        session_context.register_udf(
            datafusion.udf(bar, [pa.bool_(), pa.utf8(), pa.utf8(), pa.utf8()], pa.utf8(), "stable", name="bar")
        )


class _NestedUDFTransformer(TableTransformer):
    """Nested subquery with UDF at two levels — triggers alias rewrite bug."""

    def __init__(self):
        super().__init__(dialect="datafusion")

    def transform(self, table, context):
        tbl = to_sql_identifier(table)
        alias = f'"{table.table}"'
        return (
            f"SELECT * "
            f"FROM (SELECT * FROM {tbl} AS {alias} "
            f'      WHERE foo(\'arg1\', {alias}."memberId")) AS "t" '
            f'WHERE foo(\'arg2\', "t"."memberId")'
        )


class _SelectStarUDFTransformer(TableTransformer):
    """SELECT * with UDF — triggers unquoted column bug after projection pushdown."""

    def __init__(self):
        super().__init__(dialect="datafusion")

    def transform(self, table, context):
        tbl = to_sql_identifier(table)
        alias = f'"{table.table}"'
        return f"SELECT * FROM {tbl} AS {alias} WHERE foo('arg1', {alias}.\"memberId\")"


class _ProjectionUDFTransformer(TableTransformer):
    """UDF in projection with inner SELECT * — triggers unquoted column bug in projection."""

    def __init__(self):
        super().__init__(dialect="datafusion")

    def transform(self, table, context):
        tbl = to_sql_identifier(table)
        alias = f'"{table.table}"'
        return (
            f'SELECT "t"."memberId", "t"."policyField", '
            f'       bar(NOT foo(\'arg1\', "t"."memberId"), '
            f'           "t"."otherField", \'otherField\', \'REPLACED\') AS "otherField" '
            f"FROM (SELECT * FROM {tbl} AS {alias} "
            f'      WHERE foo(\'arg2\', {alias}."memberId")) AS "t"'
        )


def test_nested_udf_transformer_materializes(tmp_path):
    """Nested subquery with UDF at two levels materializes after optimization."""
    catalog = _make_real_catalog(tmp_path, data=_PUSHDOWN_DATA, iceberg_schema=_PUSHDOWN_SCHEMA)

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        context=DataLoaderContext(
            table_transformer=_NestedUDFTransformer(),
            udf_registry=_FooBarUDFRegistry(),
        ),
    )
    result = _materialize(loader)

    assert result.num_rows == 3
    assert set(result.column_names) == {"memberId", "policyField", "otherField"}
    result = result.sort_by("memberId")
    assert result.column("memberId").to_pylist() == [100, 200, 300]


def test_select_star_projection_with_mixed_case_materializes(tmp_path):
    """SELECT * with mixed-case columns materializes after projection pushdown."""
    catalog = _make_real_catalog(tmp_path, data=_PUSHDOWN_DATA, iceberg_schema=_PUSHDOWN_SCHEMA)

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        columns=["memberId", "policyField"],
        context=DataLoaderContext(
            table_transformer=_SelectStarUDFTransformer(),
            udf_registry=_FooBarUDFRegistry(),
        ),
    )
    result = _materialize(loader)

    assert result.num_rows == 3
    assert set(result.column_names) == {"memberId", "policyField"}
    result = result.sort_by("memberId")
    assert result.column("memberId").to_pylist() == [100, 200, 300]
    assert result.column("policyField").to_pylist() == ["policy_a", "policy_b", "policy_c"]


def test_projection_udf_with_mixed_case_materializes(tmp_path):
    """UDF in projection with inner SELECT * and mixed-case columns materializes."""
    catalog = _make_real_catalog(tmp_path, data=_PUSHDOWN_DATA, iceberg_schema=_PUSHDOWN_SCHEMA)

    loader = OpenHouseDataLoader(
        catalog=catalog,
        database="db",
        table="tbl",
        context=DataLoaderContext(
            table_transformer=_ProjectionUDFTransformer(),
            udf_registry=_FooBarUDFRegistry(),
        ),
    )
    result = _materialize(loader)

    assert result.num_rows == 3
    assert set(result.column_names) == {"memberId", "policyField", "otherField"}
    result = result.sort_by("memberId")
    assert result.column("memberId").to_pylist() == [100, 200, 300]
    assert result.column("policyField").to_pylist() == ["policy_a", "policy_b", "policy_c"]
    # foo returns True; NOT True = False; bar(False, val, ...) returns val (not replaced)
    assert result.column("otherField").to_pylist() == ["x", "y", "z"]
