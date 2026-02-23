from unittest.mock import MagicMock

import pytest
from pyiceberg.expressions import AlwaysTrue, EqualTo
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


def _make_mock_table(num_tasks=2):
    """Create a mock PyIceberg Table with scan support."""
    table = MagicMock()
    table.metadata = MagicMock()
    table.io = MagicMock()

    scan = MagicMock()
    scan.projection.return_value = MagicMock()

    file_scan_tasks = [MagicMock() for _ in range(num_tasks)]
    scan.plan_files.return_value = file_scan_tasks

    table.scan.return_value = scan
    return table, scan, file_scan_tasks


def _make_mock_catalog(table):
    """Create a mock catalog that returns the given table."""
    catalog = MagicMock()
    catalog.load_table.return_value = table
    return catalog


def test_iter_yields_splits_per_file_scan_task():
    table, scan, file_scan_tasks = _make_mock_table(num_tasks=3)
    catalog = _make_mock_catalog(table)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    splits = list(loader)

    assert len(splits) == 3
    for split in splits:
        assert isinstance(split, DataLoaderSplit)

    catalog.load_table.assert_called_once_with(("db", "tbl"))


def test_iter_does_not_pass_selected_fields_when_no_columns():
    table, scan, _ = _make_mock_table(num_tasks=1)
    catalog = _make_mock_catalog(table)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    list(loader)

    table.scan.assert_called_once()
    call_kwargs = table.scan.call_args.kwargs
    assert "selected_fields" not in call_kwargs


def test_iter_passes_selected_columns():
    table, scan, _ = _make_mock_table(num_tasks=1)
    catalog = _make_mock_catalog(table)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", columns=["a", "b"])
    list(loader)

    call_kwargs = table.scan.call_args.kwargs
    assert call_kwargs["selected_fields"] == ("a", "b")


def test_iter_converts_filters_to_pyiceberg():
    table, scan, _ = _make_mock_table(num_tasks=1)
    catalog = _make_mock_catalog(table)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", filters=col("x") == 1)
    list(loader)

    call_kwargs = table.scan.call_args.kwargs
    assert isinstance(call_kwargs["row_filter"], EqualTo)
    assert call_kwargs["row_filter"] == EqualTo("x", 1)


def test_iter_default_filter_is_always_true():
    table, scan, _ = _make_mock_table(num_tasks=1)
    catalog = _make_mock_catalog(table)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    list(loader)

    call_kwargs = table.scan.call_args.kwargs
    assert isinstance(call_kwargs["row_filter"], AlwaysTrue)


def test_iter_empty_plan_files_yields_nothing():
    table, scan, _ = _make_mock_table(num_tasks=0)
    catalog = _make_mock_catalog(table)

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    splits = list(loader)

    assert splits == []


# --- Retry tests ---


def _make_http_error(status_code: int) -> HTTPError:
    """Create an HTTPError with a mock response carrying the given status code."""
    response = Response()
    response.status_code = status_code
    return HTTPError(response=response)


def test_load_table_retries_on_os_error():
    """load_table retries on OSError and succeeds on the second attempt."""
    table, _, _ = _make_mock_table(num_tasks=1)
    catalog = MagicMock()
    catalog.load_table.side_effect = [OSError("connection reset"), table]

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    splits = list(loader)

    assert len(splits) == 1
    assert catalog.load_table.call_count == 2


def test_load_table_retries_on_connection_error():
    """load_table retries on requests ConnectionError."""
    table, _, _ = _make_mock_table(num_tasks=1)
    catalog = MagicMock()
    catalog.load_table.side_effect = [RequestsConnectionError("refused"), table]

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    splits = list(loader)

    assert len(splits) == 1
    assert catalog.load_table.call_count == 2


def test_load_table_retries_on_timeout():
    """load_table retries on requests Timeout."""
    table, _, _ = _make_mock_table(num_tasks=1)
    catalog = MagicMock()
    catalog.load_table.side_effect = [Timeout("timed out"), table]

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    splits = list(loader)

    assert len(splits) == 1
    assert catalog.load_table.call_count == 2


def test_load_table_retries_on_5xx_http_error():
    """load_table retries on 5xx HTTPError."""
    table, _, _ = _make_mock_table(num_tasks=1)
    catalog = MagicMock()
    catalog.load_table.side_effect = [_make_http_error(503), table]

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    splits = list(loader)

    assert len(splits) == 1
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


def test_plan_files_retries_on_transient_error():
    """plan_files retries on OSError and succeeds on the second attempt."""
    table, scan, _ = _make_mock_table(num_tasks=1)
    catalog = _make_mock_catalog(table)

    file_scan_tasks = [MagicMock()]
    scan.plan_files.side_effect = [OSError("read timeout"), file_scan_tasks]

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
    splits = list(loader)

    assert len(splits) == 1
    assert scan.plan_files.call_count == 2


def test_retries_exhausted_reraises():
    """After all retry attempts are exhausted, the last exception is re-raised."""
    catalog = MagicMock()
    catalog.load_table.side_effect = OSError("persistent failure")

    loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")

    with pytest.raises(OSError, match="persistent failure"):
        list(loader)

    assert catalog.load_table.call_count == 3
