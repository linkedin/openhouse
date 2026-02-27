import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass

from pyiceberg.catalog import Catalog
from requests import HTTPError
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader._timer import log_duration
from openhouse.dataloader.data_loader_split import DataLoaderSplit
from openhouse.dataloader.filters import Filter, _to_pyiceberg, always_true
from openhouse.dataloader.table_identifier import TableIdentifier
from openhouse.dataloader.table_transformer import TableTransformer
from openhouse.dataloader.udf_registry import UDFRegistry

logger = logging.getLogger(__name__)


def _is_transient(exc: BaseException) -> bool:
    """Return True if the exception is transient and worth retrying."""
    if isinstance(exc, HTTPError):
        return exc.response is not None and exc.response.status_code >= 500
    return isinstance(exc, OSError)


def _retry[T](fn: Callable[[], T], label: str, max_attempts: int) -> T:
    """Call *fn* with retry logic, logging duration of each attempt.

    Retries on ``OSError`` (transient network/storage I/O failures),
    except ``HTTPError`` which is only retried for 5xx status codes.
    Uses exponential backoff with up to *max_attempts* total attempts.
    """
    for attempt in Retrying(
        retry=retry_if_exception(_is_transient),
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(),
        reraise=True,
    ):
        with attempt, log_duration(logger, "%s (attempt %d)", label, attempt.retry_state.attempt_number):
            return fn()
    raise AssertionError("unreachable")  # pragma: no cover


@dataclass
class DataLoaderContext:
    """Context and customization for the DataLoader.

    Provides execution context (e.g. tenant, environment) and optional customizations
    like table transformations applied before loading data.

    Args:
        execution_context: Dictionary of execution context information (e.g. tenant, environment)
        table_transformer: Transformation to apply to the table before loading (e.g. column masking)
        udf_registry: UDFs required for the table transformation
    """

    execution_context: Mapping[str, str] | None = None
    table_transformer: TableTransformer | None = None
    udf_registry: UDFRegistry | None = None


class OpenHouseDataLoader:
    """An API for distributed data loading of OpenHouse tables"""

    def __init__(
        self,
        catalog: Catalog,
        database: str,
        table: str,
        branch: str | None = None,
        columns: Sequence[str] | None = None,
        filters: Filter | None = None,
        context: DataLoaderContext | None = None,
        max_attempts: int = 3,
    ):
        """
        Args:
            catalog: Catalog for loading tables
            database: Database name
            table: Table name
            branch: Optional branch name
            columns: Column names to load, or None to load all columns
            filters: Row filter expression, defaults to always_true() (all rows)
            context: Data loader context
            max_attempts: Total number of attempts including the initial try (default 3)
        """
        self._catalog = catalog
        self._table_id = TableIdentifier(database, table, branch)
        self._columns = columns
        self._filters = filters if filters is not None else always_true()
        self._context = context or DataLoaderContext()
        self._max_attempts = max_attempts

        self._iceberg_table = _retry(
            lambda: self._catalog.load_table((self._table_id.database, self._table_id.table)),
            label=f"load_table {self._table_id}",
            max_attempts=self._max_attempts,
        )

    @property
    def table_properties(self) -> Mapping[str, str]:
        """Properties of the table being loaded"""
        return self._iceberg_table.metadata.properties

    @property
    def snapshot_id(self) -> int | None:
        """Snapshot ID of the loaded table, or None if the table has no snapshots"""
        return self._iceberg_table.metadata.current_snapshot_id

    def __iter__(self) -> Iterator[DataLoaderSplit]:
        """Iterate over data splits for distributed data loading of the table.

        Yields:
            DataLoaderSplit for each file scan task in the table
        """
        table = self._iceberg_table

        row_filter = _to_pyiceberg(self._filters)

        scan_kwargs: dict = {"row_filter": row_filter}
        if self._columns:
            scan_kwargs["selected_fields"] = tuple(self._columns)

        scan = table.scan(**scan_kwargs)

        scan_context = TableScanContext(
            table_metadata=table.metadata,
            io=table.io,
            projected_schema=scan.projection(),
            row_filter=row_filter,
        )

        # plan_files() materializes all tasks at once (PyIceberg doesn't support streaming)
        # Manifests are read in parallel with one thread per manifest
        scan_tasks = _retry(
            lambda: scan.plan_files(), label=f"plan_files {self._table_id}", max_attempts=self._max_attempts
        )

        for scan_task in scan_tasks:
            yield DataLoaderSplit(
                file_scan_task=scan_task,
                scan_context=scan_context,
            )
