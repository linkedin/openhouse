import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import cached_property
from types import MappingProxyType

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Snapshot
from requests import HTTPError
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from openhouse.dataloader._pushdown import ScanPushdown, analyze_pushdown
from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader._timer import log_duration
from openhouse.dataloader.data_loader_split import DataLoaderSplit
from openhouse.dataloader.datafusion_sql import to_datafusion_sql
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
        snapshot_id: int | None = None,
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
            snapshot_id: Optional snapshot ID for time-travel reads
            columns: Column names to load, or None to load all columns
            filters: Row filter expression, defaults to always_true() (all rows)
            context: Data loader context
            max_attempts: Total number of attempts including the initial try (default 3)
        """
        if branch is not None and branch.strip() == "":
            raise ValueError("branch must not be empty or whitespace")
        if branch is not None and snapshot_id is not None:
            raise ValueError("Cannot specify both branch and snapshot_id")
        self._catalog = catalog
        self._table_id = TableIdentifier(database, table, branch)
        self._snapshot_id = snapshot_id
        self._columns = columns
        self._filters = filters if filters is not None else always_true()
        self._context = context or DataLoaderContext()
        self._max_attempts = max_attempts

    @cached_property
    def _iceberg_table(self) -> Table:
        return _retry(
            lambda: self._catalog.load_table((self._table_id.database, self._table_id.table)),
            label=f"load_table {self._table_id}",
            max_attempts=self._max_attempts,
        )

    @property
    def table_properties(self) -> Mapping[str, str]:
        """Properties of the table being loaded"""
        return MappingProxyType(self._iceberg_table.metadata.properties)

    @cached_property
    def snapshot_id(self) -> int | None:
        """Snapshot ID of the loaded table, or None if the table has no snapshots"""
        if self._snapshot_id is not None:
            return self._snapshot_id
        if self._table_id.branch:
            snapshot = self._iceberg_table.snapshot_by_name(self._table_id.branch)
            if snapshot is None:
                raise ValueError(
                    f"Branch '{self._table_id.branch}' not found for table "
                    f"{self._table_id.database}.{self._table_id.table}"
                )
            return snapshot.snapshot_id
        return self._iceberg_table.metadata.current_snapshot_id

    def _verify_snapshot(self, snapshot: Snapshot | None) -> None:
        """Log the resolved snapshot or raise if a user-provided snapshot_id was not found."""
        if snapshot:
            logger.info("Using snapshot %d for table %s", snapshot.snapshot_id, self._table_id)
        elif self._snapshot_id is not None:
            raise ValueError(f"Snapshot {self._snapshot_id} not found for table {self._table_id}")
        else:
            logger.info("No snapshot found for table %s", self._table_id)

    def _build_transform_sql(self, transformer: TableTransformer, context: Mapping[str, str]) -> str | None:
        """Return DataFusion-compatible SQL for the transformation, or ``None``."""
        sql = transformer.transform(self._table_id, context)
        if sql is None:
            return None
        return to_datafusion_sql(sql, transformer.dialect)

    def _analyze_pushdown(self, transform_sql: str) -> ScanPushdown:
        """Determine which projections and filters can be pushed through the transformer."""
        return analyze_pushdown(
            transform_sql=transform_sql,
            table_schema=self._iceberg_table.metadata.schema(),
            table_id=self._table_id,
            columns=self._columns,
            filters=self._filters,
        )

    def __iter__(self) -> Iterator[DataLoaderSplit]:
        """Iterate over data splits for distributed data loading of the table.

        Yields:
            DataLoaderSplit for each file scan task in the table
        """
        table = self._iceberg_table

        # Build transform SQL: call transformer once to get the SQL string
        transformer = self._context.table_transformer
        execution_context = self._context.execution_context or {}
        transform_sql = self._build_transform_sql(transformer, execution_context) if transformer is not None else None

        if transform_sql is not None:
            pushdown = self._analyze_pushdown(transform_sql)
            row_filter = pushdown.scan_filter
            scan_kwargs: dict = {"row_filter": row_filter}
            if pushdown.scan_columns:
                scan_kwargs["selected_fields"] = pushdown.scan_columns
            combined_sql = pushdown.combined_sql
        else:
            row_filter = _to_pyiceberg(self._filters)
            scan_kwargs = {"row_filter": row_filter}
            if self._columns:
                scan_kwargs["selected_fields"] = tuple(self._columns)
            combined_sql = None

        if self.snapshot_id is not None:
            scan_kwargs["snapshot_id"] = self.snapshot_id

        scan = table.scan(**scan_kwargs)

        self._verify_snapshot(scan.snapshot())

        scan_context = TableScanContext(
            table_metadata=table.metadata,
            io=table.io,
            projected_schema=scan.projection(),
            row_filter=row_filter,
            table_id=self._table_id,
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
                transform_sql=combined_sql,
                udf_registry=self._context.udf_registry,
            )
