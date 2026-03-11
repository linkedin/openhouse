from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from types import MappingProxyType

from datafusion.context import SessionContext
from pyarrow import RecordBatch
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import ArrivalOrder, FileScanTask

from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader.table_identifier import TableIdentifier, _quote_identifier
from openhouse.dataloader.udf_registry import NoOpRegistry, UDFRegistry


def _create_transform_session(
    table_id: TableIdentifier,
    udf_registry: UDFRegistry,
) -> SessionContext:
    """Create a DataFusion SessionContext for running split-level transforms.

    Returns a ready-to-query SessionContext where UDFs are registered and the
    target schema exists.
    """
    session = SessionContext()
    udf_registry.register_udfs(session)

    session.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(table_id.database)}").collect()
    return session


def _bind_batch_table(session: SessionContext, table_id: TableIdentifier, batch: RecordBatch) -> None:
    """Bind a single batch to the table name used by transform SQL."""
    session.deregister_table(table_id.sql_name)
    session.register_record_batches(table_id.sql_name, [[batch]])


class DataLoaderSplit:
    """A single data split"""

    def __init__(
        self,
        file_scan_task: FileScanTask,
        scan_context: TableScanContext,
        transform_sql: str | None = None,
        udf_registry: UDFRegistry | None = None,
        batch_size: int | None = None,
    ):
        if transform_sql is not None and scan_context.table_id is None:
            raise ValueError("scan_context.table_id is required when transform_sql is provided")
        self._file_scan_task = file_scan_task
        self._scan_context = scan_context
        self._transform_sql = transform_sql
        self._udf_registry = udf_registry or NoOpRegistry()
        self._batch_size = batch_size

    @property
    def id(self) -> str:
        """Unique ID for the split. This is stable across executions for a given
        snapshot and split size.
        """
        file_path = self._file_scan_task.file.file_path
        return hashlib.sha256(file_path.encode("utf-8")).hexdigest()

    @property
    def table_properties(self) -> Mapping[str, str]:
        """Properties of the table being loaded"""
        return MappingProxyType(self._scan_context.table_metadata.properties)

    def __iter__(self) -> Iterator[RecordBatch]:
        """Reads the file scan task and yields Arrow RecordBatches.

        Uses PyIceberg's ArrowScan to handle format dispatch, schema resolution,
        delete files, and partition spec lookups. The number of batches loaded
        into memory at once is bounded to prevent using too much memory at once.
        """
        ctx = self._scan_context
        arrow_scan = ArrowScan(
            table_metadata=ctx.table_metadata,
            io=ctx.io,
            projected_schema=ctx.projected_schema,
            row_filter=ctx.row_filter,
        )

        batches = arrow_scan.to_record_batches(
            [self._file_scan_task],
            order=ArrivalOrder(concurrent_streams=1, batch_size=self._batch_size),
        )

        if self._transform_sql is None:
            yield from batches
        else:
            if self._scan_context.table_id is None:
                raise RuntimeError("table_id is required when transform_sql is provided")
            session = _create_transform_session(self._scan_context.table_id, self._udf_registry)
            for batch in batches:
                yield from self._apply_transform(session, batch)

    def _apply_transform(self, session: SessionContext, batch: RecordBatch) -> Iterator[RecordBatch]:
        """Execute the transform SQL against a single RecordBatch."""
        if self._transform_sql is None or self._scan_context.table_id is None:
            raise RuntimeError("transform_sql and table_id are required for _apply_transform")

        _bind_batch_table(session, self._scan_context.table_id, batch)
        df = session.sql(self._transform_sql)
        yield from df.collect()
