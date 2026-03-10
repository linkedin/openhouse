from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from types import MappingProxyType

from datafusion.context import SessionContext
from pyarrow import RecordBatch
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import FileScanTask

from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader.table_identifier import TableIdentifier, _quote_identifier
from openhouse.dataloader.udf_registry import NoOpRegistry, UDFRegistry


def _create_transform_session(
    batch: RecordBatch,
    table_id: TableIdentifier,
    udf_registry: UDFRegistry,
) -> SessionContext:
    """Create a DataFusion SessionContext with the batch registered as a table.

    Returns a ready-to-query SessionContext where *batch* is available under
    ``table_id.sql_name``.
    """
    session = SessionContext()
    udf_registry.register_udfs(session)

    session.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(table_id.database)}").collect()
    session.register_record_batches(table_id.sql_name, [[batch]])

    return session


class DataLoaderSplit:
    """A single data split"""

    def __init__(
        self,
        file_scan_task: FileScanTask,
        scan_context: TableScanContext,
        transform_sql: str | None = None,
        table_id: TableIdentifier | None = None,
        udf_registry: UDFRegistry | None = None,
    ):
        if transform_sql is not None and table_id is None:
            raise ValueError("table_id is required when transform_sql is provided")
        self._file_scan_task = file_scan_task
        self._scan_context = scan_context
        self._transform_sql = transform_sql
        self._table_id = table_id
        self._udf_registry = udf_registry or NoOpRegistry()

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
        delete files, and partition spec lookups.
        """
        ctx = self._scan_context
        arrow_scan = ArrowScan(
            table_metadata=ctx.table_metadata,
            io=ctx.io,
            projected_schema=ctx.projected_schema,
            row_filter=ctx.row_filter,
        )

        if self._transform_sql is None:
            yield from arrow_scan.to_record_batches([self._file_scan_task])
            return

        for batch in arrow_scan.to_record_batches([self._file_scan_task]):
            yield from self._apply_transform(batch)

    def _apply_transform(self, batch: RecordBatch) -> Iterator[RecordBatch]:
        """Execute the transform SQL against a single RecordBatch."""
        if self._transform_sql is None or self._table_id is None:
            raise RuntimeError("transform_sql and table_id are required for _apply_transform")

        session = _create_transform_session(batch, self._table_id, self._udf_registry)
        df = session.sql(self._transform_sql)
        yield from df.collect()
