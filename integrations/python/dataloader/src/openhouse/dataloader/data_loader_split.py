from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from types import MappingProxyType

from datafusion.context import SessionContext
from pyarrow import RecordBatch
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import FileScanTask

from openhouse.dataloader._jvm import apply_libhdfs_opts
from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader.filters import _quote_identifier
from openhouse.dataloader.table_identifier import TableIdentifier
from openhouse.dataloader.udf_registry import NoOpRegistry, UDFRegistry


def to_sql_identifier(table_id: TableIdentifier) -> str:
    """Return the quoted DataFusion SQL identifier, e.g. ``"db"."tbl"``."""
    return f"{_quote_identifier(table_id.database)}.{_quote_identifier(table_id.table)}"


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
    name = to_sql_identifier(table_id)
    session.deregister_table(name)
    session.register_record_batches(name, [[batch]])


class DataLoaderSplit:
    """A single data split"""

    def __init__(
        self,
        file_scan_task: FileScanTask,
        scan_context: TableScanContext,
        transform_sql: str | None = None,
        udf_registry: UDFRegistry | None = None,
    ):
        self._file_scan_task = file_scan_task
        self._scan_context = scan_context
        self._transform_sql = transform_sql
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
        if ctx.worker_jvm_args is not None:
            apply_libhdfs_opts(ctx.worker_jvm_args)
        arrow_scan = ArrowScan(
            table_metadata=ctx.table_metadata,
            io=ctx.io,
            projected_schema=ctx.projected_schema,
            row_filter=ctx.row_filter,
        )

        batches = arrow_scan.to_record_batches([self._file_scan_task])

        if self._transform_sql is None:
            yield from batches
        else:
            # Materialize the first batch before creating the transform session
            # so that the HDFS JVM starts (and picks up worker_jvm_args) before
            # any UDF registration code can trigger JNI.
            batch_iter = iter(batches)
            first = next(batch_iter, None)
            session = _create_transform_session(self._scan_context.table_id, self._udf_registry)
            if first is not None:
                yield from self._apply_transform(session, first)
                for batch in batch_iter:
                    yield from self._apply_transform(session, batch)

    def _apply_transform(self, session: SessionContext, batch: RecordBatch) -> Iterator[RecordBatch]:
        """Execute the transform SQL against a single RecordBatch."""
        _bind_batch_table(session, self._scan_context.table_id, batch)
        df = session.sql(self._transform_sql)  # type: ignore[arg-type]  # caller guarantees not None
        yield from df.collect()
