from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping, Sequence
from types import MappingProxyType

from datafusion.context import SessionContext
from pyarrow import RecordBatch
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import ArrivalOrder, FileScanTask

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
    """A data split that reads one or more files."""

    def __init__(
        self,
        file_scan_tasks: Sequence[FileScanTask],
        scan_context: TableScanContext,
        transform_sql: str | None = None,
        udf_registry: UDFRegistry | None = None,
        batch_size: int | None = None,
    ):
        self._file_scan_tasks = list(file_scan_tasks)
        if not self._file_scan_tasks:
            raise ValueError("file_scan_tasks must not be empty")
        self._scan_context = scan_context
        self._transform_sql = transform_sql
        self._udf_registry = udf_registry or NoOpRegistry()
        self._batch_size = batch_size

    @property
    def id(self) -> str:
        """Unique ID for the split. This is stable across executions for a given
        snapshot and split size.
        """
        paths = sorted(t.file.file_path for t in self._file_scan_tasks)
        combined = "\0".join(paths)
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()

    @property
    def table_properties(self) -> Mapping[str, str]:
        """Properties of the table being loaded"""
        return MappingProxyType(self._scan_context.table_metadata.properties)

    def __iter__(self) -> Iterator[RecordBatch]:
        """Reads the file scan tasks and yields Arrow RecordBatches.

        When the split contains multiple files, they are read concurrently.
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

        batches = arrow_scan.to_record_batches(
            self._file_scan_tasks,
            order=ArrivalOrder(concurrent_streams=len(self._file_scan_tasks), batch_size=self._batch_size),
        )

        if self._transform_sql is None:
            yield from batches
        else:
            # Materialize the first batch before creating the transform session
            # so that the HDFS JVM starts (and picks up worker_jvm_args) before
            # any UDF registration code can trigger JNI.
            batch_iter = iter(batches)
            first = next(batch_iter, None)
            if first is None:
                return
            session = _create_transform_session(self._scan_context.table_id, self._udf_registry)
            yield from self._apply_transform(session, first)
            for batch in batch_iter:
                yield from self._apply_transform(session, batch)

    def _apply_transform(self, session: SessionContext, batch: RecordBatch) -> Iterator[RecordBatch]:
        """Execute the transform SQL against a single RecordBatch."""
        _bind_batch_table(session, self._scan_context.table_id, batch)
        df = session.sql(self._transform_sql)  # type: ignore[arg-type]  # caller guarantees not None
        yield from df.collect()
