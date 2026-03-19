from __future__ import annotations

import pyarrow as pa
from pyiceberg.io.pyarrow import ArrowScan, schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask

from openhouse.dataloader._table_scan_context import TableScanContext


class IcebergScanDelegate:
    """Python object that the Rust TableProvider calls back into.

    Provides ``schema()`` and ``scan()`` methods used by the Rust
    ``PythonTableProvider`` to serve DataFusion queries from Iceberg data files.
    """

    def __init__(self, scan_context: TableScanContext, file_scan_task: FileScanTask) -> None:
        self._ctx = scan_context
        self._file_scan_task = file_scan_task

    def schema(self) -> pa.Schema:
        """Return the full PyArrow schema for the table."""
        return schema_to_pyarrow(self._ctx.projected_schema)

    def scan(
        self,
        projection: list[int] | None,
        filters: list[str],
        limit: int | None,
    ) -> pa.RecordBatchReader:
        """Called by DataFusion via the Rust bridge with pushdown info.

        Args:
            projection: Column indices to read, or None for all columns.
            filters: Filter expressions as display strings (best-effort;
                DataFusion applies filters post-scan for correctness).
            limit: Row limit hint, or None for unlimited.

        Returns:
            A ``pyarrow.RecordBatchReader`` streaming the requested data.
        """
        ctx = self._ctx

        if projection is not None:
            all_fields = list(ctx.projected_schema.fields)
            selected = [all_fields[i] for i in projection]
            projected_schema = Schema(*selected)
        else:
            projected_schema = ctx.projected_schema

        arrow_scan = ArrowScan(
            table_metadata=ctx.table_metadata,
            io=ctx.io,
            projected_schema=projected_schema,
            row_filter=ctx.row_filter,
        )

        batches = arrow_scan.to_record_batches([self._file_scan_task])
        pa_schema = schema_to_pyarrow(projected_schema)
        return pa.RecordBatchReader.from_batches(pa_schema, batches)
