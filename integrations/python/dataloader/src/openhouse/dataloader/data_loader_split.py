from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from types import MappingProxyType

from datafusion.context import SessionContext
from datafusion.plan import LogicalPlan
from datafusion.substrait import Producer
from pyarrow import RecordBatch
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import ArrivalOrder, FileScanTask

from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader.udf_registry import NoOpRegistry, UDFRegistry


class DataLoaderSplit:
    """A single data split"""

    def __init__(
        self,
        file_scan_task: FileScanTask,
        scan_context: TableScanContext,
        plan: LogicalPlan | None = None,
        session_context: SessionContext | None = None,
        udf_registry: UDFRegistry | None = None,
        batch_size: int | None = None,
    ):
        self._file_scan_task = file_scan_task
        self._udf_registry = udf_registry or NoOpRegistry()
        self._scan_context = scan_context
        self._batch_size = batch_size

        if (plan is None) != (session_context is None):
            raise ValueError("plan and session_context must both be provided or both be None")

        if plan is not None:
            # TODO: Deserialize back to a LogicalPlan once we integrate with DataFusion for execution.
            # The UDF registry is retained so UDFs can be re-registered on remote workers.
            assert session_context is not None  # guaranteed by the guard above
            self._udf_registry.register_udfs(session_context)
            self._plan_substrait_bytes: bytes | None = Producer.to_substrait_plan(plan, session_context).encode()
        else:
            self._plan_substrait_bytes = None

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
        yield from arrow_scan.to_record_batches(
            [self._file_scan_task],
            order=ArrivalOrder(concurrent_streams=1, batch_size=self._batch_size),
        )
