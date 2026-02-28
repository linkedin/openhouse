from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping

from datafusion.plan import LogicalPlan
from pyarrow import RecordBatch
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import FileScanTask

from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader._timer import log_duration
from openhouse.dataloader.udf_registry import NoOpRegistry, UDFRegistry

logger = logging.getLogger(__name__)


class DataLoaderSplit:
    """A single data split"""

    def __init__(
        self,
        file_scan_task: FileScanTask,
        scan_context: TableScanContext,
        plan: LogicalPlan | None = None,
        udf_registry: UDFRegistry | None = None,
    ):
        self._plan = plan
        self._file_scan_task = file_scan_task
        self._udf_registry = udf_registry or NoOpRegistry()
        self._scan_context = scan_context

    @property
    def table_properties(self) -> Mapping[str, str]:
        """Properties of the table being loaded"""
        return self._scan_context.table_metadata.properties

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
        it = iter(arrow_scan.to_record_batches([self._file_scan_task]))
        while True:
            with log_duration(logger, "record_batch %s", self._file_scan_task.file.file_path):
                batch = next(it, None)
            if batch is None:
                break
            yield batch
