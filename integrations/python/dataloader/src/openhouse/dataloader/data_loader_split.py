from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Iterator, Mapping
from types import MappingProxyType

from datafusion.plan import LogicalPlan
from pyarrow import RecordBatch
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import FileScanTask

from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader.udf_registry import NoOpRegistry, UDFRegistry

logger = logging.getLogger(__name__)

_DEFAULT_LOG_EVERY_N_ROWS = 512_000

_BYTE_UNITS = [("B", 1), ("KiB", 1024), ("MiB", 1024**2), ("GiB", 1024**3), ("TiB", 1024**4)]


def _fmt_bytes(n: int) -> str:
    """Format a byte count with the most appropriate unit."""
    for unit, threshold in reversed(_BYTE_UNITS):
        if n >= threshold:
            return f"{n / threshold:.1f} {unit}"
    return f"{n} B"


class DataLoaderSplit:
    """A single data split"""

    def __init__(
        self,
        file_scan_task: FileScanTask,
        scan_context: TableScanContext,
        plan: LogicalPlan | None = None,
        udf_registry: UDFRegistry | None = None,
        log_every_n_rows: int = _DEFAULT_LOG_EVERY_N_ROWS,
    ):
        self._plan = plan
        self._file_scan_task = file_scan_task
        self._udf_registry = udf_registry or NoOpRegistry()
        self._scan_context = scan_context
        self._log_every_n_rows = log_every_n_rows

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

    def _track_batches(self, batches: Iterator[RecordBatch]) -> Iterator[RecordBatch]:
        """Wrap a batch iterator with throughput tracking and periodic logging."""
        total_rows = 0
        total_bytes = 0
        batch_count = 0
        elapsed = 0.0
        rows_since_last_log = 0
        split_id = self.id[:12]

        it = iter(batches)
        while True:
            start_time = time.monotonic()
            try:
                batch = next(it)
            except StopIteration:
                elapsed += time.monotonic() - start_time
                break
            elapsed += time.monotonic() - start_time
            total_rows += batch.num_rows
            total_bytes += batch.nbytes
            batch_count += 1
            rows_since_last_log += batch.num_rows
            if rows_since_last_log >= self._log_every_n_rows:
                rows_since_last_log = 0
                self._log_progress(split_id, batch_count, total_rows, total_bytes, elapsed)

            yield batch

        self._log_progress(split_id, batch_count, total_rows, total_bytes, elapsed)

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
        yield from self._track_batches(arrow_scan.to_record_batches([self._file_scan_task]))

    @staticmethod
    def _log_progress(split_id: str, batch_count: int, total_rows: int, total_bytes: int, elapsed: float) -> None:
        rate_bytes = total_bytes / elapsed if elapsed > 0 else 0
        logger.info(
            "Split %s: %d batches, %d rows, %s materialized in %.3fs (%.0f rows/s, %s/s)",
            split_id,
            batch_count,
            total_rows,
            _fmt_bytes(total_bytes),
            elapsed,
            total_rows / elapsed if elapsed > 0 else 0,
            _fmt_bytes(int(rate_bytes)),
        )
