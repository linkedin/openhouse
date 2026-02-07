from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyarrow import RecordBatch
from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask
from pyiceberg.table.metadata import TableMetadata

if TYPE_CHECKING:
    from datafusion.plan import LogicalPlan

    from openhouse.dataloader.udf_registry import UDFRegistry


@dataclass(frozen=True)
class TableScanContext:
    """Table-level context for reading Iceberg data files.

    Created once per table scan by OpenHouseDataLoader and shared
    across all DataLoaderSplit instances for that scan.

    Attributes:
        table_metadata: Full Iceberg table metadata (schema, properties, partition specs, etc.)
        io: FileIO configured for the table's storage location
        projected_schema: Subset of columns to read (equals table schema when no projection)
        row_filter: Row-level filter expression pushed down to the scan
    """

    table_metadata: TableMetadata
    io: FileIO
    projected_schema: Schema
    row_filter: BooleanExpression = AlwaysTrue()


class DataLoaderSplit:
    """A single data split"""

    def __init__(
        self,
        plan: LogicalPlan,
        file_scan_task: FileScanTask,
        udf_registry: UDFRegistry,
        scan_context: TableScanContext,
    ):
        self._plan = plan
        self._file_scan_task = file_scan_task
        self._udf_registry = udf_registry
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
        yield from arrow_scan.to_record_batches([self._file_scan_task])
