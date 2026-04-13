from __future__ import annotations

from dataclasses import dataclass, field

from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadata

from openhouse.dataloader._observability import PerfConfig
from openhouse.dataloader.table_identifier import TableIdentifier

_DEFAULT_PERF_CONFIG = PerfConfig()


def _unpickle_scan_context(
    table_metadata: TableMetadata,
    io_properties: dict[str, str],
    projected_schema: Schema,
    row_filter: BooleanExpression,
    table_id: TableIdentifier,
    worker_jvm_args: str | None = None,
    perf_config: PerfConfig = _DEFAULT_PERF_CONFIG,
) -> TableScanContext:
    return TableScanContext(
        table_metadata=table_metadata,
        io=load_file_io(properties=io_properties, location=table_metadata.location),
        projected_schema=projected_schema,
        row_filter=row_filter,
        table_id=table_id,
        worker_jvm_args=worker_jvm_args,
        perf_config=perf_config,
    )


@dataclass(frozen=True)
class TableScanContext:
    """Table-level context for reading Iceberg data files.

    Created once per table scan by OpenHouseDataLoader and shared
    across all DataLoaderSplit instances for that scan.

    Attributes:
        table_metadata: Full Iceberg table metadata (schema, properties, partition specs, etc.)
        io: FileIO configured for the table's storage location
        projected_schema: Subset of columns to read (equals table schema when no projection)
        table_id: Identifier for the table being scanned
        row_filter: Row-level filter expression pushed down to the scan
        worker_jvm_args: JVM arguments applied when the JNI JVM is created in worker processes
        perf_config: Serializable performance configuration that travels with splits
    """

    table_metadata: TableMetadata
    io: FileIO
    projected_schema: Schema
    table_id: TableIdentifier
    row_filter: BooleanExpression = AlwaysTrue()
    worker_jvm_args: str | None = None
    perf_config: PerfConfig = field(default_factory=PerfConfig)

    def __reduce__(self) -> tuple:
        return (
            _unpickle_scan_context,
            (
                self.table_metadata,
                dict(self.io.properties),
                self.projected_schema,
                self.row_filter,
                self.table_id,
                self.worker_jvm_args,
                self.perf_config,
            ),
        )
