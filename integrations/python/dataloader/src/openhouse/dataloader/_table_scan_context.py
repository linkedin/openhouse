from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadata


def _unpickle_scan_context(
    table_metadata: TableMetadata,
    io_properties: dict[str, str],
    projected_schema: Schema,
    row_filter: BooleanExpression,
) -> TableScanContext:
    return TableScanContext(
        table_metadata=table_metadata,
        io=load_file_io(properties=io_properties),
        projected_schema=projected_schema,
        row_filter=row_filter,
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
        row_filter: Row-level filter expression pushed down to the scan
    """

    table_metadata: TableMetadata
    io: FileIO
    projected_schema: Schema
    row_filter: BooleanExpression = AlwaysTrue()

    def __reduce__(self) -> tuple:
        return (
            _unpickle_scan_context,
            (self.table_metadata, self.io.properties, self.projected_schema, self.row_filter),
        )
