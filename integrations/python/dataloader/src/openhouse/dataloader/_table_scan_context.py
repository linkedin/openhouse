from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.io import FileIO
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadata


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
