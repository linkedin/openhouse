# Delta harness capability matrix

This matrix summarizes only behavior present in the executable catalog in the
current checkout. The DML operation list is defined once, then the prepared-table
table shows where those operations run. See [TEST-COVERAGE.md](TEST-COVERAGE.md)
for the exact assertions.

## Catalog totals

| Capability set | Added cases | Catalog total | Embedded result |
|----------------|------------:|--------------:|-----------------|
| Scalar values, DML, and rejected DML | 34 | 34 | 34 passed, 0 skipped, 0 failed |

## DML operations

Every operation below runs on each compatible prepared table listed in the next
section.

| Operation | Cases | Observable contract |
|-----------|------:|---------------------|
| Projection | 2 | A selected column matches the complete table state, and the read changes neither rows nor snapshots. |
| Append | 2 | Two rows are added, prepared rows remain unchanged, and one snapshot is committed. |
| Full overwrite | 2 | Prepared rows are replaced by the expected rows, and one snapshot is committed. |
| Predicate delete | 2 | Matching rows are removed, all other rows remain unchanged, and one snapshot is committed. |
| Predicate update | 2 | Only the selected row and column change, and one snapshot is committed. |
| Merge upsert | 2 | One row is updated, one row is inserted, all other rows remain unchanged, and one snapshot is committed. |

## Prepared tables

| Prepared table | Formats | Initial state | Tests |
|----------------|---------|---------------|-------|
| Core unpartitioned table | Parquet, ORC | Three rows with long, integer, string, double, boolean, and timestamp-string columns. | All six DML operations and all six rejected-DML statements. |
| Scalar-value table | Parquet, ORC | Three rows covering bigint, integer, double, decimal, string, binary, date, timestamp, and timestamp-without-time-zone columns. | Round trip, nulls, special floating-point values, numeric boundaries, Unicode, and empty strings. |

## Rejected DML

| Statement | Cases | Expected result |
|-----------|------:|-----------------|
| Delete using an undeclared column | 2 | Analysis fails and names the missing column. |
| Delete using a nondeterministic predicate | 2 | Analysis fails and identifies the determinism requirement. |
| Update using a nondeterministic predicate | 2 | Analysis fails and identifies the determinism requirement. |
| Insert with too few values | 2 | Analysis fails because data columns are missing. |
| Merge with duplicate target assignments | 2 | Analysis fails because one target column is assigned more than once. |
| Merge with two source rows matching one target row | 2 | Execution reports the cardinality violation and leaves the table unchanged. |
