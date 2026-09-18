# What the Delta harness tests

The harness tests observable end-user behavior. Each test case runs against
compatible prepared tables, so the same operation is checked under different
initial conditions and storage formats.

## Data representation

The data-type tests require these values to survive a write and read without
changing:

| Test | Expected behavior |
|---|---|
| Scalar values | `bigint`, `int`, `double`, `decimal(10,2)`, `string`, `binary`, `date`, `timestamp`, and `timestamp_ntz` values read back exactly. Complete DML row assertions also cover `boolean`. |
| Nulls | Every non-key scalar column accepts `NULL` and reads back as null. |
| Floating-point values | `NaN` and positive infinity read back as the same special `double` values. |
| Numeric limits | The maximum `bigint`, maximum `int`, and `99999999.99` in `decimal(10,2)` read back exactly. |
| Strings | Unicode text, including Japanese characters and an emoji, and an empty string read back exactly. |

## Reads and table changes

Each operation has a concrete expected result:

| Operation | Expected behavior |
|---|---|
| Projection | Reading the string column in key order returns the values from the complete table and leaves the rows and snapshot count unchanged. |
| `INSERT INTO` | Appending two rows preserves every starting row and commits one new snapshot. |
| `INSERT OVERWRITE` | Writing two rows replaces every starting row and commits one new snapshot. |
| `DELETE` | Deleting keys below `2` removes only those rows and commits one new snapshot. |
| `UPDATE` | Updating key `2` changes only its string value, preserves every other row and column, and commits one new snapshot. |
| `MERGE` | A matched source row updates the string value for key `2`, an unmatched source row inserts key `7`, and the operation commits one new snapshot. |

## Invalid operations

Invalid operations must fail with a diagnostic that identifies the problem:

| Operation | Expected behavior |
|---|---|
| `DELETE` with an unknown column | Analysis rejects the statement and names the missing column. |
| `DELETE` with `rand()` in its predicate | Analysis rejects the nondeterministic predicate. |
| `UPDATE` with `rand()` in its predicate | Analysis rejects the nondeterministic predicate. |
| `INSERT INTO` with too few values | Analysis rejects the statement because data columns are missing. |
| `MERGE` assigning one target column twice | Analysis rejects the conflicting assignments. |
| `MERGE` matching two source rows to one target row | Execution reports the cardinality violation and preserves the complete table state. |

## Reuse across table states

Read and DML test cases are independent of the table states where they run. As
the suite adds prepared tables with partitioning, ordering, schema evolution,
replacement lineage, delete files, references, or policies, compatible test
cases run against those new initial conditions.

[How Delta harness coverage multiplies](CAPABILITY-MATRIX.md) explains that
multiplication model and the role of the embedded and acceptance environments.

## Standard DML

The DML contract covers filtered reads; inserts from values, queries, and data
frames; full-table and partition-scoped overwrites; deletes by predicates,
subqueries, aliases, and whole-table conditions; updates by predicates,
subqueries, expressions, aliases, multiple columns, partition moves, and null
assignment; and merges with matched, unmatched, conditional, wildcard, common
table expression, and set-operation sources.

Most operations run on the seeded unpartitioned table. A null-sensitive delete
runs on the same table with an additional null value, while partition-scoped
overwrites run on a date-partitioned table. Each compatible operation runs in
Parquet and ORC with the same complete-row and snapshot assertions.

## Replace table as select

Replacement adds prepared tables whose current rows and schema arrived through
`CREATE OR REPLACE TABLE AS SELECT`. The standard DML contracts run again on
unpartitioned and date-partitioned replacement lineages, including null-sensitive
deletes and partition-scoped writes.

Focused replacement behavior covers enablement and replication restrictions,
same-shape replacement, writes after replacement, schema and partition changes,
property and policy preservation, time travel and snapshot recovery, changelog
and incremental reads across replacement, rename ordering, sort-order changes,
creator identity, and concurrent replacement and append.

The intended behavior remains documented where the product is currently unsafe:
narrowing a bigint column must not silently wrap values, and a racing append must
not report success while discarding the replacement.

## Maintenance and planning

Maintenance coverage verifies the visible effects of snapshot expiration, data
file rewrites, manifest rewrites, and orphan-file removal. Each procedure must
select the intended files, preserve live rows, and leave snapshots and metadata in
the expected state.

Planning coverage checks how filters and table layouts become scan tasks and
compaction groups. Metadata-table queries expose the files, manifests, snapshots,
and sequence information used to explain those decisions.
