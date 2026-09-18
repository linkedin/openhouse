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

## DML across table states

The same DML contracts run on date-partitioned tables, tables with a write order,
and tables whose schema has gained a nullable column.

Date partitioning checks that predicates and writes preserve partition semantics.
Write ordering checks that row-level changes remain logically identical when file
layout is ordered. Schema evolution checks that name-addressed reads, deletes, and
updates preserve the added column while changing only their intended values.

The expected behavior remains explicit for a known defect where deleting by a
partition predicate on a write-ordered table can fail in the Spark and Iceberg
rewrite.
