# Delta integration test coverage

The integration tests cover observable end-user behavior by multiplying
prepared tables by compatible test cases:

```text
Executable Test Set = compatible(Prepared Tables x Test Cases)
```

## Data representation

The scalar prepared table is unpartitioned, contains standard seed rows, and is
materialized in Parquet and ORC.

```text
Scalar Type Tests =
  compatible([Scalar Parquet Table, Scalar ORC Table] x [Test Cases Below])
```

| Test case | Expected behavior |
|---|---|
| Scalar values | `bigint`, `int`, `double`, `decimal(10,2)`, `string`, `binary`, `date`, `timestamp`, and `timestamp_ntz` values read back exactly. Complete DML row assertions also cover `boolean`. |
| Nulls | Every non-key scalar column accepts `NULL` and reads back as null. |
| Floating-point values | `NaN` and positive infinity read back as the same special `double` values. |
| Numeric limits | The maximum `bigint`, maximum `int`, and `99999999.99` in `decimal(10,2)` read back exactly. |
| Strings | Unicode text, including Japanese characters and an emoji, and an empty string read back exactly. |

## Reads and table changes

The standard prepared table is unpartitioned, contains three deterministic seed
rows, and is materialized in Parquet and ORC.

```text
Core DML Tests =
  compatible([Standard Parquet Table, Standard ORC Table] x [Test Cases Below])
```

| Test case | Expected behavior |
|---|---|
| Projection | Reading the string column in key order returns the values from the complete table and leaves the rows and snapshot count unchanged. |
| `INSERT INTO` | Appending two rows preserves every starting row and commits one new snapshot. |
| `INSERT OVERWRITE` | Writing two rows replaces every starting row and commits one new snapshot. |
| `DELETE` | Deleting keys below `2` removes only those rows and commits one new snapshot. |
| `UPDATE` | Updating key `2` changes only its string value, preserves every other row and column, and commits one new snapshot. |
| `MERGE` | A matched source row updates the string value for key `2`, an unmatched source row inserts key `7`, and the operation commits one new snapshot. |

## Invalid operations

The rejection tests use the same standard Parquet and ORC prepared tables.

```text
DML Rejection Tests =
  compatible([Standard Parquet Table, Standard ORC Table] x [Test Cases Below])
```

| Test case | Expected behavior |
|---|---|
| `DELETE` with an unknown column | Analysis rejects the statement and names the missing column. |
| `DELETE` with `rand()` in its predicate | Analysis rejects the nondeterministic predicate. |
| `UPDATE` with `rand()` in its predicate | Analysis rejects the nondeterministic predicate. |
| `INSERT INTO` with too few values | Analysis rejects the statement because data columns are missing. |
| `MERGE` assigning one target column twice | Analysis rejects the conflicting assignments. |
| `MERGE` matching two source rows to one target row | Execution reports the cardinality violation and preserves the complete table state. |

[How integration tests are generated](CAPABILITY-MATRIX.md) explains how
compatibility selects the prepared-table and test-case pairs in these equations.

## Standard DML

Standard DML uses seeded unpartitioned tables, tables containing a null value,
and date-partitioned tables. Each prepared table is materialized in Parquet and
ORC.

```text
Standard DML Tests =
  compatible([Standard, Null-Containing, Date-Partitioned Prepared Tables]
    x [Test Cases Below])
```

| Test case | Expected behavior |
|---|---|
| Filtered read | Predicates return exactly the matching rows and leave table state unchanged. |
| Insert values | Literal rows append without changing existing rows. |
| Insert query | Query results append with the target column mapping preserved. |
| DataFrame write | Appended DataFrame rows match the input values and schema. |
| Full-table overwrite | New rows replace the complete prior contents. |
| Partition overwrite | New rows replace only the selected partition and preserve every other partition. |
| Delete | Predicates, subqueries, aliases, and whole-table conditions remove exactly the selected rows. |
| Update | Predicates, subqueries, expressions, aliases, multi-column assignments, partition moves, and null assignments change only the selected rows and columns. |
| Merge | Matched, unmatched, conditional, wildcard, common-table-expression, and set-operation sources apply their clauses to the exact target rows. |

## Replace table as select

Replacement prepared tables have rows, schema, and lineage produced by `CREATE
OR REPLACE TABLE AS SELECT`. They include unpartitioned and date-partitioned
tables in Parquet and ORC.

```text
RTAS Tests =
  compatible([Replacement Prepared Tables]
    x [Standard DML Test Cases, RTAS Test Cases Below])
```

| Test case | Expected behavior |
|---|---|
| Enablement and replication restrictions | Unsupported replacements fail and preserve the current table. |
| Same-shape replacement | The selected rows replace the current rows and create the expected replacement lineage. |
| DML after replacement | Compatible Standard DML test cases retain their row and snapshot behavior after replacement. |
| Schema replacement | The new schema and rows become current without retaining removed fields. |
| Partition replacement | The new partition specification becomes current and subsequent writes use it. |
| Properties and policies | Required table metadata survives replacement or changes to the requested value. |
| Time travel and recovery | Historical snapshots remain readable and the selected snapshot can become current again. |
| Changelog and incremental reads | Replacement produces the expected inserted and deleted rows across the requested snapshot range. |
| Rename and sort order | Replacement remains correct before and after rename, and the requested sort order remains visible. |
| Creator identity | Replacement records the expected creator metadata. |
| Concurrent replacement and append | Successful commits preserve both operations; otherwise one operation reports a conflict. |
| Narrowing a `bigint` | Values outside the target range cause rejection instead of wrapping. |
