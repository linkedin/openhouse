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

## Merge-on-read

Merge-on-read tests use unpartitioned, date-partitioned, and replacement-lineage
prepared tables whose row-level changes create position-delete files.

```text
Merge-on-Read Tests =
  compatible([Merge-on-Read Prepared Tables]
    x [Standard DML Test Cases, Test Cases Below])
```

| Test case | Expected behavior |
|---|---|
| Standard DML | Reads and mutations retain their logical row and snapshot behavior while position deletes are present. |
| Create position deletes | Row-level mutations create delete files that identify only the removed rows. |
| Persist write mode | The configured merge-on-read mode remains visible after writes and table changes. |
| Read with live deletes | Queries exclude deleted rows and return every remaining row. |
| Write with live deletes | Later writes preserve existing deletes and produce the expected current rows. |
| Metadata and changelog | Metadata tables and changelog scans expose the delete-file effects. |
| Replication | Replicated state preserves the logical rows and merge-on-read metadata. |
| Time travel and rollback | Historical reads and rollback return the expected rows before and after deletes. |
| Maintenance | Compaction removes obsolete delete state while preserving current rows. |

## Branches and write-audit-publish

These tests use prepared tables with branches, tags, divergent histories,
staged snapshots, and merge-on-read delete files.

```text
Branch and WAP Tests =
  compatible([Branched, Tagged, Divergent, Staged Prepared Tables]
    x [Test Cases Below])
```

| Test case | Expected behavior |
|---|---|
| Create branch or tag | The new reference points to the requested snapshot without changing the main reference. |
| Scoped write | A write through one branch changes only that branch until publication. |
| Merge or fast-forward | The target reference advances to the expected source state and preserves committed rows. |
| Cherry-pick | The selected snapshot is applied once with its expected rows and metadata. |
| Divergent histories | An unsupported merge reports a conflict and preserves both references. |
| Delete reference | The selected branch or tag disappears while retained references remain readable. |
| Reference interactions | Time travel, rename, maintenance, table evolution, storage formats, and merge-on-read retain their expected behavior on references. |
| Stage inserts and DML | Staged changes remain outside the current table state until publication. |
| Independent staging identifiers | Each identifier exposes only its own staged changes. |
| Publish staged changes | Cherry-pick and `publish_changes` make the selected staged snapshot current exactly once. |
| Reject invalid publication | Repeated publication and expired staged snapshots fail without changing the current table. |
