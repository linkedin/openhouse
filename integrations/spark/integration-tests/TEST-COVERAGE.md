# OpenHouse integration test coverage

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

## Catalog constraints

Catalog-constraint tests use standard prepared tables in Parquet and ORC.

```text
Catalog Constraint Tests =
  compatible([Standard Parquet Table, Standard ORC Table] x [Test Cases Below])
```

| Test case | Expected behavior |
|---|---|
| Accepted table property change | The requested property is committed and the table remains readable. |
| Accepted storage format | The catalog records the selected format and subsequent writes use it. |
| Accepted partition change | The catalog records the requested partition metadata and later writes remain readable. |
| Rejected property change | The diagnostic identifies the violated rule and preserves the prior metadata and rows. |
| Rejected storage format | The operation fails before creating an unusable table or side object. |
| Rejected partition evolution | The operation preserves the existing partition specification and readable table state. |
