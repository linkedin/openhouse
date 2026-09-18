# Delta harness test coverage

This document describes only behavior exercised by the executable catalog in the
current checkout. The core catalog contains 34 cases: 17 behavior contracts run
against both Parquet and ORC. All 34 cases pass against the embedded OpenHouse
catalog, with no skips.

## Scalar values

Ten cases exercise five scalar-value contracts on an unpartitioned table in each
file format.

| Case family | Behavior asserted |
|-------------|-------------------|
| `types.roundtrip` | Seeded bigint, integer, double, decimal, string, binary, date, timestamp, and timestamp-without-time-zone values read back exactly. |
| `types.nulls` | A row with every non-key value set to `NULL` reads back with all eight non-key columns null. |
| `types.specialFloats` | Inserted `NaN` and positive infinity values retain their special floating-point semantics. |
| `types.boundaries` | `Long.MaxValue`, `Int.MaxValue`, and `99999999.99` in `decimal(10,2)` read back unchanged. |
| `types.unicodeAndEmpty` | A Unicode string and an empty string remain distinct and read back unchanged. |

## Reads and mutations

Twelve cases exercise six read or mutation contracts on the same seeded
three-row table in each file format.

| Case family | Behavior asserted |
|-------------|-------------------|
| `read.projection` | A projected string column agrees with the same column in the complete table state, and the read changes neither rows nor snapshots. |
| `insert.into` | Two literal rows are appended, all prepared rows remain unchanged, and one snapshot is committed. |
| `insert.overwrite` | The prepared rows are replaced by the two expected rows, and one snapshot is committed. |
| `delete.byPredicate` | Rows below the predicate boundary are removed, every other row remains unchanged, and one snapshot is committed. |
| `update.byPredicate` | Only the selected row and column change, every other value remains unchanged, and one snapshot is committed. |
| `merge.upsert` | One matching row is updated, one unmatched row is inserted, every other row remains unchanged, and one snapshot is committed. |

## Rejected DML

Twelve cases exercise six rejected-statement contracts on the seeded table in each
file format.

| Case family | Behavior asserted |
|-------------|-------------------|
| `dmlValidation.nonExistentColumn` | A `DELETE` predicate that names an undeclared column fails with an analysis error that identifies the column. |
| `dmlValidation.nonDeterministicDelete` | A `DELETE` with `rand()` in its predicate fails with an analysis error that identifies the determinism requirement. |
| `dmlValidation.nonDeterministicUpdate` | An `UPDATE` with `rand()` in its predicate fails with an analysis error that identifies the determinism requirement. |
| `dmlValidation.insertArity` | An `INSERT` with too few values fails with an analysis error about missing data columns. |
| `dmlValidation.mergeConflictingUpdates` | A `MERGE` that assigns the same target column twice fails with a multiple-assignment analysis error. |
| `dmlValidation.mergeCardinalityViolation` | A `MERGE` whose source matches one target row twice reports the cardinality violation and leaves rows and snapshots unchanged. |

## Execution and lifecycle checks

The Spark-free module tests verify behavior that protects the validity of every
catalog result:

- Catalog contribution order and all 34 case IDs are stable, and case IDs are
  unique.
- Table layouts read the configured data source when they execute, so an
  environment adapter can select the deployed catalog without rebuilding the
  case list.
- `HARNESS_PARALLELISM` accepts positive integers, rejects invalid values, and
  uses a safe fallback when the runtime reports no available processors.
- Retries apply only to recognized transient failures while creating a fresh
  Spark session. A failure after a case body starts, including cleanup failure,
  is terminal.
- Exception-cause traversal terminates for cyclic cause chains.
- Cleanup runs only after the harness owns the table. A case failure remains
  primary when cleanup also fails, while a cleanup failure after a successful
  body is reported directly.
- Generated table names remain unique under concurrent generation and counter
  resets.
- Core seed SQL and date rollover remain deterministic.
- Gradle `check` verifies that the published jar contains the portable `Plan`
  and `Runner` entry points and excludes the embedded launcher and server
  environment.
