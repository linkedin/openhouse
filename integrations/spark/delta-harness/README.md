# OpenHouse Delta harness

The Delta harness defines OpenHouse behavior as reusable Scala test cases. The same
scenario catalog runs in two environments:

- This repository runs it against an embedded OpenHouse server.
- The li-openhouse acceptance suite supplies a remote `Ctx` and runs the portable
  catalog as Airflow shards.

The scenario definitions and assertions stay in this module. Environment adapters
provide only the catalog connection, runtime dependencies, and execution policy.

## Run locally

The harness requires JDK 17. Run commands from the repository root.

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

./gradlew --no-daemon \
  :integrations:spark:openhouse-spark-delta-harness_2.12:runOpenHouse
```

Pass case ID substrings through `--args` to select a smaller slice:

```bash
./gradlew --no-daemon \
  :integrations:spark:openhouse-spark-delta-harness_2.12:runOpenHouse \
  --args='merge.upsert parquet'
```

Every supplied substring must occur in the case ID. With no filters,
`runOpenHouse` runs the complete catalog in the current checkout.

The wrapper script performs the same run:

```bash
export JAVA17_HOME=$(/usr/libexec/java_home -v 17)
integrations/spark/delta-harness/run-openhouse.sh rtas.schema parquet
```

`HARNESS_PARALLELISM` controls concurrent case attempts. It must be a positive
integer. Set it to `1` when diagnosing order-sensitive product or service behavior:

```bash
HARNESS_PARALLELISM=1 \
  integrations/spark/delta-harness/run-openhouse.sh merge.upsert
```

Run the core catalog:

```bash
./gradlew --no-daemon \
  :integrations:spark:openhouse-spark-delta-harness_2.12:verifyOpenHouseFoundation
```

The module attaches that core catalog run to Gradle `check`, so the representative
embedded behavior cannot drift unnoticed.

Run the Spark-free framework and catalog tests with:

```bash
./gradlew --no-daemon \
  :integrations:spark:openhouse-spark-delta-harness_2.12:test
```

## How tests are structured

Each test describes three things:

1. The table state that exists before the test.
2. The SQL query or statement under test.
3. The complete rows, snapshots, metadata, or error expected afterward.

A DML operation is described once and runs against every compatible prepared
table. Adding a partitioned, ordered, evolved, or otherwise specialized table
therefore exercises the existing DML behavior without redefining it.

Every test creates a fresh table, records its starting state, runs one behavior,
and checks the complete observable result. Rejected statements also verify the
error type and diagnostic. When an invalid statement can reach execution, the
test verifies that rows and snapshots remain unchanged.

[TEST-COVERAGE.md](TEST-COVERAGE.md) describes the assertions made by the
executable catalog in the current checkout.
[CAPABILITY-MATRIX.md](CAPABILITY-MATRIX.md) shows the DML operations, prepared
tables, and the dimensions across which the tests run.

## Skipped tests

A test for a known product defect remains in the catalog with its intended
assertion. It reports the defect as its skip reason until the product is fixed,
then the same assertion becomes the regression test.

A test that depends on a service unavailable in the embedded environment is
skipped only in the local run. The li-openhouse acceptance environment still
runs the assertion.

The coverage document records every skipped test and the behavior that remains
to be validated.

## Extend coverage

### Add a DML operation

Describe the SQL statement and its complete expected result once. Add it to the
operation group used by every compatible prepared table.

### Add a prepared table

Describe the table's schema, partitioning, ordering, properties, and starting
rows. Select the existing DML operations that apply to it. Those operations
then run against the new table without duplicating their definitions.

### Add another behavior

Describe the starting table, action, and expected outcome together so a reader
can understand the test without consulting implementation terminology.

For every coverage change, update [TEST-COVERAGE.md](TEST-COVERAGE.md) with the
observable behavior and [CAPABILITY-MATRIX.md](CAPABILITY-MATRIX.md) with the new
operation or prepared table.
