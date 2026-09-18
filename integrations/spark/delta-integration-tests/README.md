# OpenHouse Delta integration tests

The Delta integration tests combine prepared tables with compatible test cases.
This repository runs the generated set against embedded OpenHouse. li-openhouse
runs the same set as Airflow acceptance tests against real clusters.

## Run locally

Run all integration tests from the repository root:

```bash
./gradlew :integrations:spark:openhouse-spark-delta-integration-tests_2.12:runOpenHouse
```

## Filter tests

The Gradle task accepts optional case-ID substrings. Every supplied substring
must occur in the case ID:

```bash
./gradlew \
  :integrations:spark:openhouse-spark-delta-integration-tests_2.12:runOpenHouse \
  --args='merge.upsert parquet'
```

Run the core integration tests:

```bash
./gradlew \
  :integrations:spark:openhouse-spark-delta-integration-tests_2.12:verifyOpenHouseFoundation
```

Run the Spark-free unit tests:

```bash
./gradlew \
  :integrations:spark:openhouse-spark-delta-integration-tests_2.12:test
```

## How tests are structured

Each generated test combines two inputs:

1. A prepared table defines the initial state, including its storage format,
   schema, rows, partitioning, ordering, properties, and history.
2. A test case defines the action and the complete rows, snapshots, metadata, or
   error expected afterward.

Compatibility determines which prepared-table and test-case pairs enter the
executable test set. A DML test case is defined once and runs against every
compatible prepared table.

Every generated test creates a fresh instance of its prepared table, runs its
test case, and checks the complete observable result. Rejected statements also
verify the error type and diagnostic. When an invalid statement can reach
execution, the test verifies that rows and snapshots remain unchanged.

[TEST-COVERAGE.md](TEST-COVERAGE.md) describes the assertions made by the
executable catalog in the current checkout.
[CAPABILITY-MATRIX.md](CAPABILITY-MATRIX.md) shows how prepared tables and test
cases multiply into the executable test set.

## Skipped tests

A test for a known product defect remains in the catalog with its intended
assertion. It reports the defect as its skip reason until the product is fixed,
then the same assertion becomes the regression test.

A test that depends on a service unavailable in the embedded environment is
skipped only in the local run. The li-openhouse acceptance environment still
runs the assertion.

The coverage document records every skipped test and the behavior that remains
to be validated.
