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

Run the fixed 34-case core catalog:

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

## Architecture

The harness separates execution mechanics, reusable table preparations, behavior
definitions, and catalog composition.

### Framework

`Framework.scala` defines:

- `Ctx`, the Spark session and namespace supplied by an environment adapter.
- `TestCase`, one stable case ID and its `Ctx => Unit` body.
- `TableTest` and `TablePreparation`, immutable preparation steps for a fresh table.
- `PreparedTable`, the live table, typed schema, prepared rows, and snapshot count.
- `DmlTestCase`, a reusable operation that can run on compatible preparations.
- `Outcome`, retry classification, skip policy, and ownership-safe cleanup.

`Runner.scala` contains the portable execution contract. It validates
`HARNESS_PARALLELISM`, runs each case in a fresh Spark session, retries only
transient connection failures while creating that session, and returns results in
catalog order. Once a case body starts, every failure is terminal because the case
may have changed observable table state.

`Env.scala` and `LocalRunner.scala` are embedded-only. `Env` starts the local
OpenHouse services and configures Spark. `LocalRunner` filters the catalog, invokes
`Runner`, and prints the local result report.

### Table fixtures and behavioral scenarios

`TableTestFixtures.scala` defines the table primitives used by the core catalog:

- The typed core `Schema` and its `Column[T]` values.
- Deterministic row generation.
- Parquet and ORC layouts.
- Standard unpartitioned preparations.
- The late-bound data source that adapters override before reading the catalog.

Each capability owns its specialized starting states. A `Scenario*` trait
contributes behavioral cases, and a `*TableFixtures` trait provides the table
construction or prepared state those cases consume.

Each case creates its own table. A preparation marks ownership only after `CREATE
TABLE` succeeds. Cleanup drops only owned artifacts. If both a case and cleanup
fail, the case failure remains primary and the cleanup failure is suppressed.

### Catalog composition

`Catalog.scala` is the only catalog assembly point.

`Catalog.foundationContributions` contains the stable 34-case core catalog:

| Contribution | Cases | Purpose |
|--------------|------:|---------|
| `dataTypeCases` | 10 | Scalar values, nulls, boundaries, special floating values, and strings. |
| `dmlCoreCases` | 12 | Six representative DML operations across Parquet and ORC. |
| `dmlRejectionCases` | 12 | Rejected DML forms and their observable diagnostics. |

`Catalog.extensionContributions` contains additional behavior registered by the
current checkout. A capability integrates at two explicit points:

1. Mix its scenario trait into `Scenarios`.
2. Register its named case list in `extensionContributions`.

`Catalog.contributions` sorts named contributions for deterministic composition.
`Catalog.cases` flattens them, and `Catalog.caseIds` exposes their stable IDs without
starting Spark.

`Plan` is the published facade used by adapters. It exposes the case type,
constructor, catalog, IDs, and known-bug reason without duplicating catalog state.

### Test coverage

The core catalog exercises generated scalar values, successful reads and mutations,
and rejected mutations. Additional scenarios extend the same catalog composition
and execution contracts.

[TEST-COVERAGE.md](TEST-COVERAGE.md) describes the assertions made by the
executable catalog in the current checkout.
[CAPABILITY-MATRIX.md](CAPABILITY-MATRIX.md) provides a concise inventory of those
tested product contracts and the supporting framework checks.

## Published boundary

The portable library publishes the framework, `Runner`, table fixtures, scenario
traits, and `Catalog`. It excludes only the embedded environment and launcher:

```groovy
def embeddedOnlySources = [
  'harness/openhouse/Env.scala',
  'harness/openhouse/LocalRunner.scala'
]
```

Spark, Iceberg, and the OpenHouse runtime are `compileOnly` dependencies of the
published jar. A consumer supplies those dependencies and constructs its own
`Ctx`.

This boundary keeps retry policy and configuration parsing portable and directly
testable while preventing the embedded OpenHouse server fixtures from leaking into
the acceptance artifact. `verifyPortableJar`, which is attached to `check`, requires
the published `Plan` and `Runner` classes and rejects the embedded `Main` and
`OpenHouseEnv` classes.

## Case identity

A generated case ID combines an operation and a preparation label:

```text
<optional preparation prefix><operation> @ <preparation label>
```

Examples:

```text
read.projection @ parquet
merge.upsert @ orc
```

Prefixes identify a meaningful starting-state transition. They are not inferred
from case IDs. Scenario code explicitly chooses which operation lists are
compatible with each preparation.

`CaseCatalogTest` pins the exact ordered 34-case core catalog and checks uniqueness
across the complete composed catalog. Additional capabilities can register cases
without rewriting the core contract.

## Assertions

Mutation cases follow one structure:

1. Capture the prepared state.
2. Execute one operation.
3. Read the resulting state.
4. Assert the complete expected row set.
5. Assert the expected snapshot delta and any relevant schema or metadata change.

Preparations also validate their own immediate transitions. A failed seed,
unexpected snapshot, or malformed starting state fails before the behavior under
test runs.

Rejection cases use `Check.intercept[E]`. They fail when the operation succeeds or
throws the wrong type. Cases also assert the diagnostic and unchanged state when
that state is part of the contract.

## Skip policy

`knownBugReason` records a product defect while preserving the intended assertion.
Removing the marker becomes the acceptance test for the fix.

`embeddedSkipReason` records a dependency that the local embedded server does not
provide. The li-openhouse environment still runs the assertion.

The two policies are independent. Product failures are not classified as embedded
limitations, and missing local dependencies do not weaken product assertions.

Capability-specific known bugs and embedded limitations are recorded in the
capability's section of [TEST-COVERAGE.md](TEST-COVERAGE.md).

## Add a capability

1. Add a scenario trait under `src/main/scala/harness/openhouse/scenarios/`.
2. Keep its operations and assertions in that capability. Put specialized table
   construction and prepared states in a narrowly named `*TableFixtures` trait
   owned by the same capability.
3. Mix the trait into `Scenarios`.
4. Add one named case list to `Catalog.extensionContributions`.
5. Add focused Spark-free tests when the capability changes framework behavior.
6. Run Spotless, the module tests, and the complete local catalog.
7. Add the capability's observed behavior and assertions to
   [TEST-COVERAGE.md](TEST-COVERAGE.md).
8. Add the capability and validated catalog result to
   [CAPABILITY-MATRIX.md](CAPABILITY-MATRIX.md).

Keep capability-specific helpers with the scenario that consumes them. Move a
primitive into shared fixtures only when multiple scenarios use it.

## Source map

Paths are relative to `integrations/spark/delta-harness/`.

| Path | Responsibility |
|------|----------------|
| `src/main/scala/harness/openhouse/Framework.scala` | Portable case, preparation, assertion, outcome, and lifecycle types. |
| `src/main/scala/harness/openhouse/Runner.scala` | Portable configuration, retry, parallel execution, and deterministic results. |
| `src/main/scala/harness/openhouse/Env.scala` | Embedded OpenHouse and Spark wiring. |
| `src/main/scala/harness/openhouse/LocalRunner.scala` | Local filtering, execution, and reporting. |
| `src/main/scala/harness/openhouse/scenarios/TableTestFixtures.scala` | Core table shape, layouts, standard seed, and late-bound data source. |
| `src/main/scala/harness/openhouse/scenarios/Catalog.scala` | Core contributions, extensions, complete catalog, and `Plan` facade. |
| `src/main/scala/harness/openhouse/scenarios/ScenarioCoreDml.scala` | Six representative operations in the 12-case core DML contribution. |
| `src/main/scala/harness/openhouse/scenarios/ScenarioDmlRejection.scala` | Rejected DML forms and unchanged-state assertions. |
| `src/test/scala/harness/scenarios/CaseCatalogTest.scala` | Core catalog inventory, catalog uniqueness, and data-source override checks. |
| `src/test/scala/harness/framework/RunnerTest.scala` | Retry, terminal failure, configuration, cause traversal, and result behavior. |
| `src/test/scala/harness/framework/TableLifecycleTest.scala` | Ownership and cleanup precedence. |
