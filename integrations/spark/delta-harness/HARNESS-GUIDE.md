# delta-harness: a guide to grokking the tests

This is the single document to read to understand what this harness is, how it is built, why it is built
that way, and what it found. It is written for a person picking the harness up cold. If you read only one
file, read this one; `run-openhouse.sh` and the `*.scala` sources are the ground truth beneath it.

---

## 1. What it is, in brief

`delta-harness` is a self-contained Scala test rig that drives real, customer-facing Spark SQL against a
real embedded OpenHouse catalog and asserts what actually happened to the table. It is not a unit test of
OpenHouse internals. It is a behavioral matrix over the surface a data engineer actually touches:
`DELETE`, `UPDATE`, `MERGE`, `INSERT`, and `OVERWRITE`; copy-on-write versus merge-on-read; DDL;
branching and Write-Audit-Publish; time travel; restore; maintenance procedures; streaming and CDC
readers; the drop-then-undrop lifecycle; and the behaviors specific to LinkedIn's `com.linkedin.iceberg`
1.5.2 fork.

A few facts set expectations before you read further.

- The suite runs a few thousand cases in each mode (the in-memory-stub mode and the real-HTS mode), and
  every case passes with no divergence between the ORC and Parquet encodings. The guide deliberately does
  not quote an exact case count, because that number changes every time a case is added; the exact figure
  is whatever the final line of a full run prints.
- A test is a localized `Plan.Case`. A reusable `TablePreparation` creates a fresh table in a known
  state, then the case body performs the action and assertions together.
- The suite scales by constructing localized cases from shared preparation collections for file
  format, partitioning, copy-on-write versus merge-on-read, replace lineage, branch, and restored
  tables. Every case materializes its own table from the selected preparation.
- The purpose is to find broken feature interactions, not to accumulate green cases. The findings, the
  `G`-series product-behavior notes, the `WAP1` note, the fork behaviors, and an error-message
  readability audit, are the real output, and the green count only tells you that the tripwires are
  still where they were left.

---

## 2. How to run it

The harness requires JDK 17, because the repository pins Lombok 1.18.20, which does not compile on JDK 21
or newer. Point the script at a 17 through `JAVA17_HOME`; it also accepts `JAVA_HOME` when that already
points at a 17.

```bash
export JAVA17_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # or wherever your 17 lives

./run-openhouse.sh                   # the full matrix; the last printed line is the case count
./run-openhouse.sh delete parquet    # a fast slice (~25s): delete tests, Parquet only
./run-openhouse.sh merge parquet      # merge tests on Parquet
./run-openhouse.sh delete.byPredicate  # one operation across its layouts
```

Each positional argument is an AND-substring filter on the case id, so a case runs only if its id
contains all of the arguments. The match is a substring rather than an exact token, which means
`partitioned` also matches `unpartitioned`. A narrow slice takes roughly 25 seconds end to end, because
the embedded-server and Spark startup dominate while the assertions themselves take milliseconds. You
should iterate on a slice and run the whole matrix only as a final gate.

Two environment variables change what is exercised.

| Variable | What it does |
|---|---|
| `HARNESS_REAL_HTS=1` | This boots the real House Table Service as a second in-JVM Spring context and runs the drop-then-undrop blocks (`undrop:*`, `undropAdmin.*`, and the `undropInteract` three-way compositions) against it. When the variable is unset, the harness uses an in-memory stub and skips those blocks, which is why the real-HTS run has more cases than the default run. |
| `ICEBERG_RUNTIME_JAR=<path>` | This is branch-testing mode. It swaps the shaded Iceberg runtime jar on the classpath for a locally built fork-branch-HEAD jar, so the whole suite runs against un-released fork bytecode. The swap is reversible, and it hard-fails when the jar it is asked to replace is not found, so a typo cannot silently leave the release jar in place. |

`HARNESS_PARALLELISM=N` overrides the worker count, which otherwise defaults to the CPU count; a value of
one or less runs sequentially.

### What the script does

`run-openhouse.sh` performs three steps. First, it resolves the OpenHouse classpath through a system
Gradle. The Gradle wrapper cannot download behind the proxy, as noted in the pitfalls below. The script
caches the result. Second, it compiles every `.scala` file under `src/main/scala/harness/openhouse/` with
`scalac`. Third, it runs `harness.Main` on JDK 17 with the `--add-opens` flags that Spark 3.5 needs.
Gradle is used only to produce the classpath and OpenHouse's own jars; it does not build the harness.

---

## 3. The mental model, and why a test looks the way it does

A test is a `Plan.Case` owned by one scenario trait. The case is usually created through
`TablePreparation.test`, which makes the preparation, action, and assertions readable in one
continuous block:

```scala
preparedCoreTables.flatMap { preparation =>
  List(
    preparation.test("delete.byPredicate") { table =>
      table.spark.sql(
        s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} < 2")

      assert(table.rows == table.preparedRows.filterNot(_.get(Core.long0) < 2))
    })
}
```

Four ideas define this structure.

1. **A schema is columns only.** `CoreTable` has one column per common type plus a `datepartition`
   string in the form `YYYY-MM-DD-HH`, while `NestedTable` and `TypesTable` cover nested structures
   and type-edge coverage. Each `Column[T]` carries its Scala type and a deterministic
   `literalAt(rowIndex)` generator, so seeding is reproducible and schema-checked.

2. **A preparation is an immutable recipe.** `TableTest[S]` remains the typed pipeline used to define
   reusable setup such as create and seed, ordered, evolved, RTAS, merge-on-read, branch, or undrop.
   `TablePreparation[S]` gives that recipe a case label and optional post-case assertion. A preparation
   object stores instructions, not a live table.

3. **Every case receives a fresh prepared table.** `TablePreparation.test` runs its recipe against a
   unique table name, captures the prepared rows and snapshot count in `PreparedTable[S]`, executes the
   localized case body, runs any preparation-level postcondition, and drops the table in `finally`.
   Reusing a preparation across a family therefore preserves isolation.

4. **The action and assertions stay together.** The case body issues the SQL or API call and immediately
   asserts the resulting rows, snapshots, metadata, or error. `PreparedTable.preparedRows` and
   `preparedSnapshotCount` support relative assertions, while `rows` and `snapshotCount` read the live
   state. A reviewer can follow a test from setup choice through action to expected result without
   jumping through a separate operation catalog or central assembly file.

The parallel runner, `harness.Main`, runs cases on a worker pool, and each worker gets its own
`spark.newSession()` with a separate `SQLConf`. Session state such as `spark.wap.branch`,
`spark.wap.id`, and changelog temp views is scoped to one worker session. Results are collected and
printed in catalog order. Fresh table names and per-case teardown keep table state isolated.

Known product bugs are tagged rather than skipped into silence. `Plan.knownBugs` maps a case-id substring
to a reason, and a matching case is reported as `SKIP (bug: …)`. This is how a genuine defect is deferred
without either failing the suite or silently pretending it passed.

---

## 4. Where things live

The harness is split by concern, and every file declares `package harness`, so the directory name does
not affect the package. Open the file whose concern matches what you are after.

| File | What it holds |
|---|---|
| `Framework.scala` | This file holds the DSL and plumbing: `Ctx`, the REST and `HtsAdmin` clients, `Outcome` and `Check`, the typed schema vocabulary, `TableTest` preparation pipelines, `TablePreparation`, and `PreparedTable`. Read it first to learn the lifecycle. |
| `ScenarioKit.scala` | This is the shared kit that every test group builds on. It holds `Layout`, reusable preparation collections, the format-multiplex hooks used by context-only cases, and cross-cutting helpers. Every `*Scenarios` trait extends it. |
| `DmlScenarios.scala` | This is the core DML surface. It owns localized read, delete, update, merge, insert, append, overwrite, DDL-consumer, schema-DDL, and copy-on-write versus merge-on-read discriminator cases. |
| `NestedTypesScenarios.scala` | This holds nested and complex-type coverage, type-edge coverage, and partition transforms together with partition-evolution rejections. |
| `MorMaintScenarios.scala` | This holds merge-on-read delete-file coexistence (operations on a table that already carries a live position delete), merge-on-read maintenance folds, merge-on-read modality hazards, and merge-on-read crossed with branch merge. |
| `MaintControlScenarios.scala` | This holds time travel, restore and rollback, the maintenance procedures such as `expire_snapshots` and `rewrite_data_files`, the REST control-plane operations for lock and unlock, and the undrop admin lifecycle. |
| `ForkScenarios.scala` | This holds the `com.linkedin.iceberg` fork-behavior pins; the fork commits themselves are tabulated in section 8. |
| `BranchWapScenarios.scala` | This holds branching and Write-Audit-Publish: the undrop three-way compositions, the direct-branch operations, and the branch and WAP battery, which covers staged-write publish visibility and the systematic branch-DDL leak. |
| `NegativeDdlScenarios.scala` | This holds the typed negatives and contract pins together with the DDL phases: properties, sort order, rename, namespace, policy, CTAS and RTAS, column tags and ACL, and encryption. |
| `InteractionScenarios.scala` | This holds the three-way compositions where the interesting behavior lives: DDL crossed with history, RTAS crossed with history, lineage, and property-merge, branch crossed with history and maintenance, and the composite branch-expiration-merge defect. |
| `SurfaceScenarios.scala` | This holds surface completion: the error-message readability guard, branch leaks, WAP negatives, streaming and CDC, procedures, metadata tables, concurrency invariants, schema-evolution edges, write-path configs, and expected-unsupported pins. |
| `HazardReaderWriterScenarios.scala` | This holds the hazard and modality interactions (expired checkpoints, RTAS wiping tags, rename breaking consumers) and the reader-by-writer-class battery (changelog, incremental, and streaming over both copy-on-write and merge-on-read). |
| `Plan.scala` | This is the ordered index. `object Plan` concatenates scenario-owned case lists and holds `knownBugs`. It contains no test behavior or matrix construction. |
| `OpenHouseMatrix.scala` | This mixes the domain traits into `object Scenarios`. The `extends` clause here is the authoritative order in which the traits' `val`s initialize, as explained in section 6. |
| `Env.scala` | This handles boot and run: the embedded OpenHouse server wiring in `OpenHouseEnv`, the embedded real HTS in `HtsEnv` and `HtsBootApp`, the retrying `Runner`, and `Main`. |

---

## 5. The axes, and why the honest target is well below the naive product

You can think of the suite as preparations crossed with localized behaviors and consumers.

- The scenario traits define the behaviors: DML, DDL, procedures, branch operations, streaming, and
  feature interactions.
- The preparation collections define the starting states: plain create and seed, RTAS replace lineage,
  branch routed through `spark.wap.branch`, restored from drop on the real HTS, schema evolved, sort
  ordered, and merge-on-read.
- Each family constructs one local case body per applicable preparation. The body contains the action
  and assertions for that exact combination.
- The consumers answer a question: after a state-changing DDL, does each reader, such as a plain scan,
  time travel, changelog, incremental, or streaming read, still work?

The naive product is much larger than what actually runs, because a large fraction of the cells would be
vacuous, and the harness refuses to inflate its count with them. Three arguments carry most of that
reduction. First, a read or insert on a delete-free merge-on-read table is byte-identical to
copy-on-write, because there are no delete files to apply and append is mode-independent, so the real
merge-on-read surface is mutation operations crossed with merge-on-read, plus delete-file coexistence,
plus reads with live deletes, rather than the whole operation catalog crossed with merge-on-read. Second,
RTAS and branch commute with file format, because refs and metadata never touch file encoding, so those
legs run on Parquet only rather than across all three formats. Third, a DDL-by-consumer cross over a
rejected or one-shot DDL has no post-state to consume, so only state-changing DDL crossed with real
consumers is non-vacuous.

When an estimate turns out to be inflated by vacuous cells, the honest move is to correct the estimate in
the open rather than to chase the vacuous number. File format, however, is not a vacuity axis, as the next
section explains.

### Format multiplex, and why "format-inert" is a hypothesis rather than an assumption

Most table-creating families iterate a preparation collection whose labels and `CREATE TABLE` recipes
already carry the format. Context-only families either create a fixed Parquet table when encoding is
irrelevant or construct one explicit case per selected format. Whether a behavior is format-independent
is something the harness verifies rather than assumes, because the fork carries patched ORC paths and
replace-path findings have exposed metadata differences. Only table-less operations have no format axis.

---

## 6. Design decisions and pitfalls

The catalog wiring is copied rather than extended. `OpenHouseEnv` composes an embedded
`OpenHouseLocalServer` together with Spark-catalog configuration lifted from `OpenHouseLocalServer` and
`TestSparkSessionUtil` as components, so no OpenHouse test class is subclassed and no existing test is
altered. The harness is a bolt-on observer.

The undrop leg drives a real HTS through a single backward-compatible production change. A customer `DROP`
hard-codes `purge=true`, so a customer can never populate the soft-deleted store, and the embedded
server's default `HouseTableRepository` is an in-memory stub, so an undrop test against it would test the
stub rather than production. For that reason, `HARNESS_REAL_HTS=1` boots the genuine House Table Service
as a second in-JVM Spring context and points the tables server at it. The only production-code change is
one `@ConditionalOnProperty` on `HouseTablesH2Repository`, with `havingValue="true"` and
`matchIfMissing=true`, so that the stub can be switched off. The change is fully backward compatible,
because an absent property leaves the stub in place exactly as before, and everything else is on the
harness side.

Assertions are deltas, and rejections are pins. A negative test asserts a rejection-message substring and,
following the readability audit in section 7, also asserts that the message is not a raw stacktrace, an
`[INTERNAL_ERROR]`, or a bare NullPointerException. These rejections are tripwires rather than contracts,
which means that if OpenHouse later supports the operation, the pinned test is meant to flip and be
updated rather than to keep passing silently. The goal is to catch a change in behavior in either
direction.

The trait layout determines the initialization order. `object Scenarios`, in `OpenHouseMatrix.scala`, is
assembled by mixing the domain traits on top of `ScenarioKit` through an explicit `extends … with …`
clause, and that clause is the authoritative order. `ScenarioKit` linearizes first, so its shared `val`s
initialize before any domain trait references them, and the domain traits then initialize in the order
written. A helper used by more than one trait must live in `ScenarioKit`, because a reference to a sibling
trait's member will not resolve and the compiler will tell you. As long as the `extends` clause and the
member order within each trait stay stable, initialization stays deterministic.

Several pitfalls are specific to this harness. The first is that only JDK 17 works, because Lombok 1.18.20
in the repository does not compile on 21 or newer. The second is that the Gradle wrapper cannot download
behind the proxy and returns a 403, so you must use a system Gradle through `GRADLE_BIN`; the script
caches the resolved classpath after the first run. The third is that Avro required a classpath fix,
because a duplicate shaded and unshaded Iceberg on the classpath broke Avro until a dependency exclusion
was added in `scripts/print-cp.init.gradle`. The fourth is that file format is a hypothesis and the format
policy is additive: you should not optimize a block down to Parquet only on the grounds that it should be
format-inert, because that is precisely the assumption the harness exists to check, and every
table-creating block covers at least Parquet and ORC while the three-format blocks keep Avro. Adding
coverage is additive and never removes an existing format.

---

## 7. What the harness found

The following are product-behavior findings, and each is demonstrated live by named cases.

The first group is guard gaps, where an operation that can corrupt or mislead is not blocked.

- **G2 is that RTAS on a locked table succeeds.** The lock rejects an `UPDATE`, and then `CREATE OR
  REPLACE` replaces the locked table, taking it from three rows to two, because the replace path never
  reaches the lock check. This is a data-loss-class gap with the cleanest one-line fix, and it is
  demonstrated by `interact.rtas.onLockedTable`.
- **G8 is that table-global DDL "on a branch" silently mutates main.** With `spark.wap.branch` set, `ADD
  COLUMN`, `SET TBLPROPERTIES`, and `WRITE ORDERED BY` change main's schema, properties, and sort order,
  because there is no branch dimension anywhere in the metadata commit path. It is demonstrated by
  `branch.ddlLeak.*`.
- **G9 and G10 are that the replace path dodges the update-path guards.** RTAS can change the partition
  spec and drop columns that `ALTER` rejects (G9), and RTAS silently wipes the `policies` plane, so that
  retention, sharing, and PII column tags are gone after a replace while user properties survive (G10).
  G10 is the highest-severity member of the replace-path cluster, and both are demonstrated by
  `interact.rtas.*` and `hazard.rtas.wipesColumnTags`.
- **G11 is that a routine snapshot expiration destroys merge connectivity between live refs.** Expiration
  retention is per-ref and head-anchored, so nothing protects the ancestry between live refs. The
  consequences are all demonstrated: a `fast_forward` merge is spuriously rejected with "main is not an
  ancestor" even though main never moved; a cherry-pick silently loses the expired intermediate commit,
  which is a partial merge that presents as success and is the worst variant; the branch becomes
  permanently unmergeable; and staged WAP snapshots are expired before publish. OpenHouse's default
  three-day expiration makes all of this automatic, and it is demonstrated by
  `interact.branch.expireMerge.*`.
- **G12 is that a lock starves maintenance for its whole lifetime while not stopping RTAS**, which makes
  it the mirror of G2. Scheduled expiration and compaction hit the lock gate and fail every cycle, so
  snapshots and files accrete unboundedly. It is demonstrated by `hazard.lock.starvesMaintenance`.
- **G3 through G7 are the lower-severity gaps**: replica-path spec divergence, free WAP and replace
  toggling, ref preservation, format-version on update, and the all-or-nothing `skipEligibilityCheck` on
  the replica path. G1 was investigated and then withdrawn, because the replication snapshot-walk turned
  out to be sound.

The second group is behavior and limitation findings.

- **G13 is that CDC changelog is unsupported over a merge-on-read table after an UPDATE or MERGE**, which
  fails with "Delete files are currently not supported in changelog scans". Merge-on-read delete-only and
  all copy-on-write cases work, but merge-on-read update and merge, the shapes a merge-on-read table
  exists to optimize, break CDC silently. This is a stock Iceberg 1.5 limitation, and it is demonstrated
  by `readerWriter.changelog.{update,merge}.mor`.
- **G14 is that `rewrite_data_files` leaves a dangling position delete on a merge-on-read table.**
  Compaction applies the delete, so the row set is correct, but it does not fold out the now-dangling
  delete file until `rewrite_position_delete_files` runs. This is stock Iceberg 1.5, which has no
  `remove-dangling-deletes` yet. It is classified as a pin rather than a bug, because the recovery path is
  verified to work by `maint.mor.rewritePositionDeleteFolds` across the merge-on-read formats. The
  operational takeaway is that, on merge-on-read under 1.5, you should pair `rewrite_data_files` with
  `rewrite_position_delete_files`.
- **WAP1 is that a staged DELETE (with `spark.wap.id` set) is not honored by WAP and publishes to main
  immediately.** In the same block, staged `INSERT`, `OVERWRITE`, `UPDATE`, and `MERGE` all stage
  correctly. The consequence is that an operator relying on WAP to stage and review a deletion gets an
  immediate, un-reviewed publish. It is demonstrated by `wapStaged.delete.bypassesWap`.

There is also an error-message readability finding. A separate sweep grades rejection messages as good,
acceptable, or bad for a non-expert SQL user. The systemic result is that the client drags the entire
error body, including a stacktrace, into the message, so that even a good server sentence reaches the user
as `400 , {json + java frames}`; surfacing only `ErrorResponseBody.message` would upgrade nearly every 4xx
path at once.

Finally, the tagged and deferred defects are the ones that appear in `Plan.knownBugs` and are reported as
`SKIP (bug: …)`. They are a nested-field DELETE optimizer NullPointerException, a RENAME COLUMN that is a
silent no-op (a genuine OpenHouse regression traced to server commit #558), and encryption that writes
plaintext because the KMS plugin is out of the repository.

> The exhaustive ledgers behind this section include the findings with code citations, the fork-commit
> audit, the tagged-defect ledger, and the dated run log. They live alongside the harness in the pull
> request that developed it, and not necessarily in this tree. You do not need them to grok the tests,
> so reach for them only when you want the evidence behind a specific claim made here.

---

## 8. The `com.linkedin.iceberg` fork

The harness runs against fork bytecode, namely `com.linkedin.iceberg:iceberg-spark-runtime-3.5_2.12`,
rather than against Apache Iceberg. The tested behaviors are listed below. Each one is pinned by a
`fork.*` case, and each is keyed to the fork's own commit number or the upstream-Iceberg issue number.

| Commit | The behavior the fork changes | Pinned by |
|---|---|---|
| `#249` | The partitioned default write distribution becomes NONE, where Apache uses HASH, which produces more and smaller files. | `fork.partitionDist.default` |
| `#229` | A `write.delete-file-replication` toggle is added for merge-on-read delete files. | `fork.deleteFileReplication` |
| `#219` | A per-output-file replication factor is stamped by the delete-file write path. | `fork.fileReplicationFactor` |
| `#228` | A `spark.sql.iceberg.split-size` read split-size property is added. | `fork.splitSize` |
| `#233` | Compaction bin-pack weight is computed by data-file length and ignores delete size. | `fork.binPackByLength` |
| `#189` | A budgeted rewrite is ordered by file-sequence-number. | `fork.compactionOrder` |
| `#251` | Column-default APIs and `SchemaParser` serialization are added; this exists on the branch HEAD only and is tabled. | `fork.colDefault.*` |

The `#251` story is worth understanding, because it is a good example of the harness resisting an
overclaim. `#251` backports column defaults to the API and core, but there is no read-application code and
no Spark wiring in the open fork, because `SparkTable` does not implement `SupportsColumnDefaultValue`. As
a result, over OSS Spark, `ADD COLUMN … DEFAULT 5` parses, but the default is not written into the Iceberg
schema, old rows read NULL, and an INSERT that omits the column is rejected. The serialization does round
trip on a branch build. The harness pins exactly that: the observable OSS-Spark DDL behavior and the
serialization. It explicitly does not claim the feature is broken, because read-application may exist
in LinkedIn's private Spark, which this harness cannot see. A whole-suite branch-versus-release run,
performed through `ICEBERG_RUNTIME_JAR`, showed no correctness deltas.

---

## 9. Adding a test

Adding a test follows a short recipe. First, pick the schema, which is `CoreTable` unless the behavior
needs nested or type-edge columns. Second, select or add the smallest reusable preparation that produces
the required starting state. Third, add a `preparation.test("caseName") { table => ... }` body in the
scenario trait whose concern matches. Keep the action and every assertion in that body. Use
`table.preparedRows` and `table.preparedSnapshotCount` for the starting state, and use `table.rows`,
`table.snapshotCount`, and metadata queries for the result.

Construct the family across each applicable preparation or format in the scenario trait, then add the
scenario-owned case list to `Plan.cases` in the intended catalog position. If the case characterizes a
deferred product bug, tag it in `Plan.knownBugs` with a reason. Run the catalog regression test to verify
the intended count and ordering change, run the narrow local slice, and then run the broader validation
gate.

---

## 10. Decisions worth knowing

File format is a per-case parameter rather than a baked-in constant, because un-baking the format is what
lets a test multiplex and compose; whether a behavior is format-inert is verified rather than assumed. The
dangling merge-on-read delete described in G14 is a pin rather than a bug, because
`rewrite_position_delete_files` is verified to recover it, and merge-on-read under 1.5 simply requires that
extra maintenance step. Encryption and KMS support is deferred, because the plugin is out of the
repository, so the plaintext behavior is pinned and the intended-behavior assertion waits for the plugin.
