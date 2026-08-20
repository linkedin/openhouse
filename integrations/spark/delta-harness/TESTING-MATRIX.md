# Delta-harness testing matrix

This document describes how the harness is organized. Every case in the suite is one point in a
cross product of independent axes, so the suite is best understood as a matrix rather than a flat
list of tests. `Plan.cases` (in `Plan.scala`) assembles the matrix by crossing an operation list with
a layout or format axis for each family, and `Scenarios` (in `OpenHouseMatrix.scala`) supplies the
operations by mixing in the per-domain traits.

## How to read a case id

A case id has the shape `<operation> @ <context>`, and some families add a preparation prefix.

| Part | Meaning |
|------|---------|
| `<operation>` | The behavior under test, for example `delete.byPredicate`, `ddl.addColumn.single`, or `merge.upsert`. |
| `@ <context>` | The table shape or environment the operation ran against, for example `partitioned/orc`, `mor-unpartitioned/avro`, `@ parquet`, or `@ embedded`. |
| `prep.rtas:`, `prep.ordered:`, `prep.evolved:`, `branchWap:`, `undrop:` | A prefix that names the preparation lineage the base table was taken through before the operation ran. |

For example, `prep.ordered:update.byPredicate @ partitioned/parquet` is the `update.byPredicate`
operation, run on a partitioned Parquet table that was created with a `WRITE ORDERED BY` clause.

## The axes

The matrix is the product of the following axes. Not every family uses every axis, because some axes
are vacuous for some operations. A branch reference, for instance, never touches file encoding, so
branch-routed families do not multiply across all three file formats.

| Axis | Values | Notes |
|------|--------|-------|
| Operation | The families listed below | The behavior being asserted. |
| Data file format | `parquet`, `orc`, `avro` | Applied through `write.format.default` and, for table-creating operations, through a per-case seed format. Format independence is treated as a hypothesis the harness verifies, not an assumption. |
| Partitioning | `unpartitioned`, `partitioned` | Partitioned tables partition by the `datepartition` string column. |
| Write mode | copy-on-write, merge-on-read | Merge-on-read tables set `format-version=2` and the merge-on-read delete, update, and merge modes, so mutations write position-delete files instead of rewriting data files. |
| Schema | `CoreTable`, `NestedTypesTable`, `TypesTable` | The column set the operation reads and writes. |
| Preparation lineage | base, ordered, evolved, replace (RTAS), branch, merge-on-read-deleted, undropped | How the base table was created and seeded before the operation ran. |
| Reference routing | main, WAP branch | Whether the operation was applied to the table directly or routed onto a write-audit-publish branch. |

## Data file formats

| Format | Explanation |
|--------|-------------|
| `parquet` | The default columnar format and the seed format when no other is set. |
| `orc` | Exercised because the fork carries patched ORC paths, so ORC coverage is not assumed to match Parquet. |
| `avro` | Exercised for the row-oriented write path on the create and merge-on-read families. |

## Schemas and data types

The harness pins one representative table per type concern. Column value generators are pure
functions of the row index, so a seed of N rows is reproducible.

### CoreTable

`CoreTable` carries one column per common primitive type plus a string date-partition column, and it
is the schema for the DML, DDL, maintenance, branching, and negative families.

| Column | SQL type |
|--------|----------|
| `foo_col_long` | `bigint` |
| `foo_col_int` | `int` |
| `foo_col_string` | `string` |
| `foo_col_double` | `double` |
| `foo_col_boolean` | `boolean` |
| `datepartition` | `string` |

### NestedTypesTable

`NestedTypesTable` covers complex and nested types, and it is the schema for the nested family.

| Column | SQL type |
|--------|----------|
| `id` | `bigint` |
| `s` | `struct<x:int,y:string>` |
| `arr` | `array<int>` |
| `m` | `map<string,int>` |
| `nested` | `struct<inner:struct<z:int>>` |

### TypesTable

`TypesTable` covers type-edge cases such as decimal and binary, and it is the schema for the type
family.

| Column | SQL type |
|--------|----------|
| `id` | `bigint` |
| `n` | `int` |
| `x` | `double` |
| `dec` | `decimal(10,2)` |
| `str` | `string` |
| `bin` | `binary` |

## Table layouts

A layout is a labeled `CREATE TABLE` recipe. The label encodes the partitioning and format so it
reads directly in the case id.

| Layout family | Labels | Explanation |
|---------------|--------|-------------|
| `layouts` | `{unpartitioned,partitioned}/{parquet,orc,avro}` | The six copy-on-write CoreTable shapes that back the DML, DDL, and negative families. |
| `morLayouts` | `mor-{unpartitioned,partitioned}/{parquet,orc,avro}` | The six merge-on-read CoreTable shapes that back the mutation families. |
| `morVerifyLayouts`, `cowVerifyLayouts` | `mor-verify/{format}`, `cow-verify/{format}` | Single-data-file shapes with `write.distribution-mode=none` so a subset delete is a partial-file match, which makes the physical outcome deterministic for the merge-on-read versus copy-on-write discriminator. |
| `nestedLayouts` | `nested-unpartitioned/{format}` | Unpartitioned shapes on `NestedTypesTable`. |
| `typesLayouts` | `types-unpartitioned/{format}` | Unpartitioned shapes on `TypesTable`. |

## Preparation lineages

Preparation determines what the base table has already been through when the operation runs. The
same operation list is reused across lineages so a behavior can be checked on each base.

| Lineage | Explanation |
|---------|-------------|
| base (`createAndSeed`) | Create under the layout and seed a fixed number of deterministic rows. |
| ordered (`createAndSeedOrdered`) | The base plus `ALTER TABLE ... WRITE ORDERED BY`, so the operation runs on a table with a declared sort order. |
| evolved (`createAndSeedEvolved`) | The base plus an added column, so the operation runs against a schema-evolved table. |
| replace (`createAndSeedRtas`, `createAndSeedRtasMor`) | The base is rebuilt through `CREATE OR REPLACE TABLE ... AS SELECT`, so the operation runs on a replace-lineage table. |
| branch (`createAndSeedOnBranch`) | The seed and the operation are routed onto a write-audit-publish branch, and the case also asserts that main is untouched. |
| merge-on-read-deleted (`createAndSeedMorDeleted`) | The base carries a live position delete, so read and maintenance operations must apply the delete at read time. |
| undropped (`createAndSeedUndropped`) | The base is taken through a real House Table Service soft-delete and restore. These cases run only when the embedded real House Table Service is enabled. |
| single-file (`createAndSeedSingleFile`) | The seed lands all rows in one data file, which is required by the copy-on-write versus merge-on-read physical discriminator. |

## Operation families

Each family is an operation list that `Plan.cases` crosses with a layout or format axis. The tables
below name the family and describe what it exercises. Representative operation names are included so
the family is recognizable in the case ids.

### DML

| Family | Explanation |
|--------|-------------|
| Reads (`read.projection`, `read.filter`, `format.materialization`) | Read-path and scan behavior, including projection, predicate pushdown, and materialization. |
| Deletes (`delete.byPredicate`, `delete.byInList`, `delete.byInSubquery`, `delete.byPartitionPredicate`, `delete.all`, `delete.truncate`, and more) | Row-level deletes across the full range of predicate shapes, including in-list, correlated and scalar subqueries, null conditions, partition predicates, and whole-table truncation. |
| Updates (`update.byPredicate`, `update.multipleColumns`, `update.byExpression`, `update.movePartition`, and more) | Row-level updates across predicate shapes, multi-column assignments, expression assignments, and partition-moving updates. |
| Merges (`merge.upsert`, `merge.insertNotMatched`, `merge.deleteMatched`, `merge.multipleMatchedClauses`, `merge.resolveByName`, and more) | `MERGE INTO` across matched and not-matched clauses, conditional clauses, upserts, source common table expressions, set operations, and by-name resolution. |
| Inserts and overwrites (`insert.into`, `insert.explicitColumns`, `append.dataFrame`, `insert.overwrite`, `insert.dynamicOverwrite`, `overwrite.dataFrame`) | The append and overwrite write paths through both SQL and the DataFrame API, including dynamic partition overwrite. |

The mutation subset (`delete.*`, `update.*`, `merge.*`) is reused on the merge-on-read, replace, and
branch lineages, because those lineages are about the mutation write path.

### DDL

DDL is split into sub-families so each area of the OpenHouse table surface is exercised on its own.
Every sub-family crosses its operation list with the six copy-on-write layouts unless noted.

| Sub-family | Operations | Explanation |
|------------|-----------|-------------|
| Schema evolution (`ddlSchemaOperations`) | `ddl.addColumn.single`, `ddl.addColumn.multiple`, `ddl.addColumn.comment`, `ddl.addColumn.position`, `ddl.alterColumn.typeWiden`, `ddl.renameColumn` | Column additions in each position and with comments, safe type widening, and column rename. These exercise how the server validates and applies a schema change. |
| Table properties (`ddlPropsOperations`) | `ddl.props.userRoundTrip`, `ddl.props.reservedOpenhouse`, `ddl.props.formatVersionForced`, `ddl.props.previousVersionsHonored` | User property round-tripping, the handling of reserved OpenHouse properties, forced format version, and honoring previously set versions. |
| Miscellaneous (`ddlMiscOperations`) | `ddl.sortOrder.orderedBy`, `ddl.sortOrder.orderedByMulti`, `ddl.renameTable`, `ddl.renameTable.conflict`, `ddl.ns.createRejected`, `ddl.ns.dropRejected` | Setting a sort order, renaming a table and the name-conflict case, and the namespace create and drop rejections. |
| Policy (`ddlPolicyOperations`) | `ddl.policy.sharing`, `ddl.policy.history`, `ddl.policy.replication`, `ddl.policy.retention`, `ddl.policy.neg.historyMaxAge`, `ddl.policy.neg.historyVersions` | `SET POLICY` for sharing, history, replication, and retention, plus the negative cases where a policy bound is out of range. |
| CTAS and RTAS (`ddlCtasRtasOperations`) | `ddl.ctas`, `ddl.rtas.enabled`, `ddl.rtas.disabled`, `ddl.rtas.replicationConflict` | Create-table-as-select, replace-table-as-select with replace enabled and disabled, and the replace-under-replication conflict. |
| Tagging, ACL, and features (`ddlTagAclFeatureOperations`) | `ddl.colTag`, `ddl.acl.grantUnshared`, `ddl.acl.grantShared`, `ddl.featureFlag.distributionMode`, `ddl.repl.tableTypeImmutable`, `ddl.encryption.active` | Column tagging, ACL grants on shared and unshared tables, the distribution-mode feature flag, replica-table-type immutability, and the encryption-active property. |
| Encryption (`ddlEncryptionOperations`) | `ddl.encryption` | The encryption capability, pinned on Parquet. |

The schema-evolution operations are also crossed with every layout as a separate `ddlSchema` block,
and there is a DDL-then-consumer battery (`ddlConsumeBattery`) that applies each state-changing DDL
and then runs each consumer to confirm the table still reads and writes.

### Maintenance

| Operations | Explanation |
|-----------|-------------|
| `maintenance.expireSnapshots`, `maintenance.rewriteDataFiles`, `maintenance.removeOrphanFiles` | The table-maintenance procedures, including the locked variants, crossed with both file formats. |

### Merge-on-read verification

| Family | Explanation |
|--------|-------------|
| `mor.writesDeleteFiles`, `cow.writesNoDeleteFiles` | The physical discriminator that proves merge-on-read wrote a position delete and copy-on-write did not. |
| merge-on-read read (`prep.morRead`), coexistence (`morCoexist`), maintenance fold and meta, hazards, and branch merge | Reads and maintenance over a table that already carries a live position delete, and the survival of position deletes across time travel, rollback, expiration, and branch merges. |

### Branching and write-audit-publish

| Family | Explanation |
|--------|-------------|
| `branching` | Branch creation and the basic branch operations. |
| `branchWap:` blocks | The DML catalog routed onto a branch, asserting both the branch delta and that main stays isolated. |
| `branchDdl`, `wapStaged` | The DDL-on-branch axis and the staged-then-publish write-audit-publish flow. |

### Interactions, surface, and hazards

| Family | Explanation |
|--------|-------------|
| `interactions` | Cross-feature cases where one feature is exercised in the presence of another. |
| `surface` | The read and write surface, including streaming read and write and the plaintext data pin. |
| `hazards`, `readerWriter` | Reader and writer hazard scenarios, such as a streaming checkpoint crossed with snapshot expiration, change-data-capture over an expired range, and replace-table-as-select wiping column tags. |

### Nested and type-edge coverage

| Family | Explanation |
|--------|-------------|
| nested (`nestedOperations` on `NestedTypesTable`) | Operations over struct, array, map, and doubly-nested struct columns. |
| types (`typesOperations` on `TypesTable`) | Operations over type-edge columns such as decimal and binary. |

### Time travel, restore, and rollback

| Family | Explanation |
|--------|-------------|
| `timeTravel` | Reads at an earlier snapshot, crossed with both file formats. |
| `restoreRollback` | `RESTORE` and rollback to an earlier snapshot, crossed with both file formats. |

### Fork behavior pins

These families pin behaviors specific to the `com.linkedin.iceberg` fork the harness runs against.
They characterize the fork surface at the API and table-property level.

| Family | Explanation |
|--------|-------------|
| `forkColDefault` | Column-default serialization through `SchemaParser`. |
| `forkPartitionDist` | Partition distribution behavior. |
| `forkDeleteFileReplication`, `forkFileReplicationFactor` | Delete-file replication and the output-file replication factor. |
| `forkSplitSize`, `forkBinPackByLength`, `forkCompactionOrder` | Split size, bin-pack by length, and compaction ordering. |

### Negatives

| Operations | Explanation |
|-----------|-------------|
| `negative.nonExistentColumn`, `negative.nonDeterministicDelete`, `negative.nonDeterministicUpdate`, `negative.insertArity`, `negative.mergeConflictingUpdates`, `negative.mergeCardinalityViolation`, `negative.partitionByNonExistent` | Cases that must be rejected. Each asserts that the operation fails, so a silent acceptance is itself a failure. |

### Control plane

| Family | Explanation |
|--------|-------------|
| `control`, `undropAdmin`, `undropInteract` | Control-plane cases such as lock and unlock, and the soft-delete, list, restore, and purge lifecycle. The undrop lifecycle cases run only when the embedded real House Table Service is enabled, and are otherwise empty. |

## How assertions are framed

Every case asserts a delta against the pre-state it observed, meaning the change in rows or in the
commit count, rather than an absolute row set. Framing assertions as deltas is what lets one
operation hold under any layout, format, and lineage, which is what makes the cross product
meaningful.

## Known bugs

A genuine product or upstream bug is tagged in `Plan.knownBugs` by a substring of the case id, along
with a prose explanation. A tagged case is reported as skipped with its reason rather than failing
the suite, which keeps the suite green while keeping the defect visible and documented.
