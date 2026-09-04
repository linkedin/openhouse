# Delta harness capability matrix

The harness is delivered as a stack of independently reviewable capability layers.
Each branch contains its parent catalog plus one additive bundle. Sibling totals are
independent, so they must not be summed together.

## Current graph

```text
main
`-- Core foundation (34)
    |-- Standard DML (+96, total 130)
    |   |-- RTAS (+264, total 394)
    |   |   |-- Compatibility and streaming (+114, total 508)
    |   |   |-- History (+36, total 430)
    |   |   |-- Governance (+21, total 415)
    |   |   |-- Maintenance and planning (+23, total 417)
    |   |   |-- Catalog DDL (+38, total 432)
    |   |   `-- Merge-on-read (+320, total 714)
    |   |       `-- Branch and write-audit-publish (+102, total 816)
    |   `-- DML state matrix (+428, total 558)
    |-- Schema and types (+60, total 94)
    |-- Catalog constraints (+24, total 58)
    `-- Column defaults (+4, total 38)
```

## Validated layers

Every result below comes from the complete embedded catalog on that branch.

| Layer | Parent | Added cases | Catalog | Result | Branch |
|-------|--------|------------:|--------:|--------|--------|
| Core foundation | `main` | 34 | 34 | 34 passed | [`delta-harness-oss`][core] |
| Standard DML | Core | 96 | 130 | 130 passed | [`delta-harness-standard-dml`][standard-dml] |
| RTAS | Standard DML | 264 | 394 | 390 passed, 4 skipped | [`delta-harness-rtas`][rtas] |
| DML state matrix | Standard DML | 428 | 558 | 554 passed, 4 skipped | [`delta-harness-standard-dml-state-matrix`][matrix] |
| Schema and types | Core | 60 | 94 | 88 passed, 6 skipped | [`delta-harness-schema-types`][schema] |
| Catalog constraints | Core | 24 | 58 | 58 passed | [`delta-harness-catalog-constraints`][constraints] |
| Compatibility and streaming | RTAS | 114 | 508 | 504 passed, 4 skipped | [`delta-harness-compatibility-streaming`][compatibility] |
| Merge-on-read | RTAS | 320 | 714 | 710 passed, 4 skipped | [`delta-harness-mor`][mor] |
| Branch and write-audit-publish | Merge-on-read | 102 | 816 | 812 passed, 4 skipped | [`delta-harness-branch`][branch] |
| Column defaults | Core | 4 | 38 | 38 passed | [`delta-harness-column-defaults`][defaults] |
| History | RTAS | 36 | 430 | 426 passed, 4 skipped | [`delta-harness-history`][history] |
| Governance | RTAS | 21 | 415 | 409 passed, 6 skipped | [`delta-harness-governance`][governance] |
| Maintenance and planning | RTAS | 23 | 417 | 413 passed, 4 skipped | [`delta-harness-maintenance-planning`][maintenance] |
| Catalog DDL | RTAS | 38 | 432 | 428 passed, 4 skipped | [`delta-harness-catalog-ddl`][catalog-ddl] |

## Foundation

The root catalog is a fixed 34-case sample of three composition paths:

| Contribution | Cases | Families |
|--------------|------:|----------|
| `dataTypeCases` | 10 | Five scalar behavior families across Parquet and ORC. |
| `dmlCases` | 12 | Six representative DML operations across Parquet and ORC. |
| `dmlValidationCases` | 12 | Six rejected DML forms across Parquet and ORC. |

The representative DML operations are:

```text
read.projection
insert.into
insert.overwrite
delete.byPredicate
update.byPredicate
merge.upsert
```

The foundation has no skipped cases.

## Standard DML

`ScenarioStandardDml` contributes the remaining reusable DML surface without
duplicating the six operations owned by the foundation.

| Family | Added operations | Cases |
|--------|-----------------:|------:|
| Read | 1 | 2 |
| Delete | 13 | 26 |
| Update | 12 | 24 |
| Merge | 15 | 30 |
| Insert and overwrite | 4 | 8 |
| Null-string delete | 1 | 2 |
| Partition-scoped writes | 2 | 4 |
| Total | 48 | 96 |

The Standard DML catalog contains 34 foundation cases plus 96 extension cases.

## RTAS

RTAS contributes 264 cases:

- 212 cases replay compatible DML operations on replaced-table preparations.
- 52 cases cover 26 replacement contract families across Parquet and ORC.

The contract covers enablement, schema and partition changes, property behavior,
retention and tag preservation, time travel, snapshot recovery, changelog and
incremental-read boundaries, rename ordering, sort order, identity preservation,
and concurrent replacement.

RTAS introduces the four known-bug skips inherited by every RTAS descendant:

| Cases | Current product behavior |
|-------|--------------------------|
| `rtas.schema.incompatibleType.notSilentlyLossy` for Parquet and ORC | A bigint-to-int replacement succeeds and wraps an out-of-range value. |
| `rtas.concurrency.replaceVersusAppend` for Parquet and ORC | Replace and append can both report success while the append wins and loses the replacement. |

## DML state matrix

This sibling replays Standard DML on additional starting states. It contributes 428
cases:

- Write-ordered preparations.
- Added-column preparations.
- Compatible null-string preparations.
- Compatible partition-scoped preparations.

Four ordered delete-by-partition cases are marked as known product bugs.

## Schema and types

This sibling contributes 60 cases for nested values and schema behavior. The scalar
data type sample remains in the foundation.

Its six skips record two product limitations:

- Nested-field DELETE crashes for Parquet and ORC.
- `RENAME COLUMN` reports success but behaves as a no-op on four core layouts.

## Catalog constraints

This sibling contributes 24 cases for file-format materialization, partition
evolution, and table property constraints. DML validation remains in the
foundation.

## Compatibility and streaming

This RTAS child contributes 114 cases:

- 96 table-evolution compatibility cases.
- 10 structured streaming cases.
- 4 concurrency cases.
- 2 REST locking cases.
- 2 writer compatibility cases.

It inherits only the four RTAS known-bug skips.

## History

This RTAS sibling contributes 36 cases:

| Contribution | Cases |
|--------------|------:|
| Changelog | 14 |
| Incremental read | 10 |
| Snapshot restore | 6 |
| Time travel | 6 |

## Governance

This RTAS sibling contributes 21 cases:

| Contribution | Cases |
|--------------|------:|
| Access control and policy | 16 |
| Column tags | 2 |
| Encryption observability | 2 |
| File replication property | 1 |

The complete catalog has six skips. Four are inherited RTAS known bugs. Two are
embedded-only skips for `accessControl.grantAndRevoke`, because the local server has
no OPA endpoint. The li-openhouse acceptance environment runs those assertions
against its configured authorization service.

## Maintenance and planning

This RTAS sibling contributes 23 cases:

| Contribution | Cases |
|--------------|------:|
| Maintenance procedures | 12 |
| Metadata tables | 6 |
| Compaction planning | 3 |
| Scan planning | 2 |

The cases cover snapshot expiration, data and manifest rewrites, orphan removal,
metadata projections, file sequence behavior, and scan split planning.

## Catalog DDL

This RTAS sibling contributes 38 cases:

| Contribution | Cases |
|--------------|------:|
| Partition transforms | 20 |
| Write distribution | 6 |
| Namespace rejection | 4 |
| Rename | 4 |
| Sort order | 4 |

The layer owns a small `CatalogDdlSupport` helper for direct Iceberg metadata reads.
That helper stays in the layer instead of creating a dependency on a schema or
catalog-constraint sibling.

## Merge-on-read

This RTAS child contributes 320 cases:

- 268 DML cases across merge-on-read preparations.
- 52 focused physical and maintenance contracts.

The contracts assert position-delete files, mode changes, behavior with live
deletes, metadata tables, time travel, rollback, changelog limitations, compaction,
snapshot expiration, manifest rewriting, and orphan removal.

## Branch and write-audit-publish

This merge-on-read child contributes 102 cases:

| Contribution | Cases |
|--------------|------:|
| Branch and tag behavior | 68 |
| Write-audit-publish behavior | 34 |

Branch coverage includes reference lifecycle, branch-routed writes, fast-forward,
cherry-pick, divergence, time travel, maintenance, format, and merge-on-read
intersections.

Write-audit-publish coverage includes enablement, session routing, staged writes,
independent staging IDs, publish operations, and rejected republish or expired
staging attempts.

## Column defaults

This Core sibling contributes four probes:

- Parser acceptance and read behavior for an added defaulted column in Parquet.
- The same behavior in ORC.
- API serialization of an Iceberg field default.
- A low-level read-application probe over files written before the column existed.

## Acceptance execution

Local validation proves the scenario code against the embedded catalog. The
li-openhouse adapter must also prove:

- The published jar contains the portable framework, `Runner`, and catalog.
- Embedded-only classes are absent.
- Airflow shards enumerate the same case IDs.
- Remote runtime settings select the deployed OpenHouse catalog and data source.
- Embedded skips run when the acceptance environment provides the missing service.

[core]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-oss
[standard-dml]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-standard-dml
[rtas]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-rtas
[matrix]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-standard-dml-state-matrix
[schema]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-schema-types
[constraints]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-catalog-constraints
[compatibility]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-compatibility-streaming
[mor]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-mor
[branch]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-branch
[defaults]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-column-defaults
[history]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-history
[governance]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-governance
[maintenance]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-maintenance-planning
[catalog-ddl]: https://github.com/mkuchenbecker/openhouse/tree/mkuchenbecker/delta-harness-catalog-ddl
