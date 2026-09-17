# Delta harness test coverage

This document describes the behavior tested by the catalog on the current branch.
The documentation PR introduces the foundation description immediately after
Harness core. Each capability PR fills only its own preallocated section, so the
description grows with the executable catalog and sibling PRs edit disjoint blocks.

## Dependency graph

```text
main
`-- Harness core (34)
    `-- Test coverage documentation
        |-- Standard DML
        |   |-- RTAS
        |   |   |-- Compatibility and streaming
        |   |   |-- History
        |   |   |-- Governance
        |   |   |-- Maintenance and planning
        |   |   |-- Catalog DDL
        |   |   `-- Merge-on-read
        |   |       `-- Branch and write-audit-publish
        |   `-- DML state matrix
        |-- Schema and types
        |-- Catalog constraints
        `-- Column defaults
```

## Review links

| Layer | Pull request |
|---|---|
| Harness core | [#682](https://github.com/linkedin/openhouse/pull/682) |
| Test coverage documentation | [#707](https://github.com/linkedin/openhouse/pull/707) |
| Standard DML | [#715](https://github.com/linkedin/openhouse/pull/715) |
| RTAS | [#704](https://github.com/linkedin/openhouse/pull/704) |
| DML state matrix | [#716](https://github.com/linkedin/openhouse/pull/716) |
| Schema and types | [#717](https://github.com/linkedin/openhouse/pull/717) |
| Catalog constraints | [#718](https://github.com/linkedin/openhouse/pull/718) |
| Column defaults | [#720](https://github.com/linkedin/openhouse/pull/720) |
| Compatibility and streaming | [#719](https://github.com/linkedin/openhouse/pull/719) |
| History | [#721](https://github.com/linkedin/openhouse/pull/721) |
| Governance | [#722](https://github.com/linkedin/openhouse/pull/722) |
| Maintenance and planning | [#723](https://github.com/linkedin/openhouse/pull/723) |
| Catalog DDL | [#724](https://github.com/linkedin/openhouse/pull/724) |
| Merge-on-read | [#705](https://github.com/linkedin/openhouse/pull/705) |
| Branch and write-audit-publish | [#706](https://github.com/linkedin/openhouse/pull/706) |

## Foundation

The 34-case foundation demonstrates each catalog composition path and runs as part
of Gradle `check`.

| Contribution | Cases | Behavior tested |
|---|---:|---|
| Data types | 10 | Long, integer, double, decimal, and string round trips; all-null rows; NaN and infinity; numeric boundaries; Unicode and empty strings. |
| Core DML | 12 | Projection, insert, insert overwrite, predicate delete, predicate update, and merge upsert on Parquet and ORC. |
| Rejected DML | 12 | Undeclared-column delete, nondeterministic delete and update predicates, short inserts, invalid merge assignments, and merge cardinality violations on Parquet and ORC. |

Successful mutations assert complete rows and snapshot deltas. Rejected mutations
assert the exception type, diagnostic, and unchanged table state where the contract
requires it.

The Spark-free suite also tests deterministic catalog identity, late-bound data
sources, ownership-gated cleanup, cleanup-failure suppression, retry boundaries,
parallel result ordering, row generation, and the portable jar contents.

## Standard DML

<!-- coverage:standard-dml:start -->
This layer adds 96 cases for a 130-case catalog, all passing locally. It runs 48
additional operations on Parquet and ORC across the canonical unpartitioned,
null-containing, and date-partitioned preparations.

The catalog covers two reads, fourteen deletes, thirteen updates, sixteen merges,
six inserts or overwrites, one null-string delete, and two partition-scoped
overwrites when combined with the representative foundation operations. Each
mutation asserts complete rows and the expected snapshot change.
<!-- coverage:standard-dml:end -->

## RTAS

<!-- coverage:rtas:start -->
This layer adds 264 cases for a 394-case catalog. The local result is 390 passed
and four documented skips.

The catalog replays compatible DML after replacement, then covers enablement and
replication gates; same-shape replacement; subsequent writes; schema and partition
changes; property, retention, tag, sort-order, and creator preservation; time
travel, rollback, snapshot recovery, changelog, and incremental reads across the
replacement boundary; rename ordering; and replace-versus-append concurrency.

The four skips keep two known defects visible in both formats:
`rtas.schema.incompatibleType.notSilentlyLossy` and
`rtas.concurrency.replaceVersusAppend`.
<!-- coverage:rtas:end -->

## DML state matrix

<!-- coverage:dml-state-matrix:start -->
_The DML state matrix PR fills this section._
<!-- coverage:dml-state-matrix:end -->

## Schema and types

<!-- coverage:schema-types:start -->
_The Schema and types PR fills this section._
<!-- coverage:schema-types:end -->

## Catalog constraints

<!-- coverage:catalog-constraints:start -->
_The Catalog constraints PR fills this section._
<!-- coverage:catalog-constraints:end -->

## Column defaults

<!-- coverage:column-defaults:start -->
_The Column defaults PR fills this section._
<!-- coverage:column-defaults:end -->

## Compatibility and streaming

<!-- coverage:compatibility-streaming:start -->
_The Compatibility and streaming PR fills this section._
<!-- coverage:compatibility-streaming:end -->

## History

<!-- coverage:history:start -->
_The History PR fills this section._
<!-- coverage:history:end -->

## Governance

<!-- coverage:governance:start -->
_The Governance PR fills this section._
<!-- coverage:governance:end -->

## Maintenance and planning

<!-- coverage:maintenance-planning:start -->
_The Maintenance and planning PR fills this section._
<!-- coverage:maintenance-planning:end -->

## Catalog DDL

<!-- coverage:catalog-ddl:start -->
_The Catalog DDL PR fills this section._
<!-- coverage:catalog-ddl:end -->

## Merge-on-read

<!-- coverage:merge-on-read:start -->
This layer adds 320 cases for a 714-case catalog. The local result is 710 passed
and the four inherited RTAS skips.

The catalog reuses 268 DML cases across merge-on-read, partitioned
merge-on-read, and replace-lineage preparations. The remaining 52 cases cover
position-delete creation, write-mode changes, operations over live delete files,
metadata and changelog behavior, format materialization, replication, time travel,
rollback, and maintenance procedures that retain or compact delete files.
<!-- coverage:merge-on-read:end -->

## Branch and write-audit-publish

<!-- coverage:branch-wap:start -->
_The Branch and write-audit-publish PR fills this section._
<!-- coverage:branch-wap:end -->
