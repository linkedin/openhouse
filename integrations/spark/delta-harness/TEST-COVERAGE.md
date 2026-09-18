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
| Harness core | [#741](https://github.com/linkedin/openhouse/pull/741) |
| Test coverage documentation | [#742](https://github.com/linkedin/openhouse/pull/742) |
| Standard DML | [#743](https://github.com/linkedin/openhouse/pull/743) |
| RTAS | [#744](https://github.com/linkedin/openhouse/pull/744) |
| DML state matrix | [#745](https://github.com/linkedin/openhouse/pull/745) |
| Schema and types | [#746](https://github.com/linkedin/openhouse/pull/746) |
| Catalog constraints | [#747](https://github.com/linkedin/openhouse/pull/747) |
| Column defaults | [#748](https://github.com/linkedin/openhouse/pull/748) |
| Compatibility and streaming | [#749](https://github.com/linkedin/openhouse/pull/749) |
| History | [#750](https://github.com/linkedin/openhouse/pull/750) |
| Governance | [#751](https://github.com/linkedin/openhouse/pull/751) |
| Maintenance and planning | [#752](https://github.com/linkedin/openhouse/pull/752) |
| Catalog DDL | [#753](https://github.com/linkedin/openhouse/pull/753) |
| Merge-on-read | [#754](https://github.com/linkedin/openhouse/pull/754) |
| Branch and write-audit-publish | [#755](https://github.com/linkedin/openhouse/pull/755) |

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
_The Standard DML PR fills this section._
<!-- coverage:standard-dml:end -->

## RTAS

<!-- coverage:rtas:start -->
_The RTAS PR fills this section._
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
This layer adds four probes for a 38-case catalog, all passing locally.

Two format-specific cases show that `ALTER TABLE ADD COLUMN ... DEFAULT` is
accepted by the SQL parser but does not persist or apply the default in the current
stack. The other probes pin Iceberg schema serialization and the low-level metadata
read contract for a column carrying an initial default.
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
_The Merge-on-read PR fills this section._
<!-- coverage:merge-on-read:end -->

## Branch and write-audit-publish

<!-- coverage:branch-wap:start -->
_The Branch and write-audit-publish PR fills this section._
<!-- coverage:branch-wap:end -->
