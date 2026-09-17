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
_The Merge-on-read PR fills this section._
<!-- coverage:merge-on-read:end -->

## Branch and write-audit-publish

<!-- coverage:branch-wap:start -->
_The Branch and write-audit-publish PR fills this section._
<!-- coverage:branch-wap:end -->
