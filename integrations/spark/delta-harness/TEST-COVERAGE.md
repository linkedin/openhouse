# What the Delta harness tests

The harness describes OpenHouse behavior through observable outcomes rather than
through implementation details or a test inventory. The same behavior is reused
across compatible prepared tables and storage formats.

## Data representation

Values written through Spark must retain their meaning when read through
OpenHouse. The tests exercise ordinary scalar values, nulls, numeric limits,
special floating-point values, binary data, dates, timestamps, Unicode text, and
empty strings.

The assertion is semantic: values read back must be equivalent to the values that
were written. A storage format must not silently narrow, reinterpret, or discard
them.

## Reads and table changes

Reads must return the expected rows without changing the table. Appends,
overwrites, deletes, updates, and merges must produce the complete expected row
set and the expected snapshot change.

The tests verify both what changed and what did not change. A targeted update
must preserve every untouched row and column. An overwrite must remove the prior
contents rather than behave like an append. A merge must distinguish matched and
unmatched rows according to its clauses.

## Invalid operations

OpenHouse must reject statements that cannot be interpreted safely. The tests
cover undeclared columns, nondeterministic row filters, incomplete inserts,
conflicting merge assignments, and merge sources that match one target row more
than once.

A rejection is part of the product contract. The error must explain the invalid
request, and operations that reach execution before failing must leave the table
unchanged.

## Reuse across table states

The read and DML contracts are independent of the table states where they run.
As the suite adds partitioning, ordering, schema evolution, replacement lineage,
delete files, references, or policies, compatible behavior contracts run again
against those prepared tables.

[How Delta harness coverage composes](CAPABILITY-MATRIX.md) explains that
multiplication model and the role of the embedded and acceptance environments.

## Standard DML

The DML contract covers filtered reads; inserts from values, queries, and data
frames; full-table and partition-scoped overwrites; deletes by predicates,
subqueries, aliases, and whole-table conditions; updates by predicates,
subqueries, expressions, aliases, multiple columns, partition moves, and null
assignment; and merges with matched, unmatched, conditional, wildcard, common
table expression, and set-operation sources.

Most operations run on the seeded unpartitioned table. A null-sensitive delete
runs on the same table with an additional null value, while partition-scoped
overwrites run on a date-partitioned table. Each compatible operation runs in
Parquet and ORC with the same complete-row and snapshot assertions.
