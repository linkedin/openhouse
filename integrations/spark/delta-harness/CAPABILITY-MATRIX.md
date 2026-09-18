# How Delta harness coverage composes

The harness separates **what OpenHouse should do** from **the table state where
that behavior should hold**. A behavior contract is written once, then runs
against every compatible prepared table and storage format.

![Coverage composition](COVERAGE-MODEL.svg)

The [Graphviz source](COVERAGE-MODEL.dot) is kept beside the rendered diagram.

## Behavior contracts

A behavior contract describes an action and its observable result:

- A read describes the rows and values returned without changing table state.
- A write describes the complete rows, snapshots, and metadata that must exist
  afterward.
- A catalog operation describes the table identity, schema, properties, history,
  or references that must be preserved or changed.
- A rejected operation describes the error and the state that must remain
  unchanged.

The contract does not know how the table reached its starting state.

## Prepared tables

A prepared table describes the starting state supplied to a behavior contract:

- Schema and seed rows.
- Partitioning and write ordering.
- Table properties and policies.
- Existing snapshots, lineages, delete files, or references.
- The storage format used to materialize the table.

Prepared tables do not redefine the behavior. They make the same behavior face a
different table state.

## Compatibility

Not every behavior applies to every table state. The harness records compatibility
at the point where a behavior and prepared table are combined.

Adding a behavior expands coverage across every existing compatible table. Adding
a prepared table expands coverage across every existing behavior that should hold
for that state. Adding a storage format repeats each compatible combination in the
new format.

This is the multiplication model: behavior contracts and prepared tables grow
independently, while compatibility determines which combinations become
executable tests.

## Execution environments

The generated test carries the same action and assertions into both environments:

- Embedded OpenHouse provides hermetic local feedback.
- The li-openhouse acceptance environment supplies the deployed catalog
  connection and runtime dependencies.

Environment adapters change how the test reaches OpenHouse. They do not change
what the test means.
