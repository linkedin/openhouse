# How Delta harness coverage multiplies

The harness generates a set of executable tests from two inputs: prepared tables
and test cases. It pairs every test case with every compatible prepared table.
Each prepared table includes its storage format.

![Coverage composition](COVERAGE-MODEL.svg)

The [Graphviz source](COVERAGE-MODEL.dot) is kept beside the rendered diagram.

## Prepared tables

A prepared table is a table in an initial state that test cases run against.
Schema and seed rows are standardized where possible so test cases compose
across table types. Other state can vary, including:

- Storage format.
- Partitioning and write ordering.
- Table properties and policies.
- Existing snapshots, lineages, delete files, or references.

A prepared table can establish the same rows and schema through create and
populate, Replace Table As Select, or drop and restore. The same compatible test
cases then reveal whether the different lineage changes the result.

## Test cases

A test case defines an action and its expected observable result. Reads assert
returned rows and values. Writes assert rows, snapshots, and metadata. Catalog
operations assert table identity and metadata. Rejected operations assert the
error and the table state that remains unchanged.

One test-case definition supplies the action and assertions for every compatible
table type.

## Compatibility and the generated set

The harness considers every prepared-table and test-case pair. Compatibility
selects the pairs that become the executable test set. Unsupported feature
combinations remain empty cells, which makes the compatibility matrix jagged.

Adding a prepared table applies every compatible existing test case to a new
initial condition. Adding a test case applies it to every compatible existing
prepared table. Adding a storage format adds prepared tables in that format, so
the new combinations reuse the existing test-case definitions.

## Run the generated set

Both environments run the same executable test set:

- Embedded OpenHouse provides local testing.
- li-openhouse runs Airflow acceptance tests against real clusters.
