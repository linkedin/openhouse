# Type Mapping Manifests

This directory hosts versioned **type mapping manifests** that codify how
primitive, complex, and logical types map across the engines and table formats
supported by OpenHouse (currently Hive and Iceberg as storage formats; Spark
and Trino as engines, with HiveQL and PyArrow coverage in progress).

Each manifest is a contract: a single source of truth for cross-engine type
semantics. Consumers (query planners, schema validators, type-system code
generators such as Calcite `TypeFactory` or Coral `CoralDataType`, and
authoring agents that inject design-time transformation shims) use it to keep
behavior consistent as dialects evolve.

## Layout

```
type-mapping-manifests/
├── README.md          # this file
├── v1/
│   └── README.md      # the v1 manifest — table-presentation form
├── v2/
│   └── README.md      # future versions
```

Each version's manifest lives entirely in its `README.md`. The tables in that
file are the canonical contract — no separate machine-readable artifact ships
in v1. If future consumers require structured input (YAML, JSON), a generated
form will be added alongside the markdown.

## Versioning policy

- Each `vN/` directory is an **immutable release**. Once published, entries
  are not edited in place; corrections ship as a new version.
- Versions are additive where possible. A new version may extend coverage to
  new dialects, new dialect versions, or new types, and may revise the
  mapping for an existing entry when behavioral evidence warrants it.
- Each version declares the exact dialect and format versions it covers in
  its `README.md` header; consumers should pin to a specific manifest version.

## How a version is produced

Manifest entries are grounded in empirically observed engine behavior:
contract tables are materialized in each dialect via native APIs, and for
each table the persisted schema is compared against the schema inferred by
every reading engine. Divergences become manifest entries.

The contract tables themselves are not published in this repository; only the
resulting manifest is.
