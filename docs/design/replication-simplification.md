# Replication Simplification Design

**Status:** Design in progress; this is not yet an implementation specification.

## Goal and motivation

Mixing catalog concerns into table-format metadata complicates catalog operations. It creates two sources of truth that must remain synchronized, and it makes operations such as replication harder: physical data cannot be moved without also rewriting OpenHouse metadata embedded in Iceberg, even when the move does not change the table's physical layout or Iceberg-native state.

This design separates OpenHouse's logical catalog state from Iceberg's representation of a table's physical layout. Iceberg metadata should describe Iceberg table state, not duplicate OpenHouse-owned catalog state. The desired invariant is that an OpenHouse-only rename, move to another cluster, or drop and recreate under a different logical name does not require OpenHouse-specific edits to Iceberg metadata. Genuine Iceberg schema and snapshot changes continue to evolve Iceberg metadata normally.

## Scope: three relocations

### 1. HTS/catalog table fields — completed background

OpenHouse table identity and catalog fields already represented in the House Tables Service (HTS) or catalog database were removed from Iceberg properties in an earlier feature branch. That work eliminated duplicate state and is background for this design, not implementation scope for this PR.

### 2. Replication source definition — planned

The source currently defines replication through `policies.replication`, serialized in Iceberg's `policies` property. Move the desired source-to-destination replication plan into typed, catalog-backed storage and expose it through the Tables Service API.

The model must support multiple destinations. Represent each source-to-destination edge as one record, with complete typed table identities and a schedule; do not use a generic JSON property bag.

The existing `destination` value is a cluster string and historically implies the source database and table. Add a structured `destinationTable` containing `clusterId`, `databaseId`, and `tableId`. The two forms are mutually exclusive; supplying both is an error.

Compatibility is intentionally one-way. New code may normalize a legacy `destination` string into an explicit `destinationTable` using the legacy same-database-and-table behavior. Do not flatten a structured `destinationTable` back into the legacy string, because older tools may then target the wrong table. Confirm actual replication worker behavior before finalizing this contract.

### 3. Replication destination state and progress — planned separately

Desired source configuration and observed destination state or progress are separate concepts and should remain separate in the model. Audit the following fields and determine their semantics and ownership:

| Field or concept | Initial hypothesis to verify |
| --- | --- |
| Replica role (`REPLICA_TABLE`) | Destination identity or role state |
| `openhouse.isTableReplicated` | Potentially transient operation context |
| `last-updated-ms` | Candidate source watermark or progress |
| Replica UUID | Already represented by catalog identity |
| `openhouse.replicaTableLocationId` | Path override |

Determine whether each value belongs in catalog/API state, should remain as transient context, or is obsolete and can be removed. Preserve identity and path behavior across rename, recreation, and cluster move.

## Migration and compatibility principles

- The catalog becomes canonical for OpenHouse state. After migration, Iceberg properties must not remain a second writable source of truth.
- Inventory all readers and writers. Classify direct Iceberg metadata consumers separately from consumers already using catalog APIs; catalog API consumers may need no migration.
- Where no catalog API exists, determine whether the consumer and use case are still necessary before adding an API.
- A versioned table `PUT` is a candidate lazy-migration point: import legacy state only when no canonical catalog state exists, apply the requested change, and remove only migrated OpenHouse state from newly written Iceberg metadata. `GET` remains side-effect-free. Define precedence so canonical catalog state always wins after migration.
- Preserve unrelated policies (including retention, history, sharing, tags, and lock state), user-defined properties, and Iceberg-native metadata.
- Do not assume an Iceberg metadata commit and catalog update are atomic. Design idempotency, concurrency handling, and recovery before implementation.
- Assess whether untouched tables need a backfill and how to handle external or direct metadata consumers.

## Workstreams and sequence

### A. Inventory

Inventory OpenHouse properties and top-level metadata on source and destination tables. Trace direct readers and writers, catalog API consumers, and external scripts. For each value, record its semantics, canonical owner, compatibility requirements, and whether it represents configuration, identity, progress, or transient context.

### B. Source replication plan

1. Verify the replication worker's target semantics.
2. Finalize the API contract and one-way legacy normalization behavior.
3. Design a typed catalog relation keyed by complete source and destination identities.
4. Update the API, scheduler, and worker.
5. Migrate the legacy policy and remove only the replication policy from Iceberg metadata.

### C. Destination state

1. Define the semantics of replica role, provenance, and watermark.
2. Determine which fields belong on the catalog table, in separate replication state, or in transient context to eliminate.
3. Preserve identity and path behavior across rename, recreation, and cluster move.
4. Migrate direct consumers.

### D. Metadata-invariance verification

Test that an OpenHouse-only rename, move, or drop and recreate does not change Iceberg metadata to encode OpenHouse state. Also verify that genuine Iceberg changes continue to evolve Iceberg metadata normally.

## Out of scope unless justified

- SQL system tables. They may be a future surface, but are not a prerequisite if the Tables Service API suffices.
- Moving all OpenHouse fields in one change. Classify each field and migrate deliberately.
- Replacing Iceberg-native metadata or user properties.

## Open questions

- What is the exact destination behavior in the production replication worker, and is that worker in this repository?
- Which destination values represent durable progress versus one-time request context?
- Is the path override required after identity and path decoupling?
- What rollout and migration strategy is needed for direct Iceberg-metadata consumers, including external systems?
- Is lazy `PUT` migration sufficient, or is bulk backfill required?
