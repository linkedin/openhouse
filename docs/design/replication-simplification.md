# Replication Simplification Design

**Status:** Design in progress. This document captures the goal and agreed direction; it is not yet an implementation specification.

## Problem statement

Today, OpenHouse "leaks" catalog concerns into Iceberg table metadata. As a result, operations that could be pure catalog operations must update Iceberg metadata unnecessarily. Operations such as `RENAME` or replication become semantically more complicated because metadata must be updated when, for example, it is transferred to a new table location. This may also affect performance: a disk read or write is required for what should be a lightweight catalog lookup.

## Proposed solution

"The only way to win is not to play."

The solution is to establish a clear separation of concerns between the OpenHouse Catalog and the Iceberg table format.

The plan is to relocate three classes of information from Iceberg table metadata to the OpenHouse Catalog:

1. **House Tables Service (HTS) data.** This is the easiest category to move because all HTS data is also represented in the OpenHouse Catalog. The change establishes a single source of truth; the main work is on the Spark side, which should use OpenHouse APIs rather than read metadata directly.
2. **Replication configuration data.** This data is stored on the source table in a `policies` blob in Iceberg metadata and is not represented in the Catalog. Replication is a property of the Catalog, not of Iceberg or the table layout, so this is a leaked concern.
3. **Replication operational data.** This data is stored on the destination table and tracks whether a table is replicated and its replication progress. Like replication configuration, this is a leaked concern: whether an Iceberg table is a replica is a property of the Catalog, not the table format.

This design is the first layer of a three-PR stack, with one PR for each category.

In the first phase, the work is primarily to ensure that all reads and writes go through existing APIs rather than accessing metadata directly.

In the second and third phases, new backend tables must be introduced in the Catalog to represent replication configuration and operational status, respectively. The APIs for accessing this data appear to largely exist, but currently pass reads and writes through to the JSON metadata.

On the operational side, the last update time is already handled incorrectly because it is updated in place instead of through an `ALTER TABLE` operation. Is that slower? Yes. Is that correct? Also yes.

## Advantages

Separating these concerns offers several advantages:

1. Iceberg tables become fully portable, and replication can potentially be reduced to an `rsync`-like operation between storage systems. This is especially relevant to OpenHouse, whose Iceberg format is non-standard in that it uses relative path names.
2. Supporting other table formats becomes easier. Introducing a new table format would otherwise require emulating Iceberg table properties to represent key catalog data. Separating these concerns makes additional table formats easier to accommodate.
3. Table administration becomes easier. Finding all replicated tables in the Catalog currently requires scanning and parsing each metadata JSON file. Similarly, tracking replication operational status requires scanning metadata JSON.

## Disadvantages

1. **Direct metadata readers must be migrated.** APIs for this data already exist, and standard readers such as Spark can be updated to use the APIs instead of reading metadata directly. Inevitably, however, some systems may bypass the APIs and read metadata directly. This design's position is that such systems were never supported in the first place.
2. **Direct filesystem inspection becomes less informative.** It will be harder to determine a table's relationship to OpenHouse by inspecting filesystem data. As with direct readers, playbooks may rely on inspecting table metadata to identify the corresponding OpenHouse table. Those playbooks will need to be updated, hopefully offset by the improved catalog search capability.

## Alternatives considered

There are no good alternatives beyond more limited implementations of this proposal. The current model is broken, and the longer it remains in place, the harder it will be to fix. The only real alternative is to do nothing, which is not feasible.

## Phases of operation

The proposed phases are as follows.

### 1. HTS/catalog table fields

Move OpenHouse table identity and catalog fields out of Iceberg properties because they already exist in the House Tables Service/catalog database. This avoids duplicating state and the inevitable issues caused by having two sources of truth.

### 2. Add replication source definition

The source table currently defines its replication plan through `policies.replication`, persisted in the Iceberg `policies` property. Move the desired source-to-destination replication plan into typed, catalog-backed storage and expose it through the Tables Service API.

Agreed API direction:

- Keep the current legacy `destination` string form readable for existing configurations. It identifies a destination cluster and historically implies the source database and table.
- Add a structured `destinationTable` with `clusterId`, `databaseId`, and `tableId` so a destination can be explicit and may use a different database or table name.
- The two forms are mutually exclusive in a request. Supplying both is an error.
- Compatibility is one-way: new code can normalize a legacy `destination` into `destinationTable` using the legacy same-database-and-table rule. Never flatten a structured `destinationTable` back into a legacy string, since older tools could target the wrong table.
- Support multiple destinations as separate plan entries. Catalog storage should represent one source-to-destination edge per record with typed schedule fields, not a generic JSON property bag.

Adding the ability to configure a more general destination table is not strictly necessary, but is easy to do now and offers administrators a path to greater flexibility in the future. An alternative that allowed dotted paths in `destination` was considered and discarded because they could collide with existing cluster IDs.

### 3. Replication destination state and progress

Replica tables also carry OpenHouse-owned operational state, making this the least well-defined of the three phases. To do this properly, first audit the true semantics of the replication operational data to determine what is necessary, particularly because the Iceberg table itself will no longer need to be modified during replication: the root table location and other metadata will instead be represented in the Catalog.

In some ways, this is easier than with compliant Iceberg table formats because changing the on-storage location between clusters becomes a catalog operation rather than a table-layout operation.

## Open questions

- What, if any, changes need to be made to the currently supported replication implementations? OpenHouse itself does not implement a replication job; it only provides a contract. OpenHouse may instead provide a reference implementation for replication between clusters that others can adopt, perhaps as a compaction job.
