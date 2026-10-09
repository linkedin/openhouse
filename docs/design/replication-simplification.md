# Replication Simplification Design

**Status:** Design and implementation in progress. This document records the goal, agreed direction, current behavior, and outstanding authorization contract.

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

### Replicated table DDL coordination

The current compatibility implementation coordinates replicated-table `RENAME` and `DROP` in
Spark. It can be disabled with `spark.openhouse.replication.ddl.cascade=false`; when disabled,
Spark sends only the source operation to the Tables Service.

The planned service-owned mode moves destination coordination to the source Tables Service. The
service will read the source table's replication destinations, apply the DDL to each destination
through that destination's Tables API, and commit the local operation last. A retry must recognize
already-applied destination operations. If the addressed replica is already absent at a destination
when applying a rename or drop, treat that destination as a successful no-op rather than a failure;
this is an expected idempotent outcome when retrying an operation. Do not treat unrelated errors,
such as a target-name conflict, as absence. If a destination succeeds and a later destination or the
source commit fails, the error must identify which destinations may already be ahead so the same
operation can be retried safely.

Peer Tables API endpoints use the existing cluster YAML loaded from
`OPENHOUSE_CLUSTER_CONFIG_PATH`. The typed binding accepts a dynamic peer ID:

```yaml
cluster:
  replication:
    peers:
      LocalHadoopClusterB:
        tables-api-base-uri: "https://tables-b.example"
```

Peer IDs are the destination cluster IDs used by the table's replication policy, compared without
case sensitivity. Base URIs must be absolute HTTP(S) URIs without embedded credentials, query
parameters, or fragments. The Docker recipe configures `http://tables-b:8080` on cluster A and
`http://tables-a:8080` on cluster B for its private local Compose network.

The service-owned mode is not enabled yet. The current Tables API authentication contract
authenticates the end user's request but has no trusted, cryptographically verifiable assertion
that a replica-table DDL request was initiated by a source-cluster cascade. Forwarding the user's
principal in a request header would be spoofable, while using a service account would bypass the
destination user's ACL. The existing Spark implementation also calls the same destination DDL APIs
as a user, so the receiver cannot distinguish its legitimate cascade from a direct user request.
Before enabling service-owned DDL or rejecting direct replica DDL, the API needs a delegation
contract that authenticates the source service and binds the original authenticated user, operation,
and exact table identifiers; each destination must then authenticate that user and enforce its own
ACLs. Until that contract exists, the Spark compatibility path remains the only active cascade
implementation.

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
